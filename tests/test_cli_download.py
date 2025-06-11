"""
Tests for gen3.cli.download module
"""
import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from gen3.cli.download import download_single, download_multiple, load_manifest, validate_manifest


@pytest.fixture
def temp_manifest():
    """Create a temporary manifest file for testing"""
    manifest_data = [
        {"guid": "test-guid-1", "file_name": "file1.txt"},
        {"object_id": "test-guid-2", "file_name": "file2.txt"},
        {"guid": "test-guid-3", "file_name": "file3.txt"}
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(manifest_data, f)
        return f.name, manifest_data


@pytest.fixture
def mock_context():
    """Mock click context with auth factory"""
    context = MagicMock()
    context.obj = {"auth_factory": MagicMock()}
    context.obj["auth_factory"].get.return_value = MagicMock()
    return context


@pytest.mark.parametrize("test_scenario,expected_result", [
    ("valid_file", "success"),
    ("file_not_found", "error"),
    ("invalid_json", "error"),
])
def test_load_manifest(temp_manifest, test_scenario, expected_result):
    """Test manifest loading with various scenarios"""
    if test_scenario == "valid_file":
        manifest_path, expected_data = temp_manifest
        result = load_manifest(manifest_path)
        assert result == expected_data
        Path(manifest_path).unlink()
    
    elif test_scenario == "file_not_found":
        with pytest.raises(Exception) as exc_info:
            load_manifest("non_existent_file.json")
        assert "Error loading manifest" in str(exc_info.value)
    
    elif test_scenario == "invalid_json":
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            f.flush()
            
            with pytest.raises(Exception) as exc_info:
                load_manifest(f.name)
            assert "Error loading manifest" in str(exc_info.value)
            Path(f.name).unlink()


@pytest.mark.parametrize("manifest_data,expected_valid", [
    ([{"guid": "test-guid-1"}, {"object_id": "test-guid-2"}], True),
    ([], True),
    ([{"guid": "valid"}, "invalid-string", {"no_required_field": "invalid"}], False),
    ({"guid": "not-a-list"}, False),
    ("string", False),
    (None, False),
    ([{"file_name": "test.txt"}, {"metadata": "something"}], False),
])
def test_validate_manifest(manifest_data, expected_valid):
    """Test manifest validation with various data types and structures"""
    result = validate_manifest(manifest_data)
    assert result == expected_valid


@pytest.mark.parametrize("return_status,expected_exit_code,expected_output", [
    ({"status": "downloaded", "filepath": "/tmp/test_file.txt"}, 0, "✓ Downloaded: /tmp/test_file.txt"),
    ({"status": "skipped", "reason": "File already exists"}, 0, "- Skipped: File already exists"),
    ({"status": "failed", "error": "Network error"}, 1, "✗ Failed: Network error"),
])
def test_download_single(mock_context, return_status, expected_exit_code, expected_output):
    """Test download_single CLI command with various outcomes"""
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_file_instance = MagicMock()
        mock_file_instance.download_single.return_value = return_status
        mock_gen3_file.return_value = mock_file_instance
        
        result = runner.invoke(download_single, ['test-guid-123'], obj=mock_context.obj)
        
        assert result.exit_code == expected_exit_code
        assert expected_output in result.output


@pytest.mark.parametrize("options,expected_call_args", [
    # Basic test
    ([], {
        "guid": 'test-guid-123',
        "download_path": '.',
        "filename_format": 'original',
        "protocol": None,
        "skip_completed": False,
        "rename": False
    }),
    # Test with all options
    (['--download-path', '/custom/path', '--filename-format', 'guid', 
      '--protocol', 's3', '--skip-completed', '--rename'], {
        "guid": 'test-guid-123',
        "download_path": '/custom/path',
        "filename_format": 'guid',
        "protocol": 's3',
        "skip_completed": True,
        "rename": True
    }),
])
def test_download_single_options(mock_context, options, expected_call_args):
    """Test download_single with various option combinations"""
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_file_instance = MagicMock()
        mock_file_instance.download_single.return_value = {"status": "downloaded", "filepath": "/test/path"}
        mock_gen3_file.return_value = mock_file_instance
        
        result = runner.invoke(download_single, ['test-guid-123'] + options, obj=mock_context.obj)
        
        assert result.exit_code == 0
        mock_file_instance.download_single.assert_called_once_with(**expected_call_args)


def test_download_single_exception(mock_context):
    """Test download_single with exception handling"""
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_gen3_file.side_effect = Exception("Auth error")
        
        result = runner.invoke(download_single, ['test-guid-123'], obj=mock_context.obj)
        
        assert result.exit_code == 1
        assert "Download failed: Auth error" in result.output


@pytest.mark.parametrize("download_result,expected_exit_code,expected_outputs", [
    # All successful
    ({"succeeded": ["guid1", "guid2"], "failed": [], "skipped": ["guid3"]}, 
     0, ["✓ Succeeded: 2", "- Skipped: 1", "✗ Failed: 0", "Success rate: 66.7%"]),
    
    # Some failures
    ({"succeeded": ["guid1"], "failed": [{"guid": "guid2", "error": "Network timeout"}], "skipped": []}, 
     1, ["✓ Succeeded: 1", "✗ Failed: 1", "Some downloads failed"]),
])
def test_download_multiple(mock_context, temp_manifest, download_result, expected_exit_code, expected_outputs):
    """Test download_multiple with various success/failure scenarios"""
    manifest_path, manifest_data = temp_manifest
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_file_instance = MagicMock()
        mock_file_instance.download_multiple.return_value = download_result
        mock_gen3_file.return_value = mock_file_instance
        
        result = runner.invoke(download_multiple, [
            '--manifest', manifest_path,
            '--no-prompt'
        ], obj=mock_context.obj)
        
        assert result.exit_code == expected_exit_code
        for expected_output in expected_outputs:
            assert expected_output in result.output
        
        Path(manifest_path).unlink()


@pytest.mark.parametrize("manifest_scenario,expected_behavior", [
    ("empty_manifest", ("No files to download", 0)),
    ("invalid_manifest", ("Invalid manifest format", 1)),
    ("file_not_found", ("Error loading manifest", 1)),
])
def test_download_multiple_edge_cases(mock_context, manifest_scenario, expected_behavior):
    """Test download_multiple edge cases"""
    runner = CliRunner()
    expected_output, expected_exit_code = expected_behavior
    
    if manifest_scenario == "empty_manifest":
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([], f)
            manifest_path = f.name
        
        result = runner.invoke(download_multiple, ['--manifest', manifest_path], obj=mock_context.obj)
        Path(manifest_path).unlink()
        
    elif manifest_scenario == "invalid_manifest":
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([{"invalid": "data"}], f)
            manifest_path = f.name
        
        result = runner.invoke(download_multiple, ['--manifest', manifest_path], obj=mock_context.obj)
        Path(manifest_path).unlink()
        
    elif manifest_scenario == "file_not_found":
        result = runner.invoke(download_multiple, ['--manifest', 'non_existent.json'], obj=mock_context.obj)
    
    assert result.exit_code == expected_exit_code
    assert expected_output in result.output


def test_download_multiple_user_cancellation(mock_context, temp_manifest):
    """Test download_multiple when user cancels"""
    manifest_path, manifest_data = temp_manifest
    runner = CliRunner()
    
    result = runner.invoke(download_multiple, [
        '--manifest', manifest_path
    ], input='n\n', obj=mock_context.obj)
    
    assert result.exit_code == 0
    assert "Download cancelled" in result.output
    Path(manifest_path).unlink()


def test_download_multiple_all_options(mock_context, temp_manifest):
    """Test download_multiple with all available options"""
    manifest_path, manifest_data = temp_manifest
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_file_instance = MagicMock()
        mock_file_instance.download_multiple.return_value = {
            "succeeded": ["guid1", "guid2", "guid3"],
            "failed": [],
            "skipped": []
        }
        mock_gen3_file.return_value = mock_file_instance
        
        result = runner.invoke(download_multiple, [
            '--manifest', manifest_path,
            '--download-path', '/custom/path',
            '--filename-format', 'combined',
            '--protocol', 's3',
            '--num-parallel', '5',
            '--skip-completed',
            '--rename',
            '--no-prompt',
            '--no-progress'
        ], obj=mock_context.obj)
        
        assert result.exit_code == 0
        mock_file_instance.download_multiple.assert_called_once_with(
            manifest_data=manifest_data,
            download_path='/custom/path',
            filename_format='combined',
            protocol='s3',
            num_parallel=5,
            skip_completed=True,
            rename=True,
            no_prompt=True,
            no_progress=True
        )
        
        Path(manifest_path).unlink()


def test_download_multiple_exception(mock_context, temp_manifest):
    """Test download_multiple with exception handling"""
    manifest_path, manifest_data = temp_manifest
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_gen3_file.side_effect = Exception("Auth error")
        
        result = runner.invoke(download_multiple, [
            '--manifest', manifest_path,
            '--no-prompt'
        ], obj=mock_context.obj)
        
        assert result.exit_code == 1
        assert "Batch download failed: Auth error" in result.output
        Path(manifest_path).unlink()


@pytest.mark.parametrize("filename_format", ["original", "guid", "combined"])
def test_all_filename_formats_accepted(mock_context, filename_format):
    """Test that all valid filename formats are accepted"""
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_file_instance = MagicMock()
        mock_file_instance.download_single.return_value = {"status": "downloaded", "filepath": "test"}
        mock_gen3_file.return_value = mock_file_instance
        
        result = runner.invoke(download_single, [
            'test-guid',
            '--filename-format', filename_format
        ], obj=mock_context.obj)
        
        assert result.exit_code == 0


def test_filename_format_validation(mock_context):
    """Test that invalid filename format is rejected"""
    runner = CliRunner()
    
    result = runner.invoke(download_single, [
        'test-guid',
        '--filename-format', 'invalid'
    ], obj=mock_context.obj)
    
    assert result.exit_code == 2  # Click validation error


def test_num_parallel_validation(mock_context, temp_manifest):
    """Test that num_parallel accepts integer values"""
    manifest_path, manifest_data = temp_manifest
    runner = CliRunner()
    
    with patch('gen3.cli.download.Gen3File') as mock_gen3_file:
        mock_file_instance = MagicMock()
        mock_file_instance.download_multiple.return_value = {
            "succeeded": [], "failed": [], "skipped": []
        }
        mock_gen3_file.return_value = mock_file_instance
        
        result = runner.invoke(download_multiple, [
            '--manifest', manifest_path,
            '--num-parallel', '10',
            '--no-prompt'
        ], obj=mock_context.obj)
        
        assert result.exit_code == 0
        Path(manifest_path).unlink()
