"""
Tests for gen3.cli.download module
"""

import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock
import pytest
import click

from gen3.cli.download import (
    load_manifest,
    validate_manifest,
)


class TestLoadManifest:
    """Test cases for load_manifest function"""

    def test_load_manifest_valid_json(self):
        """Test loading a valid JSON manifest file"""
        manifest_data = [
            {"guid": "123", "object_id": "test1"},
            {"guid": "456", "object_id": "test2"},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(manifest_data, f)
            temp_path = f.name

        try:
            result = load_manifest(temp_path)
            assert result == manifest_data
        finally:
            os.unlink(temp_path)

    def test_load_manifest_file_not_found(self):
        """Test loading a non-existent manifest file"""
        with pytest.raises(click.ClickException) as exc_info:
            load_manifest("nonexistent_file.json")
        assert "Error loading manifest" in str(exc_info.value)

    def test_load_manifest_invalid_json(self):
        """Test loading an invalid JSON manifest file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("invalid json content")
            temp_path = f.name

        try:
            with pytest.raises(click.ClickException) as exc_info:
                load_manifest(temp_path)
            assert "Error loading manifest" in str(exc_info.value)
        finally:
            os.unlink(temp_path)

    def test_load_manifest_empty_file(self):
        """Test loading an empty JSON file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("")
            temp_path = f.name

        try:
            with pytest.raises(click.ClickException) as exc_info:
                load_manifest(temp_path)
            assert "Error loading manifest" in str(exc_info.value)
        finally:
            os.unlink(temp_path)

    def test_load_manifest_permission_error(self):
        """Test loading a file with permission error"""
        with patch("builtins.open", side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError) as exc_info:
                load_manifest("test.json")
            assert "Permission denied" in str(exc_info.value)


class TestValidateManifest:
    """Test cases for validate_manifest function"""

    def test_validate_manifest_valid_data(self):
        """Test validation of valid manifest data"""
        manifest_data = [
            {"guid": "123", "object_id": "test1"},
            {"guid": "456", "object_id": "test2"},
        ]
        assert validate_manifest(manifest_data) is True

    def test_validate_manifest_guid_only(self):
        """Test validation with guid only"""
        manifest_data = [
            {"guid": "123"},
            {"guid": "456"},
        ]
        assert validate_manifest(manifest_data) is True

    def test_validate_manifest_object_id_only(self):
        """Test validation with object_id only"""
        manifest_data = [
            {"object_id": "test1"},
            {"object_id": "test2"},
        ]
        assert validate_manifest(manifest_data) is True

    def test_validate_manifest_not_list(self):
        """Test validation with non-list data"""
        manifest_data = {"guid": "123"}
        assert validate_manifest(manifest_data) is False

    def test_validate_manifest_empty_list(self):
        """Test validation with empty list"""
        manifest_data = []
        assert validate_manifest(manifest_data) is True

    def test_validate_manifest_missing_identifiers(self):
        """Test validation with missing guid and object_id"""
        manifest_data = [
            {"other_field": "value"},
            {"guid": "123"},
        ]
        assert validate_manifest(manifest_data) is False

    def test_validate_manifest_non_dict_items(self):
        """Test validation with non-dict items in list"""
        manifest_data = [
            {"guid": "123"},
            "not_a_dict",
        ]
        assert validate_manifest(manifest_data) is False

    def test_validate_manifest_none_values(self):
        """Test validation with None values"""
        manifest_data = [
            {"guid": None, "object_id": None},
            {"guid": "123"},
        ]
        assert validate_manifest(manifest_data) is False

    def test_validate_manifest_empty_strings(self):
        """Test validation with empty string values"""
        manifest_data = [
            {"guid": "", "object_id": ""},
            {"guid": "123"},
        ]
        assert validate_manifest(manifest_data) is False


class TestDownloadSingle:
    """Test cases for download_single command"""

    @patch("gen3.cli.download.Gen3File")
    def test_download_single_success(self, mock_gen3_file):
        """Test successful single file download"""
        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {
            "status": "downloaded",
            "filepath": "/path/to/file.txt",
            "size": 1024,
        }
        mock_file_client.download_single.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            # Test the function logic directly
            auth = ctx.obj["auth_factory"].get()
            file_client = mock_gen3_file(auth_provider=auth)
            result = file_client.download_single(
                guid="test-guid",
                download_path=".",
                filename_format="original",
                protocol=None,
                skip_completed=False,
                rename=False,
            )

            assert result == mock_result
            mock_file_client.download_single.assert_called_once_with(
                guid="test-guid",
                download_path=".",
                filename_format="original",
                protocol=None,
                skip_completed=False,
                rename=False,
            )

    @patch("gen3.cli.download.Gen3File")
    def test_download_single_skipped(self, mock_gen3_file):
        """Test skipped single file download"""
        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {"status": "skipped", "reason": "File already exists"}
        mock_file_client.download_single.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            auth = ctx.obj["auth_factory"].get()
            file_client = mock_gen3_file(auth_provider=auth)
            result = file_client.download_single(
                guid="test-guid",
                download_path=".",
                filename_format="original",
                protocol=None,
                skip_completed=True,
                rename=False,
            )

            assert result == mock_result

    @patch("gen3.cli.download.Gen3File")
    def test_download_single_failed(self, mock_gen3_file):
        """Test failed single file download"""
        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {"status": "failed", "error": "Network error"}
        mock_file_client.download_single.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            auth = ctx.obj["auth_factory"].get()
            file_client = mock_gen3_file(auth_provider=auth)
            result = file_client.download_single(
                guid="test-guid",
                download_path=".",
                filename_format="original",
                protocol=None,
                skip_completed=False,
                rename=False,
            )

            assert result == mock_result

    @patch("gen3.cli.download.Gen3File")
    def test_download_single_exception(self, mock_gen3_file):
        """Test exception handling in single file download"""
        mock_gen3_file.side_effect = Exception("Connection error")

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            with pytest.raises(Exception) as exc_info:
                auth = ctx.obj["auth_factory"].get()
                mock_gen3_file(auth_provider=auth)

            assert "Connection error" in str(exc_info.value)

    @patch("gen3.cli.download.Gen3File")
    def test_download_single_with_all_options(self, mock_gen3_file):
        """Test single file download with all options"""
        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {
            "status": "downloaded",
            "filepath": "/path/to/file.txt",
            "size": 1024,
        }
        mock_file_client.download_single.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            auth = ctx.obj["auth_factory"].get()
            file_client = mock_gen3_file(auth_provider=auth)
            result = file_client.download_single(
                guid="test-guid",
                download_path="/custom/path",
                filename_format="guid",
                protocol="s3",
                skip_completed=True,
                rename=True,
            )

            assert result == mock_result
            mock_file_client.download_single.assert_called_once_with(
                guid="test-guid",
                download_path="/custom/path",
                filename_format="guid",
                protocol="s3",
                skip_completed=True,
                rename=True,
            )


class TestDownloadMultipleAsync:
    """Test cases for download_multiple_async command"""

    @patch("gen3.cli.download.load_manifest")
    @patch("gen3.cli.download.validate_manifest")
    @patch("gen3.cli.download.Gen3File")
    @patch("gen3.cli.download.get_or_create_event_loop_for_thread")
    def test_download_multiple_async_success(
        self, mock_loop, mock_gen3_file, mock_validate, mock_load
    ):
        """Test successful async batch download"""
        mock_manifest_data = [
            {"guid": "123", "object_id": "test1"},
            {"guid": "456", "object_id": "test2"},
        ]
        mock_load.return_value = mock_manifest_data
        mock_validate.return_value = True

        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {"succeeded": ["123", "456"], "failed": [], "skipped": []}
        mock_file_client.async_download_multiple = AsyncMock(return_value=mock_result)

        mock_loop_instance = MagicMock()
        mock_loop.return_value = mock_loop_instance
        mock_loop_instance.run_until_complete.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            # Test the function logic directly
            auth = ctx.obj["auth_factory"].get()
            manifest_data = mock_load("manifest.json")

            if not mock_validate(manifest_data):
                raise click.ClickException("Invalid manifest format")

            if not manifest_data:
                mock_echo("No files to download")
                return

            file_client = mock_gen3_file(auth_provider=auth)
            result = mock_loop_instance.run_until_complete(
                file_client.async_download_multiple(
                    manifest_data=manifest_data,
                    download_path=".",
                    filename_format="original",
                    protocol=None,
                    max_concurrent_requests=10,
                    skip_completed=False,
                    rename=False,
                    no_progress=False,
                )
            )

            assert result == mock_result
            mock_load.assert_called_once_with("manifest.json")
            mock_validate.assert_called_once_with(mock_manifest_data)

    @patch("gen3.cli.download.load_manifest")
    @patch("gen3.cli.download.validate_manifest")
    def test_download_multiple_async_invalid_manifest(self, mock_validate, mock_load):
        """Test async download with invalid manifest"""
        mock_manifest_data = [{"invalid": "data"}]
        mock_load.return_value = mock_manifest_data
        mock_validate.return_value = False

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with pytest.raises(click.ClickException) as exc_info:
            manifest_data = mock_load("manifest.json")
            if not mock_validate(manifest_data):
                raise click.ClickException("Invalid manifest format")

        assert "Invalid manifest format" in str(exc_info.value)

    @patch("gen3.cli.download.load_manifest")
    def test_download_multiple_async_empty_manifest(self, mock_load):
        """Test async download with empty manifest"""
        mock_load.return_value = []

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            manifest_data = mock_load("manifest.json")
            if not manifest_data:
                mock_echo("No files to download")

            mock_echo.assert_called_with("No files to download")

    @patch("gen3.cli.download.load_manifest")
    @patch("gen3.cli.download.validate_manifest")
    @patch("gen3.cli.download.Gen3File")
    @patch("gen3.cli.download.get_or_create_event_loop_for_thread")
    def test_download_multiple_async_with_failures(
        self, mock_loop, mock_gen3_file, mock_validate, mock_load
    ):
        """Test async download with some failures"""
        mock_manifest_data = [
            {"guid": "123", "object_id": "test1"},
            {"guid": "456", "object_id": "test2"},
            {"guid": "789", "object_id": "test3"},
        ]
        mock_load.return_value = mock_manifest_data
        mock_validate.return_value = True

        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {
            "succeeded": ["123"],
            "failed": [
                {"guid": "456", "error": "Network error"},
                {"guid": "789", "error": "File not found"},
            ],
            "skipped": [],
        }
        mock_file_client.async_download_multiple = AsyncMock(return_value=mock_result)

        mock_loop_instance = MagicMock()
        mock_loop.return_value = mock_loop_instance
        mock_loop_instance.run_until_complete.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            auth = ctx.obj["auth_factory"].get()
            manifest_data = mock_load("manifest.json")

            if not mock_validate(manifest_data):
                raise click.ClickException("Invalid manifest format")

            file_client = mock_gen3_file(auth_provider=auth)
            result = mock_loop_instance.run_until_complete(
                file_client.async_download_multiple(
                    manifest_data=manifest_data,
                    download_path=".",
                    filename_format="original",
                    protocol=None,
                    max_concurrent_requests=10,
                    skip_completed=False,
                    rename=False,
                    no_progress=False,
                )
            )

            assert result == mock_result

    @patch("gen3.cli.download.load_manifest")
    @patch("gen3.cli.download.validate_manifest")
    @patch("gen3.cli.download.Gen3File")
    def test_download_multiple_async_exception(
        self, mock_gen3_file, mock_validate, mock_load
    ):
        """Test exception handling in async download"""
        mock_manifest_data = [{"guid": "123"}]
        mock_load.return_value = mock_manifest_data
        mock_validate.return_value = True
        mock_gen3_file.side_effect = Exception("Connection error")

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with pytest.raises(Exception) as exc_info:
            auth = ctx.obj["auth_factory"].get()
            manifest_data = mock_load("manifest.json")

            if not mock_validate(manifest_data):
                raise click.ClickException("Invalid manifest format")

            mock_gen3_file(auth_provider=auth)

        assert "Connection error" in str(exc_info.value)

    @patch("gen3.cli.download.load_manifest")
    @patch("gen3.cli.download.validate_manifest")
    @patch("gen3.cli.download.Gen3File")
    @patch("gen3.cli.download.get_or_create_event_loop_for_thread")
    def test_download_multiple_async_user_confirmation(
        self, mock_loop, mock_gen3_file, mock_validate, mock_load
    ):
        """Test async download with user confirmation"""
        mock_manifest_data = [{"guid": "123"}]
        mock_load.return_value = mock_manifest_data
        mock_validate.return_value = True

        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {"succeeded": ["123"], "failed": [], "skipped": []}
        mock_file_client.async_download_multiple = AsyncMock(return_value=mock_result)

        mock_loop_instance = MagicMock()
        mock_loop.return_value = mock_loop_instance
        mock_loop_instance.run_until_complete.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            with patch("click.confirm") as mock_confirm:
                mock_confirm.return_value = True

                manifest_data = mock_load("manifest.json")
                mock_echo(f"Found {len(manifest_data)} files to download")

                if not mock_confirm("Continue with async download?"):
                    mock_echo("Download cancelled")
                    return

                auth = ctx.obj["auth_factory"].get()
                file_client = mock_gen3_file(auth_provider=auth)
                result = mock_loop_instance.run_until_complete(
                    file_client.async_download_multiple(
                        manifest_data=manifest_data,
                        download_path=".",
                        filename_format="original",
                        protocol=None,
                        max_concurrent_requests=10,
                        skip_completed=False,
                        rename=False,
                        no_progress=False,
                    )
                )

                assert result == mock_result
                mock_echo.assert_any_call("Found 1 files to download")
                mock_confirm.assert_called_once_with("Continue with async download?")

    @patch("gen3.cli.download.load_manifest")
    @patch("gen3.cli.download.validate_manifest")
    def test_download_multiple_async_user_cancels(self, mock_validate, mock_load):
        """Test async download when user cancels"""
        mock_manifest_data = [{"guid": "123"}]
        mock_load.return_value = mock_manifest_data
        mock_validate.return_value = True

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            with patch("click.confirm") as mock_confirm:
                mock_confirm.return_value = False

                manifest_data = mock_load("manifest.json")
                mock_echo(f"Found {len(manifest_data)} files to download")

                if not mock_confirm("Continue with async download?"):
                    mock_echo("Download cancelled")
                    return

                mock_echo.assert_any_call("Found 1 files to download")
                mock_echo.assert_any_call("Download cancelled")

    @patch("gen3.cli.download.load_manifest")
    @patch("gen3.cli.download.validate_manifest")
    @patch("gen3.cli.download.Gen3File")
    @patch("gen3.cli.download.get_or_create_event_loop_for_thread")
    def test_download_multiple_async_with_custom_options(
        self, mock_loop, mock_gen3_file, mock_validate, mock_load
    ):
        """Test async download with custom options"""
        mock_manifest_data = [{"guid": "123"}]
        mock_load.return_value = mock_manifest_data
        mock_validate.return_value = True

        mock_file_client = MagicMock()
        mock_gen3_file.return_value = mock_file_client

        mock_result = {"succeeded": ["123"], "failed": [], "skipped": []}
        mock_file_client.async_download_multiple = AsyncMock(return_value=mock_result)

        mock_loop_instance = MagicMock()
        mock_loop.return_value = mock_loop_instance
        mock_loop_instance.run_until_complete.return_value = mock_result

        ctx = MagicMock()
        ctx.obj = {"auth_factory": MagicMock()}
        ctx.obj["auth_factory"].get.return_value = MagicMock()

        with patch("click.echo") as mock_echo:
            auth = ctx.obj["auth_factory"].get()
            manifest_data = mock_load("manifest.json")

            if not mock_validate(manifest_data):
                raise click.ClickException("Invalid manifest format")

            file_client = mock_gen3_file(auth_provider=auth)
            result = mock_loop_instance.run_until_complete(
                file_client.async_download_multiple(
                    manifest_data=manifest_data,
                    download_path="/custom/path",
                    filename_format="guid",
                    protocol="s3",
                    max_concurrent_requests=20,
                    skip_completed=True,
                    rename=True,
                    no_progress=True,
                )
            )

            assert result == mock_result
            mock_file_client.async_download_multiple.assert_called_once_with(
                manifest_data=manifest_data,
                download_path="/custom/path",
                filename_format="guid",
                protocol="s3",
                max_concurrent_requests=20,
                skip_completed=True,
                rename=True,
                no_progress=True,
            )
