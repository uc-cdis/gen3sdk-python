import pytest
import math
from gen3.fhir import (
    Gen3FHIRAuthzTagger,
    _is_new,
    _is_done,
    _merge_needed,
    split_file,
    transform_chunk,
    merge_chunks,
    tag_fhir_resources_with_authz,
    DEFAULT_WORK_DIR,
)
import pathlib
import os
import json
import subprocess
import shutil
import yaml

TMP_ROOT = pathlib.Path(__file__).parent / "test_data" / "fhir_outputs"
SRC = pathlib.Path(
    f"{pathlib.Path(__file__).parent}/test_data/test_fhir_Patient.ndjson"
)
IN = pathlib.Path(f"{pathlib.Path(__file__).parent}/test_data/Patient.ndjson")
OUT = pathlib.Path(f"{TMP_ROOT}/fhir_output_Patient.ndjson")
CONFIG_SRC = pathlib.Path(f"{pathlib.Path(__file__).parent}/test_data/fhir_config.yaml")
GLOBAL_CONFIG_SRC = pathlib.Path(
    f"{pathlib.Path(__file__).parent}/test_data/fhir_global_config.yaml"
)
TAGGER = Gen3FHIRAuthzTagger(CONFIG_SRC)
BATCH_SIZE = 1
BASE_RECORD = {
    "timestamp": "2026-08-12T21:00:20.677168+00:00",
    "input_file": f"{IN}",
    "output_file": f"{pathlib.Path(TMP_ROOT)}/status.ndjson",
    "config_hash": "9c61e70e3ea403e3daf1ee795036253c",
    "batch_size": 1,
}


@pytest.fixture(scope="session")
def tmp_root():
    shutil.rmtree(TMP_ROOT, ignore_errors=True)
    TMP_ROOT.mkdir(parents=True)


def mock_state(
    directory,
    config="match",
    chunks=0,
    done=0,
    output=None,
    record=None,
):
    """
    Build an on-disk run directory and return (directory, record).

    Args:
        tmp_path (Path): parent directory; a fresh subdir is created under it
        config: "match" -> .config.json equal to record
                dict    -> record updated with these overrides
                str     -> written verbatim (for malformed-JSON cases)
                None    -> no .config.json written
        chunks (int): number of chunk_NNN.chunk files
        done (int): number of chunk_NNN.done files (indices align with chunks)
        output: None -> no output file, "empty" -> touched, "full" -> one row
        record (dict): base record; defaults to BASE_RECORD

    Returns:
        (Path, dict)
    """

    record = dict(record or BASE_RECORD)
    out = pathlib.Path(directory) / "status.ndjson"
    record["output_file"] = str(out)

    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)

    if config == "match":
        params = dict(record)
    elif isinstance(config, dict):
        params = dict(record)
        params.update(config)
    else:
        params = None

    if params is not None:
        (directory / ".config.json").write_text(json.dumps(params), encoding="utf-8")
    elif isinstance(config, str) and config != "match":
        (directory / ".config.json").write_text(config, encoding="utf-8")

    for i in range(chunks):
        (directory / f"chunk_{i:03d}.chunk").write_text("{}\n", encoding="utf-8")
    for i in range(done):
        (directory / f"chunk_{i:03d}.done").write_text("{}\n", encoding="utf-8")

    if output == "empty":
        out.touch()
    elif output == "full":
        out.write_text('{"id":1}\n', encoding="utf-8")

    return directory, record


def test_fhir_output():
    """Tests that output file matches the input file with the only difference being the tags"""
    fin = [
        json.loads(line)
        for line in pathlib.Path(IN).read_bytes().splitlines()
        if line.strip()
    ]
    src = [
        json.loads(line)
        for line in pathlib.Path(SRC).read_bytes().splitlines()
        if line.strip()
    ]
    tag_fhir_resources_with_authz(IN, OUT, CONFIG_SRC, BATCH_SIZE, TMP_ROOT, 8)
    out = [
        json.loads(line)
        for line in pathlib.Path(OUT).read_bytes().splitlines()
        if line.strip()
    ]

    # check that output matched input in everything other than the tags
    assert len(fin) == len(
        out
    ), "Length of the output file does not match the length of the input file"
    assert {r["id"] for r in fin} == {
        r["id"] for r in out
    }, "IDs in the output file do not match the IDs in the input file. Order not maintained"
    assert [{k: v for k, v in r.items() if k != "meta"} for r in out] == [
        {k: v for k, v in r.items() if k != "meta"} for r in fin
    ], "Content of the output file does not match the content of the input file (other than the security tags)"
    assert src == out, "Output file does not match source file"


def test_chunking():
    """Tests that number of chunks is correct and the recombined chunks match the input file"""
    split_file(IN, BATCH_SIZE, TMP_ROOT)
    chunks = list(TMP_ROOT.glob("*.chunk"))
    fin = [
        json.loads(line)
        for line in pathlib.Path(IN).read_bytes().splitlines()
        if line.strip()
    ]
    expected = math.ceil(len(fin) / BATCH_SIZE)

    # number of chunk files matches expected
    assert len(chunks) == expected, f"Expected {expected} chunks, found {len(chunks)}"
    recombined = [
        json.loads(line)
        for p in sorted(chunks)
        for line in p.read_bytes().splitlines()
        if line.strip()
    ]
    # chunks recombine to match input file
    assert (
        recombined == fin
    ), "Recombined chunks do not match the content of the input file"


def test_transform():
    """Asserts transform_chunk creates the same number of .done files as .chunk and no .chunk files remain once transformation is completed"""
    TAGGER.relevant_authz_rules(os.path.basename(IN).split(".")[0])
    chunks = list(TMP_ROOT.glob("*.chunk"))
    for c in chunks:
        transform_chunk(c, TAGGER, TMP_ROOT)
    transformed = list(TMP_ROOT.glob("*.done"))

    # same number of transformed files as chunk files
    assert len(transformed) == len(
        chunks
    ), f"Expected same number of transformed .done files as number of .chunk files after transformation completed, found {len(transformed)}"
    # all .chunk files deleted after transformation completed
    assert (
        len(list(TMP_ROOT.glob("*.chunk"))) == 0
    ), f"Expected 0 chunks after transformation completed, found {len(list(TMP_ROOT.glob("*.chunk")))}"


def test_merge():
    """Asserts that after merge is completed, the output file exists and is not empty, there are no .done files remaining,
    the length of the output matches the sum of the transformed files and the input file, and the file is not corrupt/formatting is correct
    """
    transformed = list(TMP_ROOT.glob("*.done"))
    transformed_sum = sum(
        len([l for l in pathlib.Path(c).read_bytes().splitlines() if l.strip()])
        for c in transformed
    )
    merge_chunks(transformed, OUT)
    merged = pathlib.Path(OUT).read_text(encoding="utf-8")
    fin = [
        json.loads(line)
        for line in pathlib.Path(IN).read_bytes().splitlines()
        if line.strip()
    ]
    fout = [
        json.loads(line)
        for line in pathlib.Path(OUT).read_bytes().splitlines()
        if line.strip()
    ]

    # output file exists after merge
    assert os.path.exists(OUT), "Output file was not created"
    # output file is not empty after merge
    assert pathlib.Path(OUT).stat().st_size > 0, "Output file empty"
    # no leftover .done files after merge completed
    assert (
        len(list(TMP_ROOT.glob("*.done"))) == 0
    ), f"Expected 0 .done files after transformation completed, found {len(list(TMP_ROOT.glob("*.done")))}"
    # output file is the same length as the combined transformed files and the length of the input file
    assert (
        len(fout) == transformed_sum == len(fin)
    ), "The length of the output file does not math the sum of the .done files and the input file"
    # check formatting of output file
    assert merged.endswith("\n"), "File does not end with a newline"
    assert not merged.endswith("\n\n"), "File ends with a blank line"
    for i, line in enumerate(merged.split("\n")[:-1]):
        assert line.strip(), f"blank line at index {i}"
        json.loads(line)  # every line independently parses


class Test_is_new:
    """Tests the logic of the run/directory being marked as new (_is_new(directory, record) returns True)"""

    def __init__(self):
        self.tmp_path = TMP_ROOT / "test_new"

    def test_is_new_with_missing_directory(self):
        # directory missing
        assert _is_new(TMP_ROOT / "does_not_exist", BASE_RECORD) is True

    def test_is_new_directory_has_no_config_file(self):
        # no config file
        directory, record = mock_state(self.tmp_path, config=None)
        assert _is_new(directory, record) is True

    def test_is_new_directory_has_corrupt_config_file(self):
        # check corrupt config file
        directory, record = mock_state(self.tmp_path, config="{not valid json")
        assert _is_new(directory, record) is True

    def test_is_new_directory_has_empty_config_file(self):
        # check empty config file
        directory, record = mock_state(self.tmp_path, config="")
        assert _is_new(directory, record) is True

    def test_is_not_new_when_config_matches(self):
        # check if not new when everything matches
        directory, record = mock_state(self.tmp_path, config="match")
        assert _is_new(directory, record) is False

    def test_is_new_when_config_changed(self):
        # check when config changed
        directory, record = mock_state(
            self.tmp_path, config={"config_hash": "different"}
        )
        assert _is_new(directory, record) is True

    @pytest.mark.parametrize("batch_size", [0, 100, 50, 4])
    def test_is_new_when_batch_size_changed(self, batch_size):
        # check when batch_size changed
        directory, record = mock_state(self.tmp_path, config={"batch_size": batch_size})
        assert _is_new(directory, record) is True

    def test_is_new_when_output_filename_changed(self):
        # check when output filename changed
        directory, record = mock_state(
            self.tmp_path, config={"output_file": "/tmp/somewhere_else.ndjson"}
        )
        assert _is_new(directory, record) is True


class Test_is_done:
    """Tests the logic of the run/directory being marked as done (_is_done(directory, record) returns True)"""

    def __init__(self):
        self.tmp_path = TMP_ROOT / "test_done"

    def test_is_done_with_matching_config_and_empty_output(self):
        # config matches but output is empty
        directory, record = mock_state(self.tmp_path, config="match", output="empty")
        assert _is_done(directory, record) is False

    def test_is_done_with_matching_config_and_no_output(self):
        # config matches but no output
        directory, record = mock_state(self.tmp_path, config="match", output=None)
        assert _is_done(directory, record) is False

    def test_is_done_with_no_config_and_full_output(self):
        # output full but no config file
        directory, record = mock_state(self.tmp_path, config=None, output="full")
        assert _is_done(directory, record) is False

    def test_is_done_with_corrupt_config_and_full_output(self):
        # output full but config file is corrupt
        directory, record = mock_state(self.tmp_path, config="{bad", output="full")
        assert _is_done(directory, record) is False

    @pytest.mark.parametrize("batch_size", [0, 100, 50, 4])
    def test_is_done_with_full_output_and_different_batch_size(self, batch_size):
        # output full batch_size changed
        directory, record = mock_state(
            self.tmp_path, config={"batch_size": batch_size}, output="full"
        )
        assert _is_done(directory, record) is False

    def test_is_done_with_full_output_and_different_config_hash(self):
        # output full but config changed
        directory, record = mock_state(
            self.tmp_path, config={"config_hash": "different"}, output="full"
        )
        assert _is_done(directory, record) is False

    def test_is_done_with_full_output_and_different_output_filename(self):
        # output full but output filename changed
        directory, record = mock_state(
            self.tmp_path,
            config={"output_file": "/tmp/somewhere_else.ndjson"},
            output="full",
        )
        assert _is_done(directory, record) is False

    def test_is_done_with_full_output_and_matching_config(self):
        # config matches and output is full
        directory, record = mock_state(self.tmp_path, config="match", output="full")
        assert _is_done(directory, record) is True


class Test_merge_needed:
    """Tests the logic of the run/directory being marked as merge needed (_merge_needed(directory, record) returns True)"""

    def __init__(self):
        self.tmp_path = TMP_ROOT / "outputs" / "test_merge"

    def test_merge_needed_when_no_done_files_remaining(self):
        # no merge on clean directory
        directory, record = mock_state(self.tmp_path, done=0)
        assert _merge_needed(directory, record) is False

    @pytest.mark.parametrize("done_files", [5, 20, 57, 100])
    def test_merge_needed_when_done_files_remaining(self, done_files):
        # merge when done files left
        directory, record = mock_state(self.tmp_path, done=done_files)
        assert _merge_needed(directory, record) is True

    @pytest.mark.parametrize(
        ["chunk_files", "done_files"], [(5, 2), (5, 5), (5, 7), (1, 10)]
    )
    def test_merge_needed_when_chunk_and_done_files_remaining(
        self, chunk_files, done_files
    ):
        # no merge when both chunk and done files left
        directory, record = mock_state(
            self.tmp_path, chunks=chunk_files, done=done_files
        )
        assert _merge_needed(directory, record) is False

    def test_merge_needed_when_directory_missing(self):
        # no merge when directory missing
        assert _merge_needed(TMP_ROOT / "gone", BASE_RECORD) is False


class Test_status:
    """Tests for overlap in different statuses"""

    def __init__(self):
        self.tmp_path = TMP_ROOT / "test_status"

    def test_fresh_directory(self):
        # fresh directory
        directory, record = mock_state(self.tmp_path, config=None)
        assert _is_new(directory, record) is True
        assert _is_done(directory, record) is False
        assert _merge_needed(directory, record) is False

    @pytest.mark.parametrize(
        ["chunk_files", "done_files"], [(5, 2), (5, 5), (5, 7), (1, 10)]
    )
    def test_need_to_resume_overlap(self, chunk_files, done_files):
        directory, record = mock_state(
            self.tmp_path, config="match", chunks=chunk_files, done=done_files
        )
        assert _is_new(directory, record) is False
        assert _is_done(directory, record) is False

    @pytest.mark.parametrize(
        "chunks", ["match", None, {"config_hash": "x"}, {"batch_size": 1}]
    )
    def test_merge_needed_overlap(self):
        # merge needed
        directory, record = mock_state(self.tmp_path, config="match", chunks=0, done=5)
        assert _is_new(directory, record) is False
        assert _is_done(directory, record) is False
        assert _merge_needed(directory, record) is True

    def test_run_complete(self):
        # fully complete
        directory, record = mock_state(self.tmp_path, config="match", output="full")
        assert _is_done(directory, record) is True
        assert _merge_needed(directory, record) is False
        assert _is_new(directory, record) is False

    @pytest.mark.parametrize(
        "state", ["match", None, {"config_hash": "x"}, {"batch_size": 1}]
    )
    def test_new_and_done_are_mutually_exclusive(self, state):
        # new and done are mutually exclusive
        directory, record = mock_state(
            TMP_ROOT / str(id(state)), config=state, output="full"
        )
        assert not (_is_new(directory, record) and _is_done(directory, record))


def test_cli():
    """Run the CLI and return the CompletedProcess."""
    args = [
        IN,
        OUT,
        CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, "CLI run failed"
    assert pathlib.Path(OUT).exists(), "CLI exited 0 but wrote no output file"
    records = [
        json.loads(line)
        for line in pathlib.Path(OUT).read_bytes().splitlines()
        if line.strip()
    ]
    in_records = [
        json.loads(line)
        for line in pathlib.Path(IN).read_bytes().splitlines()
        if line.strip()
    ]
    assert len(records) == len(
        in_records
    ), "Output number of records doesn't match the input number of records"
    assert all(
        "security" in r.get("meta", {}) for r in records
    ), "Security tags missing"


@pytest.mark.parametrize("bad", [0, -1, None, "bad"])
def test_invalid_batch_size_is_rejected(bad):
    """Assert invalid batch_size is rejected and raises an error"""
    out = TMP_ROOT / "cli_test" / "cli_out.ndjson"

    args = [
        IN,
        out,
        CONFIG_SRC,
        "--batch_size",
        str(bad),
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode != 0, "Function accept invalid batch_size"
    assert not pathlib.Path(
        out
    ).exists(), "Rejected the argument but still wrote output"


def test_missing_input_file_fails_cleanly():
    """Asserts error is raised if missing input file passed as argument"""
    out = TMP_ROOT / "cli_test" / "nope.ndjson"
    args = [
        "nope.ndjson",
        out,
        CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0, "Function accepts non-existent input file"
    assert "Traceback" not in result.stderr, "Raw traceback instead of an error message"
    assert not pathlib.Path(
        out
    ).exists(), "Output file written even though invalid input file passed"


def test_output_directory_does_not_exist():
    """Test response if output directory doesn't exists. Directory (and parents) should be created if missing"""
    out = TMP_ROOT / "missing_dir" / "out.ndjson"
    args = [
        IN,
        out,
        CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, "Raises error instead of creating output directory"
    assert pathlib.Path(out).exists(), "Process not completed, output file not written"


def test_global_config_overrides_other_rules():
    """Assert global authorization overrides any other rules"""
    out = TMP_ROOT / "global_config" / "out.ndjson"
    args = [
        IN,
        out,
        GLOBAL_CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, "Run fails"
    assert pathlib.Path(out).exists(), "Process not completed, output file not written"
    tagged_file = [
        json.loads(line)
        for line in pathlib.Path(out).read_bytes().splitlines()
        if line.strip()
    ]
    with open(GLOBAL_CONFIG_SRC, "r") as f:
        config = yaml.safe_load(f)

    for rec in tagged_file:
        security = (rec.get("meta")).get("security")[0].get("code")
        assert (
            security == config["global_authz"]
        ), "File not tagged with global authorization"
