import pytest
import math
from gen3.fhir import (
    Gen3FHIRAuthzTagger,
    _is_new,
    _is_done,
    _merge_needed,
    compute_ranges,
    transform_chunk,
    merge_chunks,
    tag_fhir_resources_with_authz,
    cleanup_fhir_transform_artifacts,
    get_resource_type,
)
import pathlib
import re
import os
import json
import subprocess
import shutil
import yaml
from typing import Any

TMP_ROOT = pathlib.Path(__file__).parent / "test_data" / "fhir_outputs"
DONE_SRC = pathlib.Path(__file__).parent / "test_data" / "fhir_inputs" / "merge"
SRC = pathlib.Path(
    f"{pathlib.Path(__file__).parent}/test_data/test_fhir_Patient.ndjson"
)
IN = pathlib.Path(f"{pathlib.Path(__file__).parent}/test_data/Patient.ndjson")
CONFIG_SRC = pathlib.Path(f"{pathlib.Path(__file__).parent}/test_data/fhir_config.yaml")
GLOBAL_CONFIG_SRC = pathlib.Path(
    f"{pathlib.Path(__file__).parent}/test_data/fhir_global_config.yaml"
)

BATCH_SIZE = 1
N_CHUNKS = len(compute_ranges(IN, BATCH_SIZE))
BASE_RECORD = {
    "timestamp": "2026-08-12T21:00:20.677168+00:00",
    "input_file": f"{IN}",
    "output_file": "status.ndjson",
    "config_hash": "9c61e70e3ea403e3daf1ee795036253c",  # pragma: allowlist secret
    "batch_size": 1,
}


@pytest.fixture(scope="session", autouse=True)
def tmp_root() -> None:
    """Clear outputs from the previous session. Outputs are left in place afterwards so intermediates can be inspected."""
    shutil.rmtree(TMP_ROOT, ignore_errors=True)
    TMP_ROOT.mkdir(parents=True)

@pytest.fixture
def case_dir(request: pytest.FixtureRequest) -> pathlib.Path:
    """A fresh directory under TMP_ROOT named after the test, so each test's intermediates are isolated and easy to find."""
    name = request.node.nodeid.split("::", 1)[1]
    directory = TMP_ROOT / re.sub(r"\W+", "_", name).strip("_")
    directory.mkdir(parents=True)
    return directory

@pytest.fixture(scope="session")
def tagger():
    """A tagger built from the synthetic Patient rules config."""
    return Gen3FHIRAuthzTagger(CONFIG_SRC)

@pytest.fixture
def transform_workdir(case_dir: pathlib.Path) -> pathlib.Path:
    """An empty directory for transform_chunk output."""
    workdir = case_dir / "transform"
    workdir.mkdir(parents=True)
    return workdir


@pytest.fixture
def merge_workdir(case_dir: pathlib.Path) -> pathlib.Path:
    """A directory pre-populated with transformed .done chunks ready to merge."""
    workdir = case_dir / "merge"
    workdir.mkdir(parents=True)
    for src in DONE_SRC.glob("*.done"):
        shutil.copy2(src, workdir / src.name)
    return workdir


def mock_state(
    directory: dir,
    config: str | dict | None = "match",
    chunks: int = 0,
    done: int = 0,
    output: str | None = None,
    record: dict | None = None,
) -> tuple[str | os.PathLike[str], dict]:
    """
    Build an on-disk run directory and return (directory, record).

    Args:
        directory (case_dir): parent directory; a fresh subdir is created under it
        config (str|dict|None): "match" -> .config.json equal to record
                dict    -> record updated with these overrides
                str     -> written verbatim (for malformed-JSON cases)
                None    -> no .config.json written
        chunks (int): number of chunks in the input; writes an input file with
                chunks * batch_size records
        done (int): number of finished chunks; writes <stem>_NNNNN.done for indices 0..done-1
        output(str): None -> no output file, "empty" -> touched, "full" -> one row
        record (dict): base record; defaults to BASE_RECORD

    Returns:
        (Path, dict)
    """

    record = dict(record or BASE_RECORD)
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)

    # input file sized so compute_ranges returns exactly `chunks` ranges
    input_file = directory / "input.ndjson"
    input_file.write_text(
        "".join(f'{{"id":{i}}}\n' for i in range(chunks * record["batch_size"])),
        encoding="utf-8",
    )
    record["input_file"] = str(input_file)

    out = pathlib.Path(directory) / "status.ndjson"
    record["output_file"] = str(out)

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

    stem = pathlib.Path(record["input_file"]).stem
    for i in range(done):
        (directory / f"{stem}_{i:05d}.done").write_text("{}\n", encoding="utf-8")

    if output == "empty":
        out.touch()
    elif output == "full":
        out.write_text('{"id":1}\n', encoding="utf-8")

    return directory, record

def read_ndjson(path: str | os.PathLike[str]) -> list[dict]:
    """
    Parse every non-blank line of an .ndjson file.

    Args:
        path (str): .ndjson file to read

    Returns:
        list[dict]: one dict per record, in file order
    """
    return [
        json.loads(line)
        for line in pathlib.Path(path).read_bytes().splitlines()
        if line.strip()
    ]


def write_config(directory: pathlib.Path, config: dict) -> pathlib.Path:
    """
    Write an authz config .yaml into directory.

    Args:
        directory (Path): where to write the config
        config (dict): config contents

    Returns:
        Path: path to the written config
    """
    path = directory / "config.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return path


def run_dirs(work_dir: pathlib.Path) -> list[pathlib.Path]:
    """
    List the run directories in a work directory.

    Args:
        work_dir (Path): work directory passed to the pipeline

    Returns:
        list[Path]: run directories, i.e. those containing a .config.json
    """
    return [p for p in work_dir.glob("*") if (p / ".config.json").is_file()]


GENDER_RULES = [
    {
        "resource_type": "Patient",
        "condition": "Patient.gender = 'male'",
        "authz": "/programs/Alpha/projects/Biobank",
    },
    {
        "resource_type": "Patient",
        "condition": "Patient.gender = 'female'",
        "authz": "/programs/Restricted/projects/Genomics",
    },
]

def test_fhir_output(case_dir: pathlib.Path) -> None:
    """Tests that output file matches the input file with the only difference being the tags"""
    output_file = case_dir / "fhir_output_Patient.ndjson"
    fin = read_ndjson(IN)
    src = read_ndjson(SRC)
    
    tag_fhir_resources_with_authz(
        input_file=IN,
        output_file=output_file,
        config=CONFIG_SRC,
        batch_size=BATCH_SIZE,
        work_dir=case_dir,
    )
    
    out = read_ndjson(output_file)

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


def test_compute_ranges():
    """Assert ranges cover the hwole input, hold batch_size records each, and recombine the input"""
    ranges = compute_ranges(IN, BATCH_SIZE)
    data = IN.read_bytes()
    fin = [json.loads(line) for line in data.splitlines() if line.strip()]
    assert len(ranges) == math.ceil(
        len(fin) / BATCH_SIZE
    ), "Unexpected number of ranges"
    assert ranges[0][0] == 0, "First range does not start at the beginning of the file"
    assert ranges[-1][1] == len(data), "Last range does not end at the end of the file"
    for (_, prev_end), (next_start, _) in zip(ranges, ranges[1:]):
        assert prev_end == next_start, "Gap or overlap between ranges"

    recombined = []
    for i, (s, e) in enumerate(ranges):
        records = [json.loads(line) for line in data[s:e].splitlines() if line.strip()]
        if i < len(ranges) - 1:
            assert (
                len(records) == BATCH_SIZE
            ), f"Range {i} does not hold batch_size records"
        recombined.extend(records)

    assert recombined == fin, "Ranges do not recombine to the input file"


def test_transform(tagger: Gen3FHIRAuthzTagger, transform_workdir: pathlib.Path) -> None:
    """Asserts transform_chunk one .done file per range, doesn't modify the input file, and no .tmp files remain once transformation is completed

    Args:
        tagger (Gen3FHIRAuthzTagger): The tagger instance to use for tagging the resources

    """
    resource_type = get_resource_type(IN)
    tagger.relevant_authz_rules(resource_type)
    before = IN.read_bytes()

    ranges = compute_ranges(IN, BATCH_SIZE)
    for i, (s, e) in enumerate(ranges):
        transform_chunk(IN, s, e, i, tagger, transform_workdir)
    done = sorted(list(transform_workdir.glob("*.done")))

    assert len(done) == len(
        ranges
    ), f"Expected {len(ranges)} .done files, found {len(done)}"
    assert not list(transform_workdir.glob("*.tmp")), "Temp files left behind"
    assert IN.read_bytes() == before, "Input file was modified"
    fin = [json.loads(line) for line in before.splitlines() if line.strip()]
    out = [
        json.loads(line)
        for p in done
        for line in p.read_bytes().splitlines()
        if line.strip()
    ]
    assert [r["id"] for r in out] == [
        r["id"] for r in fin
    ], "Records missing or out of order"


def test_merge(merge_workdir: pathlib.Path) -> None:
    """Asserts that after merge is completed, the output file exists and is not empty, there are no .done files remaining,
    the length of the output matches the sum of the transformed files and the input file, and the file is not corrupt/formatting is correct
    """
    out = merge_workdir / "merged.ndjson"
    transformed = list(merge_workdir.glob("*.done"))
    transformed_sum = sum(
        len([l for l in pathlib.Path(c).read_bytes().splitlines() if l.strip()])
        for c in transformed
    )
    merge_chunks(transformed, out)
    merged = pathlib.Path(out).read_text(encoding="utf-8")
    fin = read_ndjson(IN)
    fout = read_ndjson(out)

    # output file exists after merge
    assert os.path.exists(out), "Output file was not created"
    # output file is not empty after merge
    assert pathlib.Path(out).stat().st_size > 0, "Output file empty"
    # no leftover .done files after merge completed
    assert (
        len(list(merge_workdir.glob("*.done"))) == 0
    ), f"Expected 0 .done files after transformation completed, found {len(list(merge_workdir.glob('*.done')))}"
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

    def test_is_new_with_missing_directory(self, case_dir: pathlib.Path) -> None:
        """A missing run directory is new."""
        assert _is_new(case_dir / "does_not_exist", BASE_RECORD) is True

    def test_is_new_directory_has_no_config_file(self, case_dir: pathlib.Path) -> None:
        """A run directory without .config.json is new."""
        directory, record = mock_state(case_dir, config=None)
        assert _is_new(directory, record) is True

    def test_is_new_directory_has_corrupt_config_file(
        self, case_dir: pathlib.Path
    ) -> None:
        """A run directory with malformed .config.json is new."""
        directory, record = mock_state(case_dir, config="{not valid json")
        assert _is_new(directory, record) is True

    def test_is_new_directory_has_empty_config_file(
        self, case_dir: pathlib.Path
    ) -> None:
        """A run directory with an empty .config.json is new."""
        directory, record = mock_state(case_dir, config="")
        assert _is_new(directory, record) is True

    def test_is_not_new_when_config_matches(self, case_dir: pathlib.Path) -> None:
        """A run directory whose .config.json matches the run is not new."""
        directory, record = mock_state(case_dir, config="match")
        assert _is_new(directory, record) is False

    def test_is_new_when_config_changed(self, case_dir: pathlib.Path) -> None:
        """A changed config hash makes the run new."""
        directory, record = mock_state(case_dir, config={"config_hash": "different"})
        assert _is_new(directory, record) is True

    @pytest.mark.parametrize("batch_size", [0, 100, 50, 4])
    def test_is_new_when_batch_size_changed(
        self, case_dir: pathlib.Path, batch_size: int
    ) -> None:
        """A changed batch_size makes the run new."""
        directory, record = mock_state(case_dir, config={"batch_size": batch_size})
        assert _is_new(directory, record) is True

    def test_is_new_when_output_filename_changed(self, case_dir: pathlib.Path) -> None:
        """A changed output file makes the run new."""
        directory, record = mock_state(
            self.tmp_path, config={"output_file": "/tmp/somewhere_else.ndjson"}
            case_dir, config={"output_file": "/tmp/somewhere_else.ndjson"}
        )
        assert _is_new(directory, record) is True


class Test_is_done:
    """Tests the logic of the run/directory being marked as done (_is_done(directory, record) returns True)"""

    tmp_path = TMP_ROOT / "test_done"

    def test_is_done_with_matching_config_and_empty_output(
        self, case_dir: pathlib.Path
    ) -> None:
        """An empty output file is not done."""
        directory, record = mock_state(case_dir, config="match", output="empty")
        assert _is_done(directory, record) is False

    def test_is_done_with_matching_config_and_no_output(
        self, case_dir: pathlib.Path
    ) -> None:
        """A missing output file is not done."""
        directory, record = mock_state(case_dir, config="match", output=None)
        assert _is_done(directory, record) is False

    def test_is_done_with_no_config_and_full_output(
        self, case_dir: pathlib.Path
    ) -> None:
        """Output without a .config.json is not done."""
        directory, record = mock_state(case_dir, config=None, output="full")
        assert _is_done(directory, record) is False

    def test_is_done_with_corrupt_config_and_full_output(
        self, case_dir: pathlib.Path
    ) -> None:
        """Output with a malformed .config.json is not done."""
        directory, record = mock_state(case_dir, config="{bad", output="full")
        assert _is_done(directory, record) is False

    @pytest.mark.parametrize("batch_size", [0, 100, 50, 4])
    def test_is_done_with_full_output_and_different_batch_size(
        self, case_dir: pathlib.Path, batch_size: int
    ) -> None:
        """Output from a different batch_size is not done."""
        directory, record = mock_state(
            case_dir, config={"batch_size": batch_size}, output="full"
        )
        assert _is_done(directory, record) is False

    def test_is_done_with_full_output_and_different_config_hash(
        self, case_dir: pathlib.Path
    ) -> None:
        """Output from a different config is not done."""
        directory, record = mock_state(
            case_dir, config={"config_hash": "different"}, output="full"
        )
        assert _is_done(directory, record) is False

    def test_is_done_with_full_output_and_different_output_filename(
        self, case_dir: pathlib.Path
    ) -> None:
        """Output recorded for a different output file is not done."""
        directory, record = mock_state(
            case_dir,
            config={"output_file": "/tmp/somewhere_else.ndjson"},
            output="full",
        )
        assert _is_done(directory, record) is False

    def test_is_done_with_full_output_and_matching_config(
        self, case_dir: pathlib.Path
    ) -> None:
        """Non-empty output with a matching .config.json is done."""
        directory, record = mock_state(case_dir, config="match", output="full")
        assert _is_done(directory, record) is True


class Test_merge_needed:
    """Tests the logic of the run/directory being marked as merge needed (_merge_needed(directory, record) returns True)"""

    def test_merge_needed_when_no_done_files(self, case_dir: pathlib.Path) -> None:
        """No merge is needed before any chunk is transformed."""
        directory, record = mock_state(case_dir, chunks=5, done=0)
        assert _merge_needed(directory, record) is False

    @pytest.mark.parametrize("chunks", [1, 5, 20, 100])
    def test_merge_needed_when_all_chunks_done(
        self, case_dir: pathlib.Path, chunks: int
    ) -> None:
        """A merge is needed once every chunk has a .done file."""
        directory, record = mock_state(case_dir, chunks=chunks, done=chunks)
        assert _merge_needed(directory, record) is True

    @pytest.mark.parametrize(["chunks", "done"], [(5, 1), (5, 4), (20, 19), (100, 57)])
    def test_merge_not_needed_when_some_chunks_missing(
        self, case_dir: pathlib.Path, chunks: int, done: int
    ) -> None:
        """No merge is needed while some chunks lack a .done file."""
        directory, record = mock_state(case_dir, chunks=chunks, done=done)
        assert _merge_needed(directory, record) is False

    def test_merge_not_needed_when_last_chunk_only_has_tmp(
        self, case_dir: pathlib.Path
    ) -> None:
        """A partially written .done.tmp does not count as a finished chunk."""
        directory, record = mock_state(case_dir, chunks=5, done=4)
        (directory / "input_00004.done.tmp").write_text("{", encoding="utf-8")
        assert _merge_needed(directory, record) is False

    def test_merge_needed_when_directory_missing(self, case_dir: pathlib.Path) -> None:
        """No merge is needed when the run directory is missing."""
        assert _merge_needed(case_dir / "gone", BASE_RECORD) is False


class Test_status:
    """Tests for overlap in different statuses"""

    def test_fresh_directory(self, case_dir: pathlib.Path) -> None:
        """A fresh run directory is new, not done, and needs no merge."""
        directory, record = mock_state(case_dir, config=None)
        assert _is_new(directory, record) is True
        assert _is_done(directory, record) is False
        assert _merge_needed(directory, record) is False

    @pytest.mark.parametrize(
        ["chunks", "done_files"], [(5, 2), (5, 5), (5, 7), (1, 10)]
    )
    def test_need_to_resume_overlap(
        self, case_dir: pathlib.Path, chunks: int, done_files: int
    ) -> None:
        """A partially transformed run is neither new nor done."""
        directory, record = mock_state(
            case_dir, config="match", chunks=chunks, done=done_files
        )
        assert _is_new(directory, record) is False
        assert _is_done(directory, record) is False

    def test_merge_needed_overlap(self, case_dir: pathlib.Path) -> None:
        """A fully transformed run is not new, not done, and needs a merge."""
        directory, record = mock_state(case_dir, config="match", chunks=5, done=5)
        assert _is_new(directory, record) is False
        assert _is_done(directory, record) is False
        assert _merge_needed(directory, record) is True

    def test_run_complete(self, case_dir: pathlib.Path) -> None:
        """A completed run is done, not new, and needs no merge."""
        directory, record = mock_state(case_dir, config="match", output="full")
        assert _is_done(directory, record) is True
        assert _merge_needed(directory, record) is False
        assert _is_new(directory, record) is False

    @pytest.mark.parametrize(
        "state", ["match", None, {"config_hash": "x"}, {"batch_size": 1}]
    )
    def test_new_and_done_are_mutually_exclusive(
        self, case_dir: pathlib.Path, state: str | dict | None
    ) -> None:
        """A run is never both new and done."""
        directory, record = mock_state(case_dir, config=state, output="full")
        assert not (_is_new(directory, record) and _is_done(directory, record))

def test_rerun_after_interrupted_transform_completes_output(
    case_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run that fails partway through transforming resumes and produces the full output on rerun."""
    output_file = case_dir / "out.ndjson"
    original = Gen3FHIRAuthzTagger.determine_authz
    calls = {"n": 0}

    def fail_halfway(self: Gen3FHIRAuthzTagger, resource: dict) -> str:
        calls["n"] += 1
        if calls["n"] > N_CHUNKS // 2:
            raise RuntimeError("simulated crash")
        return original(self, resource)

    monkeypatch.setattr(Gen3FHIRAuthzTagger, "determine_authz", fail_halfway)
    with pytest.raises(RuntimeError):
        tag_fhir_resources_with_authz(
            IN, output_file, CONFIG_SRC, batch_size=BATCH_SIZE, work_dir=case_dir
        )
    monkeypatch.setattr(Gen3FHIRAuthzTagger, "determine_authz", original)

    tag_fhir_resources_with_authz(
        IN, output_file, CONFIG_SRC, batch_size=BATCH_SIZE, work_dir=case_dir
    )

    assert read_ndjson(output_file) == read_ndjson(SRC)


def test_rerun_after_interrupted_merge_completes_output(
    case_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run that fails partway through merging is not reported as done and produces the full output on rerun."""
    output_file = case_dir / "out.ndjson"
    original = shutil.copyfileobj
    calls = {"n": 0}

    def fail_on_second_chunk(*args: Any, **kwargs: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise OSError("simulated disk full")
        return original(*args, **kwargs)

    monkeypatch.setattr(shutil, "copyfileobj", fail_on_second_chunk)
    with pytest.raises(OSError):
        tag_fhir_resources_with_authz(
            IN, output_file, CONFIG_SRC, batch_size=BATCH_SIZE, work_dir=case_dir
        )
    monkeypatch.setattr(shutil, "copyfileobj", original)

    tag_fhir_resources_with_authz(
        IN, output_file, CONFIG_SRC, batch_size=BATCH_SIZE, work_dir=case_dir
    )

    assert read_ndjson(output_file) == read_ndjson(SRC)


def test_rerun_after_output_deleted_regenerates_output(case_dir: pathlib.Path) -> None:
    """Deleting the output of a completed run and rerunning regenerates the full output."""
    output_file = case_dir / "out.ndjson"
    tag_fhir_resources_with_authz(
        IN, output_file, CONFIG_SRC, batch_size=BATCH_SIZE, work_dir=case_dir
    )
    output_file.unlink()

    tag_fhir_resources_with_authz(
        IN, output_file, CONFIG_SRC, batch_size=BATCH_SIZE, work_dir=case_dir
    )

    assert read_ndjson(output_file) == read_ndjson(SRC)


def test_force_removes_run_dir_when_run_fails(case_dir: pathlib.Path) -> None:
    """With force=True, a failed run leaves no run directory behind."""
    work_dir = case_dir / "work"
    config = write_config(case_dir, {"rules": GENDER_RULES[:1]})

    with pytest.raises(ValueError):
        tag_fhir_resources_with_authz(
            IN, case_dir / "out.ndjson", config, work_dir=work_dir, force=True
        )

    assert not list(work_dir.iterdir())


def test_work_dir_defaults_to_env_var(
    case_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without an explicit work_dir, run directories go under GEN3_FHIR_WORK_DIR."""
    work_dir = case_dir / "env_work"
    monkeypatch.setenv("GEN3_FHIR_WORK_DIR", str(work_dir))

    tag_fhir_resources_with_authz(IN, case_dir / "out.ndjson", CONFIG_SRC)

    assert len(run_dirs(work_dir)) == 1


def test_same_input_and_output_is_rejected(case_dir: pathlib.Path) -> None:
    """Using the input file as the output file raises ValueError."""
    with pytest.raises(ValueError):
        tag_fhir_resources_with_authz(IN, IN, CONFIG_SRC, work_dir=case_dir)


def test_empty_input_is_rejected(case_dir: pathlib.Path) -> None:
    """An empty input file raises ValueError."""
    empty = case_dir / "Patient.ndjson"
    empty.touch()
    with pytest.raises(ValueError):
        tag_fhir_resources_with_authz(
            empty, case_dir / "out.ndjson", CONFIG_SRC, work_dir=case_dir
        )


def test_mixed_resource_types_are_rejected(case_dir: pathlib.Path) -> None:
    """An input file holding more than one resourceType raises ValueError."""
    mixed = case_dir / "Patient.ndjson"
    mixed.write_text(
        IN.read_text(encoding="utf-8")
        + json.dumps({"resourceType": "Observation", "id": "obs-1"})
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        tag_fhir_resources_with_authz(
            mixed, case_dir / "out.ndjson", CONFIG_SRC, work_dir=case_dir
        )


@pytest.mark.parametrize(
    "rules",
    [
        pytest.param(
            [
                {**GENDER_RULES[0], "condition": "Patient.gender.exists()"},
                *GENDER_RULES,
            ],
            id="overlapping_rules",
        ),
        pytest.param(GENDER_RULES[:1], id="no_matching_rule"),
        pytest.param(
            [{**GENDER_RULES[0], "resource_type": "Observation"}],
            id="no_rules_for_resource_type",
        ),
    ],
)
def test_untaggable_resources_are_rejected(
    case_dir: pathlib.Path, rules: list[dict]
) -> None:
    """A resource matching more than one rule, or no rule, raises ValueError."""
    config = write_config(case_dir, {"rules": rules})
    with pytest.raises(ValueError):
        tag_fhir_resources_with_authz(
            IN, case_dir / "out.ndjson", config, work_dir=case_dir
        )


def test_failed_run_writes_no_output(case_dir: pathlib.Path) -> None:
    """A run that fails on an untaggable resource leaves no output file behind."""
    output_file = case_dir / "out.ndjson"
    config = write_config(case_dir, {"rules": GENDER_RULES[:1]})
    with pytest.raises(ValueError):
        tag_fhir_resources_with_authz(IN, output_file, config, work_dir=case_dir)
    assert not output_file.exists()


def test_global_authz_without_rules_tags_every_resource(case_dir: pathlib.Path) -> None:
    """A config with only global_authz tags every resource with it."""
    output_file = case_dir / "out.ndjson"
    config = write_config(case_dir, {"global_authz": "/programs/A/projects/B"})

    tag_fhir_resources_with_authz(IN, output_file, config, work_dir=case_dir)

    assert {r["meta"]["security"][0]["code"] for r in read_ndjson(output_file)} == {
        "/programs/A/projects/B"
    }


class Test_cleanup:
    """Tests that cleanup removes run directories and nothing else"""

    @pytest.fixture
    def work_dir(self, case_dir: pathlib.Path) -> pathlib.Path:
        """A work dir holding one completed run dir and one unrelated user dir."""
        work_dir = case_dir / "work"
        tag_fhir_resources_with_authz(
            IN, case_dir / "out.ndjson", CONFIG_SRC, work_dir=work_dir
        )
        (work_dir / "unrelated").mkdir()
        (work_dir / "unrelated" / "keep.txt").write_text("keep", encoding="utf-8")
        return work_dir

    def test_cleanup_removes_run_dirs(self, work_dir: pathlib.Path) -> None:
        """Cleanup removes every run directory."""
        cleanup_fhir_transform_artifacts(work_dir)
        assert not run_dirs(work_dir)

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_cleanup_returns_run_dir_count(
        self, work_dir: pathlib.Path, dry_run: bool
    ) -> None:
        """Cleanup returns the number of run dirs removed, or that would be removed on a dry run."""
        assert cleanup_fhir_transform_artifacts(work_dir, dry_run=dry_run) == 1

    def test_cleanup_leaves_non_run_dirs(self, work_dir: pathlib.Path) -> None:
        """Cleanup does not touch directories it did not create."""
        cleanup_fhir_transform_artifacts(work_dir)
        assert (work_dir / "unrelated" / "keep.txt").is_file()

    def test_cleanup_force_leaves_non_empty_work_dir(
        self, work_dir: pathlib.Path
    ) -> None:
        """Cleanup with force does not remove a work dir that still holds other files."""
        cleanup_fhir_transform_artifacts(work_dir, force=True)
        assert (work_dir / "unrelated" / "keep.txt").is_file()

    def test_cleanup_force_removes_empty_work_dir(self, work_dir: pathlib.Path) -> None:
        """Cleanup with force removes the work dir once only run dirs were in it."""
        shutil.rmtree(work_dir / "unrelated")
        cleanup_fhir_transform_artifacts(work_dir, force=True)
        assert not work_dir.exists()

    def test_cleanup_dry_run_removes_nothing(self, work_dir: pathlib.Path) -> None:
        """Cleanup with dry_run removes no run dirs."""
        cleanup_fhir_transform_artifacts(work_dir, dry_run=True)
        assert len(run_dirs(work_dir)) == 1


def test_cli(case_dir: pathlib.Path) -> None:
    """Run the CLI and return the CompletedProcess."""
    out = case_dir / "cli_out.ndjson"
    args = [
        IN,
        out,
        CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
        "--work_dir",
        case_dir,
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, "CLI run failed"
    assert pathlib.Path(out).exists(), "CLI exited 0 but wrote no output file"
    records = read_ndjson(out)
    in_records = read_ndjson(IN)
    assert len(records) == len(
        in_records
    ), "Output number of records doesn't match the input number of records"
    assert all(
        "security" in r.get("meta", {}) for r in records
    ), "Security tags missing"


def test_cli_rejects_same_input_and_output(case_dir: pathlib.Path) -> None:
    """The CLI exits with a usage error, not a crash, when input and output are the same file."""
    result = subprocess.run(
        ["gen3", "fhir", "transform", IN, IN, CONFIG_SRC, "--work_dir", case_dir],
        capture_output=True,
        text=True,
        timeout=60,
    )
    # click exits 2 for a UsageError, an uncaught exception exits 1
    assert result.returncode == 2

@pytest.mark.parametrize("bad", [0, -1, None, "bad"])
def test_invalid_batch_size_is_rejected(
    case_dir: pathlib.Path, bad: int | str | None
) -> None:
    """Assert invalid batch_size is rejected and raises an error

    Args:
        bad (int|str|None): bad inputs for batch_size

    """
    out = case_dir / "cli_out.ndjson"
    args = [
        IN,
        out,
        CONFIG_SRC,
        "--batch_size",
        str(bad),
        "--work_dir",
        case_dir,
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


def test_missing_input_file_fails_cleanly(case_dir: pathlib.Path) -> None:
    """Asserts error is raised if missing input file passed as argument"""
    out = case_dir / "nope.ndjson"
    args = [
        "nope.ndjson",
        out,
        CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
        "--work_dir",
        case_dir,
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


def test_output_directory_does_not_exist(case_dir: pathlib.Path) -> None:
    """Test response if output directory doesn't exists. Directory (and parents) should be created if missing"""
    out = case_dir / "missing_dir" / "out.ndjson"
    args = [
        IN,
        out,
        CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
        "--work_dir",
        case_dir,
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, "Raises error instead of creating output directory"
    assert pathlib.Path(out).exists(), "Process not completed, output file not written"


def test_global_config_overrides_other_rules(case_dir: pathlib.Path) -> None:
    """Assert global authorization overrides any other rules"""
    out = case_dir / "out.ndjson"
    args = [
        IN,
        out,
        GLOBAL_CONFIG_SRC,
        "--batch_size",
        str(BATCH_SIZE),
        "--work_dir",
        case_dir,
    ]
    result = subprocess.run(
        ["gen3", "fhir", "transform", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, "Run fails"
    assert pathlib.Path(out).exists(), "Process not completed, output file not written"
    tagged_file = read_ndjson(out)
    with open(GLOBAL_CONFIG_SRC, "r") as f:
        config = yaml.safe_load(f)

    for rec in tagged_file:
        security = (rec.get("meta")).get("security")[0].get("code")
        assert (
            security == config["global_authz"]
        ), "File not tagged with global authorization"
