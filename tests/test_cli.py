"""clickgen writes a stream and summarises it."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from streaming_feature_pipeline.cli import main
from streaming_feature_pipeline.events import ClickEvent


def test_generate_to_a_file_and_summarise(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "clicks.jsonl"
    assert main(["--events", "3000", "--out", str(out), "--late-fraction", "0.02"]) == 0
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3000
    ClickEvent.from_json(lines[0])
    summary = json.loads(capsys.readouterr().out)
    assert summary["events"] == 3000
    assert summary["side_output"] > 0
    assert summary["late_firings"] >= 0


@pytest.mark.parametrize(
    "args",
    [
        ["--events", "0"],
        ["--events", "-5"],
        ["--late-fraction", "1.5"],
        ["--late-fraction", "-0.1"],
        ["--window-min", "10", "--slide-min", "3"],
        ["--bound-s", "-1"],
    ],
)
def test_invalid_arguments_exit_2_with_a_message(
    args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as stop:
        main(args)
    assert stop.value.code == 2
    assert "clickgen: error:" in capsys.readouterr().err


def test_a_rerun_replaces_the_output_file_whole(tmp_path: Path) -> None:
    out = tmp_path / "clicks.jsonl"
    assert main(["--events", "500", "--out", str(out)]) == 0
    first = out.read_text(encoding="utf-8")
    assert main(["--events", "500", "--out", str(out)]) == 0
    assert out.read_text(encoding="utf-8") == first, "same seed, same file: safe to re-run"
    assert [p.name for p in tmp_path.iterdir()] == ["clicks.jsonl"], "no temp file is left behind"
