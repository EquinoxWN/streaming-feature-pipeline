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
