import pathlib

import pandas as pd
import pytest

from altinn.flatten import _attach_metadata


def test_attach_metadata_returns_dataframe(
    monkeypatch: "pytest.MonkeyPatch", tmp_path: "pathlib.Path"
) -> None:
    # Prepare a fake JSON file with metadata
    meta_content = {
        "ALTINNTIDSPUNKTLEVERT": "2025-03-20T15:54:40.637Z",
        "OTHER_FIELD": "value",
    }
    json_file = tmp_path / "meta_test.json"
    json_file.write_text(
        '{"ALTINNTIDSPUNKTLEVERT": "2025-03-20T15:54:40.637Z", "OTHER_FIELD": "value"}',
        encoding="utf-8",
    )
    # Patch _read_json_meta to return our dict
    monkeypatch.setattr("altinn.flatten._read_json_meta", lambda fp: meta_content)
    df = _attach_metadata(str(json_file))
    assert isinstance(df, pd.DataFrame)
    assert "FELTNAVN" in df.columns
    assert "FELTVERDI" in df.columns
    assert "ALTINNTIDSPUNKTLEVERT" in df["FELTNAVN"].values
    assert "OTHER_FIELD" in df["FELTNAVN"].values


def test_attach_metadata_returns_empty_dataframe(
    monkeypatch: "pytest.MonkeyPatch",
) -> None:
    # Patch _read_json_meta to return None
    monkeypatch.setattr("altinn.flatten._read_json_meta", lambda fp: None)
    df = _attach_metadata("dummy_path")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_attach_metadata_handles_empty_dict(monkeypatch: "pytest.MonkeyPatch") -> None:
    # Patch _read_json_meta to return empty dict
    monkeypatch.setattr("altinn.flatten._read_json_meta", lambda fp: {})
    df = _attach_metadata("dummy_path")
    assert isinstance(df, pd.DataFrame)
    assert df.empty
