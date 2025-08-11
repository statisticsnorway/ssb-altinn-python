import re
from typing import Any

import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch

from altinn.flatten import _add_interninfo_columns


def dummy_extract_angiver_id(file_path: str) -> str:
    return "12345"


def dummy_extract_counter(value: str) -> list[str]:
    # Find all digits that appear between £ and $
    matches: list[str] = re.findall(r"£(\d+)\$", value)
    return matches


def dummy_create_levels_col(row: dict[str, Any]) -> int:
    # Simulate LEVELS calculation
    counter: Any = row.get("COUNTER", [])
    if isinstance(counter, list) and len(counter) > 1:
        return 2
    elif isinstance(counter, list) and len(counter) == 1:
        return 1
    else:
        return 0


@pytest.fixture
def patch_helpers(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr("altinn.flatten._extract_angiver_id", dummy_extract_angiver_id)
    monkeypatch.setattr("altinn.flatten._extract_counter", dummy_extract_counter)
    monkeypatch.setattr("altinn.flatten._create_levels_col", dummy_create_levels_col)


@pytest.fixture
def sample_xml_dict() -> dict[str, Any]:
    return {
        "Root": {
            "InternInfo": {
                "enhetsIdent": "ENH123",
                "delregNr": "DR456",
                "enhetsType": "TYPE789",
                "raNummer": "RA321",
            }
        }
    }


def test_add_interninfo_columns_basic(
    patch_helpers: None, sample_xml_dict: dict[str, Any]
) -> None:
    df: pd.DataFrame = pd.DataFrame(
        {
            "FELTNAVN": ["field1£1$", "field2£2$ £1$", "field3", "field4@xsi:nil"],
            "FELTVERDI": ["val1", "val2\nwithnewline", "val3", "val4"],
        }
    )
    result: pd.DataFrame = _add_interninfo_columns(
        df, sample_xml_dict, "Root", "dummy_path.xml"
    )
    # Check columns added
    assert set(
        [
            "IDENT_NR",
            "VERSION_NR",
            "DELREG_NR",
            "ENHETS_TYPE",
            "SKJEMA_ID",
            "COUNTER",
            "LEVELS",
        ]
    ).issubset(result.columns)
    # Check values
    assert (result["IDENT_NR"] == "ENH123").all()
    assert (result["DELREG_NR"] == "DR456").all()
    assert (result["ENHETS_TYPE"] == "TYPE789").all()
    assert (result["SKJEMA_ID"] == "RA321A3").all()
    assert (result["VERSION_NR"] == "12345").all()
    # Check FELTNAVN cleaned
    assert all("£" not in v for v in result["FELTNAVN"])
    # Check FELTVERDI cleaned
    assert all("\n" not in v for v in result["FELTVERDI"])
    # Check @xsi:nil row removed
    assert not any(result["FELTNAVN"].str.contains("@xsi:nil"))
    # Check COUNTER and LEVELS
    assert result.loc[result["FELTNAVN"] == "field1", "COUNTER"].iloc[0] == ["1"]
    assert result.loc[result["FELTNAVN"] == "field2", "COUNTER"].iloc[0] == ["2", "1"]
    assert result.loc[result["FELTNAVN"] == "field1", "LEVELS"].iloc[0] == 1
    assert result.loc[result["FELTNAVN"] == "field2", "LEVELS"].iloc[0] == 2


def test_add_interninfo_columns_empty_df(
    patch_helpers: None, sample_xml_dict: dict[str, Any]
) -> None:
    df: pd.DataFrame = pd.DataFrame(columns=["FELTNAVN", "FELTVERDI"])
    result: pd.DataFrame = _add_interninfo_columns(
        df, sample_xml_dict, "Root", "dummy_path.xml"
    )
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    # Should still have the new columns
    for col in [
        "IDENT_NR",
        "VERSION_NR",
        "DELREG_NR",
        "ENHETS_TYPE",
        "SKJEMA_ID",
        "COUNTER",
        "LEVELS",
    ]:
        assert col in result.columns


def test_add_interninfo_columns_missing_feltverdi(
    patch_helpers: None, sample_xml_dict: dict[str, Any]
) -> None:
    df: pd.DataFrame = pd.DataFrame({"FELTNAVN": ["field1£1$", "field2£2$ £1$"]})
    result: pd.DataFrame = _add_interninfo_columns(
        df, sample_xml_dict, "Root", "dummy_path.xml"
    )
    assert "FELTVERDI" not in result.columns or all(
        "\n" not in str(v) for v in result.get("FELTVERDI", [])
    )
