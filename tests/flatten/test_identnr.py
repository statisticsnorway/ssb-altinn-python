import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch

from altinn.flatten import _add_interninfo_columns


# Dummy helpers
def dummy_check_altinn_type(file_path: str) -> str:
    return "RA"  # Simulate returning "RA"


def dummy_extract_angiver_id(file_path: str) -> str:
    return "12345"


def dummy_extract_counter(value: str):
    return []


def dummy_create_levels_col(row):
    return 0


@pytest.fixture
def patch_helpers(monkeypatch: MonkeyPatch):
    monkeypatch.setattr("altinn.flatten._check_altinn_type", dummy_check_altinn_type)
    monkeypatch.setattr("altinn.flatten._extract_angiver_id", dummy_extract_angiver_id)
    monkeypatch.setattr("altinn.flatten._extract_counter", dummy_extract_counter)
    monkeypatch.setattr("altinn.flatten._create_levels_col", dummy_create_levels_col)


@pytest.fixture
def sample_xml_dict():
    return {
        "Root": {
            "InternInfo": {
                "enhetsIdent": "ENH123",
                "enhetOrgnr": "ORG456",
                "delregNr": "DR789",
                "enhetsType": "TYPE001",
                "raNummer": "RA321",
            }
        }
    }


def test_ident_nr_column(patch_helpers, sample_xml_dict):
    df = pd.DataFrame({"FELTNAVN": ["field1", "field2"]})
    result = _add_interninfo_columns(df, sample_xml_dict, "Root", "dummy_path.xml")

    # _check_altinn_type returns "RA", so IDENT_NR should be enhetsIdent
    expected = ["ENH123", "ENH123"]
    assert result["IDENT_NR"].tolist() == expected


def test_ident_nr_column_rs(monkeypatch, sample_xml_dict):
    # Patch _check_altinn_type to return "RS"
    monkeypatch.setattr("altinn.flatten._check_altinn_type", lambda x: "RS")
    monkeypatch.setattr("altinn.flatten._extract_angiver_id", lambda x: "12345")
    monkeypatch.setattr("altinn.flatten._extract_counter", lambda x: [])
    monkeypatch.setattr("altinn.flatten._create_levels_col", lambda row: 0)

    df = pd.DataFrame({"FELTNAVN": ["field1", "field2"]})
    result = _add_interninfo_columns(df, sample_xml_dict, "Root", "dummy_path.xml")

    # _check_altinn_type returns "RS", so IDENT_NR should be enhetOrgnr
    expected = ["ORG456", "ORG456"]
    assert result["IDENT_NR"].tolist() == expected


def test_ident_nr_column_invalid(monkeypatch, sample_xml_dict):
    # Patch _check_altinn_type to return something invalid
    monkeypatch.setattr("altinn.flatten._check_altinn_type", lambda x: "XX")
    monkeypatch.setattr("altinn.flatten._extract_angiver_id", lambda x: "12345")
    monkeypatch.setattr("altinn.flatten._extract_counter", lambda x: [])
    monkeypatch.setattr("altinn.flatten._create_levels_col", lambda row: 0)

    df = pd.DataFrame({"FELTNAVN": ["field1", "field2"]})

    # Expect KeyError because "innvalid" is not in InternInfo
    with pytest.raises(KeyError):
        _add_interninfo_columns(df, sample_xml_dict, "Root", "dummy_path.xml")
