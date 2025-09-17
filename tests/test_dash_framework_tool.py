import shutil
from pathlib import Path

import pandas as pd
import pytest

from altinn.dash_framework_tool import AltinnFormProcessor


class FakeConn:
    """Minimal fake for eimerdb.EimerDBInstance."""

    def __init__(self, existing_return):
        """Init method."""
        self._existing_return = existing_return
        self.insert_calls = []  # list[tuple[str, pd.DataFrame]]
        self.queries = []

    def query(self, sql: str):
        self.queries.append(sql)
        return self._existing_return

    def insert(self, table: str, df: pd.DataFrame) -> None:
        # Store a copy to avoid accidental mutation by caller
        self.insert_calls.append((table, df.copy(deep=True)))


@pytest.fixture()
def processor_factory(monkeypatch, tmp_path):
    """Factory to create a processor with a mocked DB connection.

    Returns a function that accepts `existing_return` for the fake connection
    and returns (processor, fake_conn).
    """

    def _factory(existing_return=None):
        if existing_return is None:
            existing_return = pd.DataFrame()
        # Monkeypatch the EimerDBInstance constructor to return our FakeConn
        fake_conn = FakeConn(existing_return)

        def _fake_ctor(storage, db_name):  # signature per usage in __init__
            return fake_conn

        monkeypatch.setattr(
            "altinn.dash_framework_tool.db.EimerDBInstance", _fake_ctor, raising=True
        )
        proc = AltinnFormProcessor(
            database_name="testdb",
            storage_location=str(tmp_path),
            ra_number="RA-0000",
            delreg_nr="00000",
            xml_period_mapping={"aar": "periodeAAr", "mnd": "periodeNummer"},
            suv_period_mapping={"aar": "year", "mnd": "month"},
            path_to_form_folder=str(tmp_path),
        )
        return proc, fake_conn

    return _factory


@pytest.fixture()
def xml_json_files(tmp_path):
    """Create an XML + JSON pair using real files from the test data directory."""
    # Use the actual XML and JSON files
    xml_source = Path(__file__).parent / "data" / "form_373a35bb8808.xml"
    json_source = Path(__file__).parent / "data" / "meta_373a35bb8808.json"
    xml_path = tmp_path / "form_abc.xml"
    json_path = tmp_path / "meta_abc.json"
    shutil.copy(xml_source, xml_path)
    shutil.copy(json_source, json_path)
    return str(xml_path), str(json_path)


def test_insert_into_database_inserts_new_rows(processor_factory):
    proc, fake = processor_factory(existing_return=pd.DataFrame(columns=["aar", "mnd"]))
    df = pd.DataFrame(
        [
            {"aar": 2023, "mnd": 6, "value": "x"},
        ]
    )

    proc.insert_into_database(df, keys=["aar", "mnd"], table_name="mytable")

    assert len(fake.insert_calls) == 1
    table, inserted = fake.insert_calls[0]
    assert table == "mytable"
    # The merge adds an _merge column and then filters; ensure our original rows made it through
    pd.testing.assert_frame_equal(
        inserted[["aar", "mnd", "value"]].reset_index(drop=True),
        df[["aar", "mnd", "value"]].reset_index(drop=True),
        check_dtype=False,
    )


def test_insert_into_database_skips_duplicates(processor_factory):
    existing = pd.DataFrame(
        [
            {"aar": 2023, "mnd": 6},
        ]
    )
    proc, fake = processor_factory(existing_return=existing)
    df = pd.DataFrame(
        [
            {"aar": 2023, "mnd": 6, "value": "x"},
        ]
    )

    proc.insert_into_database(df, keys=["aar", "mnd"], table_name="mytable")

    assert len(fake.insert_calls) == 0


def test_insert_into_database_invalid_existing_type_raises(processor_factory):
    proc, _ = processor_factory(
        existing_return=[{"aar": 2023, "mnd": 6}]
    )  # not a DataFrame
    df = pd.DataFrame(
        [
            {"aar": 2023, "mnd": 6, "value": "x"},
        ]
    )

    with pytest.raises(ValueError):
        proc.insert_into_database(df, keys=["aar", "mnd"], table_name="mytable")


def test_extract_json_success(processor_factory, xml_json_files):
    proc, _ = processor_factory()
    _, json_path = xml_json_files
    proc.json_path = json_path

    out = proc.extract_json()

    assert set(out.columns) == {"dato_mottatt", "refnr"}
    assert out.loc[0, "refnr"] == "373a35bb8808"
    assert str(out.loc[0, "dato_mottatt"]).startswith("2024-04-29T06:31:17")


def test_extract_json_raises_when_path_none(processor_factory):
    proc, _ = processor_factory()
    proc.json_path = None
    with pytest.raises(ValueError):
        proc.extract_json()


def test_extract_skjemamottak_xml_and_extract_skjemamottak(
    processor_factory, xml_json_files
):
    proc, _ = processor_factory()
    xml_path, json_path = xml_json_files
    proc.xml_path = xml_path
    proc.json_path = json_path

    xml_df = proc.extract_skjemamottak_xml()

    assert set(["skjema", "ident", "aar", "mnd"]).issubset(xml_df.columns)
    assert xml_df.shape[0] == 1
    # ident should be cast to string of integers
    assert xml_df.loc[0, "ident"] == "999999994"

    full_df = proc.extract_skjemamottak()
    # Expected columns order
    expected_cols = [
        "aar",
        "mnd",
        "skjema",
        "ident",
        "refnr",
        "dato_mottatt",
        "editert",
        "kommentar",
        "aktiv",
    ]
    assert list(full_df.columns) == expected_cols
    assert full_df.loc[0, "aar"] == 2023
    assert full_df.loc[0, "mnd"] == 6
    assert full_df.loc[0, "ident"] == "999999994"
    # Time should be pandas Timestamp floored to seconds
    assert isinstance(full_df.loc[0, "dato_mottatt"], pd.Timestamp)
    assert full_df.loc[0, "dato_mottatt"].microsecond == 0
    assert not full_df.loc[0, "editert"]
    assert full_df.loc[0, "aktiv"]
    assert full_df.loc[0, "kommentar"] == ""


def test_extract_kontaktinfo(processor_factory, xml_json_files):
    proc, _ = processor_factory()
    xml_path, json_path = xml_json_files
    proc.xml_path = xml_path
    proc.json_path = json_path

    df = proc.extract_kontaktinfo()

    expected_columns = {
        "aar",
        "mnd",
        "ident",
        "skjema",
        "kontaktperson",
        "epost",
        "telefon",
        "bekreftet_kontaktinfo",
        "kommentar_kontaktinfo",
        "kommentar_krevende",
        "refnr",
        "dato_mottatt",
    }
    assert expected_columns.issubset(df.columns)
    assert df.shape[0] == 1
    # Types and values
    assert df.loc[0, "ident"] == "999999994"
    assert df.loc[0, "telefon"] == "21090000"
    assert df.loc[0, "bekreftet_kontaktinfo"] == "1"
    assert df.loc[0, "aar"] == 2023
    assert df.loc[0, "mnd"] == 6


def test_insert_helpers_call_insert_into_database(
    processor_factory, xml_json_files, monkeypatch
):
    proc, _ = processor_factory()
    xml_path, json_path = xml_json_files
    proc.xml_path = xml_path
    proc.json_path = json_path

    calls = {}

    def fake_insert_into_database(data, keys, table_name):
        calls["data"] = data.copy()
        calls["keys"] = list(keys)
        calls["table_name"] = table_name

    monkeypatch.setattr(proc, "insert_into_database", fake_insert_into_database)

    # skjemamottak
    proc.process_skjemamottak()
    assert calls["table_name"] == "skjemamottak"
    assert calls["keys"] == ["aar", "mnd", "skjema", "refnr"]
    assert {"aar", "mnd", "skjema", "ident", "refnr"}.issubset(calls["data"].columns)

    # kontaktinfo
    calls.clear()
    proc.process_kontaktinfo()
    assert calls["table_name"] == "kontaktinfo"
    assert calls["keys"] == ["aar", "mnd", "skjema", "refnr"]
    assert {"aar", "mnd", "skjema", "ident", "refnr"}.issubset(calls["data"].columns)
