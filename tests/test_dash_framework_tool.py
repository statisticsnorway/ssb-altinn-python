import json

import pandas as pd
import pytest

from altinn.dash_framework_tool import AltinnEimerdbProcessor


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
        proc = AltinnEimerdbProcessor(
            database_name="testdb",
            storage_location=str(tmp_path),
            ra_number="RA-0000",
            delreg_nr="00000",
            xml_period_mapping={"aar": "year", "mnd": "month"},
            suv_period_mapping={"aar": "year", "mnd": "month"},
            path_to_form_folder=str(tmp_path),
        )
        return proc, fake_conn

    return _factory


@pytest.fixture()
def xml_json_files(tmp_path):
    """Create a minimal XML + JSON pair compatible with the processor methods."""
    xml_content = """
    <root>
      <row>
        <raNummer>RA-0571</raNummer>
        <reporteeOrgNr>123456789</reporteeOrgNr>
        <year>2024</year>
        <month>4</month>
        <kontaktPersonNavn>Ola Nordmann</kontaktPersonNavn>
        <kontaktPersonEpost>ola@example.com</kontaktPersonEpost>
        <kontaktPersonTelefon>99887766</kontaktPersonTelefon>
        <kontaktInfoBekreftet>1</kontaktInfoBekreftet>
        <kontaktInfoKommentar>Alt ok</kontaktInfoKommentar>
        <forklarKrevendeForh>Ingen</forklarKrevendeForh>
      </row>
    </root>
    """.strip()
    xml_path = tmp_path / "form_abc.xml"
    xml_path.write_text(xml_content, encoding="utf-8")

    json_payload = {
        "altinnReferanse": "abc",
        "altinnTidspunktLevert": "2024-04-29T06:31:17.3678585Z",
    }
    json_path = tmp_path / "meta_abc.json"
    json_path.write_text(json.dumps(json_payload), encoding="utf-8")

    return str(xml_path), str(json_path)


def test_insert_into_eimerdb_inserts_new_rows(processor_factory):
    proc, fake = processor_factory(existing_return=pd.DataFrame(columns=["aar", "mnd"]))
    df = pd.DataFrame(
        [
            {"aar": 2024, "mnd": 4, "value": "x"},
        ]
    )

    proc.insert_into_eimerdb(df, keys=["aar", "mnd"], table_name="mytable")

    assert len(fake.insert_calls) == 1
    table, inserted = fake.insert_calls[0]
    assert table == "mytable"
    # The merge adds an _merge column and then filters; ensure our original rows made it through
    pd.testing.assert_frame_equal(
        inserted[["aar", "mnd", "value"]].reset_index(drop=True),
        df[["aar", "mnd", "value"]].reset_index(drop=True),
        check_dtype=False,
    )


def test_insert_into_eimerdb_skips_duplicates(processor_factory):
    existing = pd.DataFrame(
        [
            {"aar": 2024, "mnd": 4},
        ]
    )
    proc, fake = processor_factory(existing_return=existing)
    df = pd.DataFrame(
        [
            {"aar": 2024, "mnd": 4, "value": "x"},
        ]
    )

    proc.insert_into_eimerdb(df, keys=["aar", "mnd"], table_name="mytable")

    assert len(fake.insert_calls) == 0


def test_insert_into_eimerdb_invalid_existing_type_raises(processor_factory):
    proc, _ = processor_factory(
        existing_return=[{"aar": 2024, "mnd": 4}]
    )  # not a DataFrame
    df = pd.DataFrame(
        [
            {"aar": 2024, "mnd": 4, "value": "x"},
        ]
    )

    with pytest.raises(ValueError):
        proc.insert_into_eimerdb(df, keys=["aar", "mnd"], table_name="mytable")


def test_extract_json_success(processor_factory, xml_json_files):
    proc, _ = processor_factory()
    _, json_path = xml_json_files
    proc.json_path = json_path

    out = proc.extract_json()

    assert set(out.columns) == {"dato_mottatt", "refnr"}
    assert out.loc[0, "refnr"] == "abc"
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

    assert set("skjema", "ident", "aar", "mnd").issubset(xml_df.columns)
    assert xml_df.shape[0] == 1
    # ident should be cast to string of integers
    assert xml_df.loc[0, "ident"] == "123456789"

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
    assert full_df.loc[0, "aar"] == 2024
    assert full_df.loc[0, "mnd"] == 4
    assert full_df.loc[0, "ident"] == "123456789"
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
    assert df.loc[0, "ident"] == "123456789"
    assert df.loc[0, "telefon"] == "99887766"
    assert df.loc[0, "bekreftet_kontaktinfo"] == "1"
    assert df.loc[0, "aar"] == 2024
    assert df.loc[0, "mnd"] == 4


def test_insert_helpers_call_insert_into_eimerdb(
    processor_factory, xml_json_files, monkeypatch
):
    proc, _ = processor_factory()
    xml_path, json_path = xml_json_files
    proc.xml_path = xml_path
    proc.json_path = json_path

    calls = {}

    def fake_insert_into_eimerdb(data, keys, table_name):
        calls["data"] = data.copy()
        calls["keys"] = list(keys)
        calls["table_name"] = table_name

    monkeypatch.setattr(proc, "insert_into_eimerdb", fake_insert_into_eimerdb)

    # skjemamottak
    proc.table_skjemamottak()
    assert calls["table_name"] == "skjemamottak"
    assert calls["keys"] == ["aar", "mnd", "skjema", "refnr"]
    assert {"aar", "mnd", "skjema", "ident", "refnr"}.issubset(calls["data"].columns)

    # kontaktinfo
    calls.clear()
    proc.table_kontaktinfo()
    assert calls["table_name"] == "kontaktinfo"
    assert calls["keys"] == ["aar", "mnd", "skjema", "refnr"]
    assert {"aar", "mnd", "skjema", "ident", "refnr"}.issubset(calls["data"].columns)
