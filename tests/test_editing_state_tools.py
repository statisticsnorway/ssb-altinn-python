import os
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from altinn.editing_state_tools import AltinnFormProcessor
from altinn.editing_state_tools import xml_to_parquet


class FakeConn:
    """Minimal fake for eimerdb.EimerDBInstance."""

    def __init__(self, existing_return: dict[str, pd.DataFrame] | None = None) -> None:
        """Init method."""
        self._existing_return = existing_return or {}
        self.insert_calls: list[tuple[str, pd.DataFrame]] = []
        self.queries: list[str] = []

    def query(self, sql: str) -> Any:
        self.queries.append(sql)
        # Extract table name from SQL
        table = sql.split()[-1]
        return self._existing_return.get(table, pd.DataFrame())

    def insert(self, table: str, df: pd.DataFrame) -> None:
        self.insert_calls.append((table, df.copy(deep=True)))


@pytest.fixture()
def processor_factory(monkeypatch, tmp_path):
    """Factory to create a processor with a mocked DB connection.

    Returns a function that accepts `existing_return` for the fake connection
    and returns (processor, fake_conn).
    """

    def _factory(existing_return=None):
        # Default: empty DataFrames with correct columns for each table
        if existing_return is None:
            existing_return = {
                "skjemamottak": pd.DataFrame(columns=["aar", "mnd", "skjema", "refnr"]),
                "kontaktinfo": pd.DataFrame(columns=["aar", "mnd", "skjema", "refnr"]),
                "enheter": pd.DataFrame(columns=["aar", "mnd", "ident", "skjema"]),
            }
        fake_conn = FakeConn(existing_return)

        def _fake_ctor(storage, db_name):
            return fake_conn

        monkeypatch.setattr(
            "altinn.editing_state_tools.db.EimerDBInstance", _fake_ctor, raising=True
        )
        proc = AltinnFormProcessor(
            database_name="testdb",
            storage_location=str(tmp_path),
            ra_number="RA-0000",
            delreg_nr="00000",
            parquet_ident_field="ReporteeOrgnr",
            parquet_period_mapping={"aar": "periodeAAr", "mnd": "periodeNummer"},
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


@pytest.fixture()
def minimal_parquet(tmp_path):
    # Create a minimal DataFrame in the expected long format
    df = pd.DataFrame(
        [
            {"FELTNAVN": "periodeAAr", "FELTVERDI": 2023},
            {"FELTNAVN": "periodeNummer", "FELTVERDI": 6},
            {"FELTNAVN": "InternInfo_raNummer", "FELTVERDI": "RA-0000"},
            {"FELTNAVN": "reporteeOrgNr", "FELTVERDI": "999999994"},
            {"FELTNAVN": "altinnReferanse", "FELTVERDI": "373a35bb8808"},
            {"FELTNAVN": "altinnTidspunktLevert", "FELTVERDI": "2024-04-29T06:31:17"},
            {"FELTNAVN": "Kontakt_kontaktPersonNavn", "FELTVERDI": "Test Person"},
            {"FELTNAVN": "Kontakt_kontaktPersonTelefon", "FELTVERDI": "21090000"},
            {"FELTNAVN": "Kontakt_kontaktPersonEpost", "FELTVERDI": "test@example.com"},
            {"FELTNAVN": "Kontakt_kontaktInfoBekreftet", "FELTVERDI": "1"},
            {"FELTNAVN": "Kontakt_kontaktInfoKommentar", "FELTVERDI": ""},
            {"FELTNAVN": "forklarKrevendeForh", "FELTVERDI": ""},
        ],
        dtype=object,
    )
    df["FELTVERDI"] = df["FELTVERDI"].astype(str)
    parquet_path = tmp_path / "form1.parquet"
    df.to_parquet(parquet_path)
    return str(parquet_path)


def test_insert_helpers_call_insert_or_save_data(
    processor_factory, minimal_parquet, monkeypatch
):
    proc, _ = processor_factory()
    proc.data = pd.read_parquet(minimal_parquet)
    calls = {}

    def fake_insert_or_save_data(data, keys, table_name):
        calls["data"] = data.copy()
        calls["keys"] = list(keys)
        calls["table_name"] = table_name

    monkeypatch.setattr(proc, "insert_or_save_data", fake_insert_or_save_data)

    # skjemamottak
    proc.process_skjemamottak()
    assert calls["table_name"] == "skjemamottak"
    assert set(["aar", "mnd", "skjema", "ident", "refnr"]).issubset(
        calls["data"].columns
    )

    # kontaktinfo
    calls.clear()
    proc.process_kontaktinfo()
    assert calls["table_name"] == "kontaktinfo"
    assert set(["aar", "mnd", "skjema", "ident", "refnr"]).issubset(
        calls["data"].columns
    )


def test_process_skjemamottak_and_kontaktinfo(processor_factory, minimal_parquet):
    proc, fake = processor_factory()
    proc.data = pd.read_parquet(minimal_parquet)
    # Test skjemamottak
    proc.process_skjemamottak()
    assert fake.insert_calls
    table, inserted = fake.insert_calls[0]
    assert table == "skjemamottak"
    assert "skjema" in inserted.columns
    assert "ident" in inserted.columns
    assert "refnr" in inserted.columns
    assert "dato_mottatt" in inserted.columns
    # Test kontaktinfo
    fake.insert_calls.clear()
    proc.process_kontaktinfo()
    assert fake.insert_calls
    table, inserted = fake.insert_calls[0]
    assert table == "kontaktinfo"
    assert "kontaktperson" in inserted.columns
    assert inserted.loc[0, "telefon"] == "21090000"
    assert inserted.loc[0, "bekreftet_kontaktinfo"] == "1"
    assert inserted.loc[0, "aar"] == 2023
    assert inserted.loc[0, "mnd"] == 6


def test_process_altinn_form_calls_all(processor_factory, minimal_parquet, monkeypatch):
    proc, fake = processor_factory()
    called = {"skjemamottak": False, "kontaktinfo": False, "skjemadata": False}

    def fake_skjemamottak():
        called["skjemamottak"] = True

    def fake_kontaktinfo():
        called["kontaktinfo"] = True

    def fake_skjemadata():
        called["skjemadata"] = True

    proc.process_skjemamottak = fake_skjemamottak
    proc.process_kontaktinfo = fake_kontaktinfo
    proc.process_skjemadata = fake_skjemadata
    proc.process_altinn_form(minimal_parquet)
    assert all(called.values())


@pytest.fixture()
def real_xml_path(tmp_path):
    # Copy the real XML and JSON meta files to a temp directory
    from shutil import copyfile

    data_dir = Path(__file__).parent / "data"
    xml_src = data_dir / "form_373a35bb8808.xml"
    json_src = data_dir / "meta_373a35bb8808.json"
    xml_dst = tmp_path / "form_373a35bb8808.xml"
    json_dst = tmp_path / "meta_373a35bb8808.json"
    copyfile(xml_src, xml_dst)
    copyfile(json_src, json_dst)
    return str(xml_dst), str(tmp_path) + "/"


def test_xml_to_parquet_creates_parquet(real_xml_path):
    xml_path, dest_folder = real_xml_path
    xml_to_parquet(xml_path, dest_folder)
    files = [f for f in os.listdir(dest_folder) if f.endswith(".parquet")]
    assert files, "No parquet file created"
    df = pd.read_parquet(os.path.join(dest_folder, files[0]))
    # Should contain expected columns
    assert "FELTNAVN" in df.columns
    assert "FELTVERDI" in df.columns
    # Should contain some SkjemaData_ and Kontakt_ rows
    assert df["FELTNAVN"].str.startswith("SkjemaData_").any()
    assert df["FELTNAVN"].str.startswith("Kontakt_").any()


def test_xml_to_parquet_excludes_contact_info(real_xml_path):
    xml_path, dest_folder = real_xml_path
    xml_to_parquet(xml_path, dest_folder, keep_contact_information=False)
    files = [f for f in os.listdir(dest_folder) if f.endswith(".parquet")]
    df = pd.read_parquet(os.path.join(dest_folder, files[0]))
    # Should NOT contain Kontakt_kontaktPerson rows
    assert not df["FELTNAVN"].str.startswith("Kontakt_kontaktPerson").any()


def test_xml_to_parquet_includes_contact_info(real_xml_path):
    xml_path, dest_folder = real_xml_path
    xml_to_parquet(xml_path, dest_folder, keep_contact_information=True)
    files = [f for f in os.listdir(dest_folder) if f.endswith(".parquet")]
    df = pd.read_parquet(os.path.join(dest_folder, files[0]))
    # Should contain Kontakt_kontaktPerson rows
    assert df["FELTNAVN"].str.startswith("Kontakt_kontaktPerson").any()


def test_xml_to_parquet_raises_on_wrong_dest(real_xml_path):
    xml_path, _ = real_xml_path
    with pytest.raises(ValueError):
        xml_to_parquet(xml_path, "/tmp")  # missing trailing slash


def test_xml_to_parquet_raises_on_wrong_path(real_xml_path):
    _, dest_folder = real_xml_path
    with pytest.raises(ValueError):
        xml_to_parquet("not_xml.txt", dest_folder)


def test_xml_to_parquet_raises_on_wrong_keep_contact_type(real_xml_path):
    xml_path, dest_folder = real_xml_path
    with pytest.raises(TypeError):
        xml_to_parquet(xml_path, dest_folder, keep_contact_information="yes")
