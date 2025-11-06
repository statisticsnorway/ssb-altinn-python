import pandas as pd

from altinn import AltinnFormProcessor
from altinn import xml_to_parquet


def test_parquet_file_data(mocker):
    """Run xml_to_parquet and capture the DataFrame passed to to_parquet()."""
    xml_path = "tests/data/form_373a35bb8808.xml"
    destination_folder = "/"

    captured = {}

    # Define a side effect to capture the DataFrame (`self`)
    def capture_to_parquet(self, path, *args, **kwargs):
        captured["df"] = self.copy()
        captured["path"] = path

    mocker.patch.object(pd.DataFrame, "to_parquet", capture_to_parquet)

    xml_to_parquet(
        path=xml_path,
        destination_folder=destination_folder,
        keep_contact_information=True,
    )

    # --- Assertions ---
    assert "df" in captured, "DataFrame was not captured"
    assert "path" in captured, "Path was not captured"

    df = captured["df"]
    assert isinstance(df, pd.DataFrame)


def test_processor():
    processor = AltinnFormProcessor(
        database_name="",
        storage_location="",
        path_to_form_folder="",
        ra_number="",
        delreg_nr="",
        parquet_ident_field="",
        parquet_period_mapping={},
    )
