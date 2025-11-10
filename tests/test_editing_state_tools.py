import pandas as pd

from altinn import AltinnFormProcessor
from altinn import xml_to_parquet


def test_processor_inheritance():
    class TestFormProcessor(AltinnFormProcessor):
        def __init__(
            self,
            ra_number: str,
            path_to_form_folder: str,
            parquet_ident_field: str,
            parquet_period_mapping: dict[str, str],
            delreg_nr: str | None = None,
            suv_period_mapping: dict[str, str] | None = None,
            suv_ident_field: str | None = None,
            database_name: str | None = None,
            storage_location: str | None = None,
            process_all_forms: bool = False,
        ) -> None:
            super().__init__(
                ra_number,
                path_to_form_folder,
                parquet_ident_field,
                parquet_period_mapping,
                delreg_nr,
                suv_period_mapping,
                suv_ident_field,
                database_name,
                storage_location,
                process_all_forms,
            )

        def insert_or_save_data(
            self, data: pd.DataFrame, keys: list[str], table_name: str
        ) -> None:
            expected = pd.read_feather(f"tests/data/{table_name}.feather")
            assert (
                expected.shape == data.shape
            ), "Shape of expected different from actual."
            for col in expected:
                assert data[col][0] == expected[col][0], f"Something wrong with {col}"

        def connect_to_database(self) -> None:
            pass

    TestFormProcessor(
        ra_number="RA-0689",
        path_to_form_folder="tests/data",
        parquet_ident_field="InternInfo_reporteeOrgNr",
        parquet_period_mapping={
            "aar": "InternInfo_periodeAAr",
            "mnd": "InternInfo_periodeNummer",
        },
    ).process_all_forms()


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


# def test_processor():
#     processor = AltinnFormProcessor(
#         database_name="",
#         storage_location="",
#         path_to_form_folder="",
#         ra_number="",
#         delreg_nr="",
#         parquet_ident_field="",
#         parquet_period_mapping={},
#     )
