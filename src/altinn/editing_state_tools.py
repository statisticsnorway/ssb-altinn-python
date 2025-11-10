"""Module for processing Altinn 3 data.

If a more diverse set of alternative data storage technologies become available, might be an idea to make AltinnFormProcessor into an abstract base class and make some more tailored variants.
"""

import glob
import logging
from typing import Any

import eimerdb as db
import pandas as pd
from dapla_suv_tools.suv_client import SuvClient

from .flatten import _read_json_meta
from .flatten import create_isee_filename
from .flatten import xml_transform

logger = logging.getLogger(__name__)


def xml_to_parquet(
    path: str, destination_folder: str, keep_contact_information: bool = False
) -> None:
    """Function for taking the received xml and json files and turning them into parquet files.

    Primarily intended as the preparation before running AltinnFormProcessor.

    Args:
        path (str): path to the xml file to transform. Requires that the json file is in the same folder.
        destination_folder (str): The folder to write the parquet file to.
        keep_contact_information (bool): Whether or not to keep contact information from the forms. Defaults to False.

    Raises:
        ValueError: If 'destination_folder' doesn't end with '/' or if 'path' doesn't end with '.xml'.
        TypeError: If keep_contact_information is not type bool or if auto-generated filename from 'create_isee_filename(path)' is not string.

    Examples:
        def main(source_file):
            xml_to_parquet(
                path = source_file,
                destination_folder = "inndata/surveyname/forms/"
            )
    """
    if not destination_folder.endswith("/"):
        raise ValueError(
            f"'destination_folder' path must end with '/'. Received: {destination_folder}"
        )
    if not path.endswith(".xml"):
        raise ValueError(f"'path' must be path to xml file. Received: {path}")
    if not isinstance(keep_contact_information, bool):
        raise TypeError(
            f"'keep_contact_information' must be type bool. Received type: {type(keep_contact_information)}"
        )
    data = xml_transform(path)
    data = data.loc[
        (data["FELTNAVN"].str.startswith("SkjemaData_"))
        | (data["FELTNAVN"].str.startswith("Kontakt_"))
        | (data["FELTNAVN"].str.startswith("Brukeropplevelse_"))
        | (data["FELTNAVN"].str.startswith("Tidsbruk_"))
        | (data["FELTNAVN"].str.startswith("InternInfo_"))
    ]
    if not keep_contact_information:
        data = data.loc[~data["FELTNAVN"].str.startswith("Kontakt_kontaktPerson")]
    data = data.reset_index(drop=True)
    json_content = _read_json_meta(path)
    json_content = (
        pd.DataFrame([json_content])
        .T.reset_index()
        .rename(columns={"index": "FELTNAVN", 0: "FELTVERDI"})
    )
    data = pd.concat([data, json_content])
    isee_name = create_isee_filename(path)
    if not isinstance(isee_name, str):
        raise TypeError(
            f"Invalid file name. Expected string, received: {type(isee_name)}"
        )
    logger.info(f"Writing file as: {isee_name}")
    data.to_parquet(f"{destination_folder}{isee_name.replace('.csv', '.parquet')}")


class AltinnFormProcessor:
    """Tool for transferring Altinn3 data to an editing ready eimerdb instance.

    Has methods for processing a single form, all forms in a folder and a method for inserting data into an eimerdb table without creating duplicates.

    Notes:
        Notice that you can use inheritance to reuse parts of this class while adapting it to suit your specific needs. An example of this would be if you don't use eimerdb, you can overwrite the 'insert_or_save_data()' method to save the data in a way that suits your needs, while reusing the rest of the code.
        If you want to process the skjemadata part of the xml using this class you can write your own implementation as a method called 'process_skjemadata()' and it will be run during the 'process_altinn_form()'
    """

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
        """Instantiate the processor and connect it to the data.

        Args:
            database_name: name of the eimerdb to insert into. Can be empty string if not using eimerdb.
            storage_location: Path to the eimerdb bucket. Can be empty string if not using eimerdb.
            path_to_form_folder: Path to folder containing forms as parquet files.
            ra_number: The RA-number for the Altinn form.
            delreg_nr: The delregisternummer for the Altinn form. Required for using Suv Tools method of populating the 'enheter' table
            parquet_period_mapping: A mapping dict with the key being the period name you want in your data and the value being the period variable name in the parquet file.
            suv_period_mapping: A mapping dict with the key being the period name you want in your data and the value being the period name in the data from Dapla Suv Tools.
            parquet_ident_field: The name of the ident field in the parquet file. Example: 'InternInfo_reporteeOrgNr'
            suv_ident_field: The name of the ident variable in the data from Dapla Suv Tools.
            process_all_forms: If True, immediately starts processing all forms contained in the supplied form_folder.

        Notes:
            If you are not using eimerdb you do not need to supply database_name or storage_location as these are only used to connect to the eimerdb instance.
        """
        self.parquet_ident_field = parquet_ident_field
        self.ra_number = ra_number
        self.delreg_nr = delreg_nr
        self.suv_ident_field = suv_ident_field
        self.database_name = database_name
        self.storage_location = storage_location
        self.parquet_period_mapping = parquet_period_mapping
        self.suv_period_mapping = suv_period_mapping
        self.period_parquet_fields = [x for x in parquet_period_mapping.values()]
        self.periods = [x for x in parquet_period_mapping.keys()]
        self.form_folder = path_to_form_folder

        self.data: pd.DataFrame | None = None
        self.connect_to_database()
        self._is_valid()
        if process_all_forms:
            self.process_all_forms()

    def _is_valid(self) -> None:
        """Validates that the provided arguments are correct.

        Should check:
        - That all self.periods are in the database table schemas.
        - That the tables 'kontaktinfo', 'skjemamottak' and 'enheter' exists.
        - That the schemas are correct.
        - That the path to fields is a 'buckets/' path
        - storage_location doesn't start with 'gs://'
        """
        pass

    def get_value_with_default(
        self, file: pd.DataFrame, field_name: str, default_value: Any = ""
    ) -> Any:
        """Method for retrieving a value from the dataframe that accounts for the value not being present."""
        try:
            return file.loc[file["FELTNAVN"] == field_name, "FELTVERDI"].item()
        except (ValueError, AttributeError):
            return default_value

    def get_refnr(self) -> str:
        """Gets the reference number (refnr)."""
        file = self.data
        if not isinstance(file, pd.DataFrame):
            raise TypeError(
                f"Expected self.data to be pd.DataFrame. Received: {type(file)}"
            )
        refnr = file.loc[file["FELTNAVN"] == "altinnReferanse"]["FELTVERDI"].item()
        return str(refnr)

    def get_date_received(self) -> Any:  # Check returned datatype.
        """Gets the date_received for the Altinn form."""
        file = self.data
        if not isinstance(file, pd.DataFrame):
            raise TypeError(
                f"Expected self.data to be pd.DataFrame. Received: {type(file)}"
            )
        date_received = file.loc[
            file["FELTNAVN"] == "altinnTidspunktLevert", "FELTVERDI"
        ].iloc[0]
        return pd.to_datetime(date_received).floor("s")

    def get_form_number(self) -> str:
        """Gets the RA-number for the form."""
        file = self.data
        if not isinstance(file, pd.DataFrame):
            raise TypeError(
                f"Expected self.data to be pd.DataFrame. Received: {type(file)}"
            )
        ra_number = file.loc[file["FELTNAVN"] == "InternInfo_raNummer"][
            "FELTVERDI"
        ].item()
        return str(ra_number)

    def get_ident(self) -> str:
        """Gets the ident for the form."""
        file = self.data
        if not isinstance(file, pd.DataFrame):
            raise TypeError(
                f"Expected self.data to be pd.DataFrame. Received: {type(file)}"
            )
        ident = file.loc[file["FELTNAVN"] == self.parquet_ident_field][
            "FELTVERDI"
        ].item()
        return str(ident)

    def get_periods(self) -> dict[str, int]:
        """Gets the period value(s) from the form.

        Period value is int because of limitations in eimerdb.
        """
        file = self.data
        if not isinstance(file, pd.DataFrame):
            raise TypeError(
                f"Expected self.data to be pd.DataFrame. Received: {type(file)}"
            )
        period_dict = {}
        for period, period_name in self.parquet_period_mapping.items():
            period_dict[period] = int(
                file.loc[file["FELTNAVN"] == period_name]["FELTVERDI"].item()
            )
        return period_dict

    def process_skjemamottak(self) -> None:
        """Processes the form and inserts it into the 'skjemamottak' table."""
        skjemamottak_record = self.get_periods() | {
            "skjema": self.get_form_number(),
            "ident": self.get_ident(),
            "refnr": self.get_refnr(),
            "dato_mottatt": self.get_date_received(),
            "editert": False,
            "kommentar": "",
            "aktiv": True,
        }
        skjemamottak_record_dataframe = pd.DataFrame([skjemamottak_record])
        self.insert_or_save_data(
            data=skjemamottak_record_dataframe,
            keys=[*self.periods, "skjema", "refnr"],
            table_name="skjemamottak",
        )

    def process_kontaktinfo(self) -> None:
        """Processes the form and inserts it into the 'kontaktinfo' table."""
        file = self.data
        if not isinstance(file, pd.DataFrame):
            raise TypeError(
                f"Expected self.data to be pd.DataFrame. Received: {type(file)}"
            )
        kontaktperson = self.get_value_with_default(
            file, "Kontakt_kontaktPersonNavn", ""
        )
        telefon = self.get_value_with_default(file, "Kontakt_kontaktPersonTelefon", "")
        epost = self.get_value_with_default(file, "Kontakt_kontaktPersonEpost", "")
        bekreftet_kontaktinfo = self.get_value_with_default(
            file, "Kontakt_kontaktInfoBekreftet", ""
        )
        kommentar_kontaktinfo = self.get_value_with_default(
            file, "Kontakt_kontaktInfoKommentar", ""
        )
        kommentar_krevende = self.get_value_with_default(
            file, "forklarKrevendeForh", ""
        )

        kontaktinfo_record = self.get_periods() | {
            "skjema": self.get_form_number(),
            "ident": self.get_ident(),
            "refnr": self.get_refnr(),
            "kontaktperson": kontaktperson,
            "telefon": telefon,
            "epost": epost,
            "bekreftet_kontaktinfo": bekreftet_kontaktinfo,
            "kommentar_kontaktinfo": kommentar_kontaktinfo,
            "kommentar_krevende": kommentar_krevende,
        }
        kontaktinfo_record_dataframe = pd.DataFrame([kontaktinfo_record])
        self.insert_or_save_data(
            data=kontaktinfo_record_dataframe,
            keys=[*self.periods, "skjema", "refnr"],
            table_name="kontaktinfo",
        )

    def process_enheter(self) -> None:
        """Processes the form and inserts it into the 'enheter' table."""
        logger.info("Processing enheter")
        for form in glob.glob(f"{self.form_folder}/**/*.parquet", recursive=True):
            if self.data is not None:
                delattr(self, "data")  # Sikre at det "nullstilles", sikkert unødvendig
            self.data = pd.read_parquet(form)
            enheter_record = self.get_periods() | {
                "ident": self.get_ident(),
                "skjema": self.get_form_number(),
            }
            enheter_record_dataframe = pd.DataFrame([enheter_record])
            self.insert_or_save_data(
                data=enheter_record_dataframe,
                keys=[*self.periods, "ident", "skjema"],
                table_name="enheter",
            )

    def process_skjemadata(self) -> None:
        """Processes the form to create skjemadata. This is a placeholder function to be replaced."""
        logger.warning("No method defined for processing skjemadata.")
        pass

    def process_altinn_form(self, form: str) -> None:
        """Processes a specific form.

        Args:
            form: Path to the xml file for the form.
        """
        logger.debug(f"Processing: {form}")
        if self.data is not None:
            delattr(self, "data")  # Sikre at det "nullstilles", sikkert unødvendig
        self.data = pd.read_parquet(form)
        self.process_skjemamottak()
        self.process_kontaktinfo()
        self.process_skjemadata()

    def process_all_forms(self) -> None:
        """Processes all forms found in the bucket path."""
        logger.info("Starting processing of all forms.")
        if self.suv_ident_field and self.suv_period_mapping:
            logger.info("Using suv for 'enheter'.")
            self.process_enheter_suv()
        else:
            logger.info("Using default 'process_enheter()' for 'enheter'.")
            self.process_enheter()
        for form in glob.glob(f"{self.form_folder}/**/*.parquet", recursive=True):
            self.process_altinn_form(f"{form}")

    def process_enheter_suv(self) -> None:
        """This method will create a table containing information about the survey sample and which form each participant should answer.

        Uses dapla-suv-tools to get information about the sample and which form(s) they are sent and inserts information into the eimerdb instance.
        """
        if self.ra_number is None:
            raise TypeError(
                f"Must be supplied with string value for ra-number. Received: {type(self.ra_number)}"
            )
        if self.delreg_nr is None:
            raise TypeError(
                f"Must be supplied with string value for delreg number. Received: {type(self.delreg_nr)}"
            )
        if not isinstance(self.suv_period_mapping, dict):
            raise TypeError(
                f"suv_period_mapping must be dict. Received: {type(self.suv_period_mapping)}"
            )
        client = SuvClient()
        form_id = {
            x["id"]
            for x in client.get_skjema_by_ra_nummer(
                ra_nummer=self.ra_number, max_results=0, versjon=1, latest_only=False
            )
        }
        if len(form_id) != 1:
            raise ValueError(f"Should only have one 'id', found: {form_id}")
        form_id = next(iter(form_id))
        results = client.get_perioder_by_skjema_id(skjema_id=form_id)
        suv_periods = [
            results[0][period] for period in self.suv_period_mapping.values()
        ]
        results = client.get_utvalg_from_sfu(
            delreg_nr=self.delreg_nr, ra_nummer=self.ra_number
        )
        for record in results:
            data = pd.DataFrame(
                [[*suv_periods, record[self.suv_ident_field], record["skjema_type"]]],
                columns=[*self.suv_period_mapping.keys(), "ident", "skjema"],
            )
            self.insert_or_save_data(data, [*self.periods, "ident"], "enheter")

    def connect_to_database(self) -> None:
        """Method for establishing a connection to an eimerdb instance.

        Can be overwritten if another database type is used.
        """
        if self.storage_location is None or self.database_name is None:
            raise ValueError(
                f"Using eimerdb requires that you define storae_location and database_name. Received storage_location: {self.storage_location} and database_name: {self.database_name}"
            )
        self.conn = db.EimerDBInstance(self.storage_location, self.database_name)

    def insert_or_save_data(
        self, data: pd.DataFrame, keys: list[str], table_name: str
    ) -> None:
        """Inserts dataframe contents into eimerdb instance.

        Checks for duplicates on keys before inserting into table.

        Args:
            data: A dataframe containing the columns specified in the eimerdb table schema with rows to insert.
            keys: Columns to use when checking existing data for duplicates before inserting new data.
            table_name: The table to insert data into.

        Note:
            This method can be used to insert into other tables in the eimerdb database or simply save the data as a file.

        Raises:
            ValueError: If 'existing' is not pd.DataFrame
        """
        existing = self.conn.query(f"SELECT * FROM {table_name}")
        if not isinstance(existing, pd.DataFrame):
            raise ValueError(
                f"Value of 'existing' not pd.DataFrame. Received: {type(existing)}"
            )
        data = data.merge(existing[keys], on=keys, how="left", indicator=True)
        new_data = data[data["_merge"] == "left_only"]
        if not new_data.empty:
            self.conn.insert(table_name, new_data)
            logger.info(f"Inserted new row into '{table_name}'.")
        else:
            logger.info(f"Already exists in '{table_name}', skipping record.")
