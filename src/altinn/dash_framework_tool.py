"""Module for processing Altinn 3 data.

TODO: Make a method that takes the xml and json files and creates a parquet file containing everything necessary for insertion, in case the user doesn't want to transfer xml and json to the prod bucket.

If a more diverse set of alternative data storage technologies become available, might be an idea to make AltinnFormProcessor into an abstract base class and make some more tailored variants.
"""

import glob
import logging
from logging.handlers import RotatingFileHandler

import eimerdb as db
import pandas as pd
from dapla_suv_tools.suv_client import SuvClient

from .flatten import xml_transform, create_isee_filename, _read_json_meta


logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)  # Set to DEBUG for more verbose output

# Create formatter
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# File handler (rotating)
file_handler = RotatingFileHandler(
    "app.log", maxBytes=1024 * 1024, backupCount=5, encoding="utf8"
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def xml_to_parquet(
    path: str, destination_folder: str, keep_contact_information: bool = False
):
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
    data.to_parquet(
        f"{destination_folder}{create_isee_filename(path).replace('.csv', '.parquet')}"
    )


class AltinnFormProcessor:
    """Tool for transferring Altinn3 data to an editing ready eimerdb instance.

    Has methods for processing a single form, all forms in a folder and a method for inserting data into an eimerdb table without creating duplicates.

    Notes:
        Notice that you can use inheritance to reuse parts of this class while adapting it to suit your specific needs. An example of this would be if you don't use eimerdb, you can overwrite the 'insert_or_save_data()' method to save the data in a way that suits your needs, while reusing the rest of the code.
        If you want to process the skjemadata part of the xml using this class you can write your own implementation as a method called 'process_skjemadata()' and it will be run during the 'process_altinn_form()'
    """

    def __init__(
        self,
        database_name: str,
        storage_location: str,
        path_to_form_folder: str,
        ra_number: str,  # Should maybe also support list?
        delreg_nr: str,
        parquet_period_mapping: dict[str, str],
        suv_period_mapping: dict[str, str] | None = None,
        parquet_ident_field: str = "reporteeOrgNr",
        suv_ident_field: str | None = None,
        isee_transform_mapping=None,
        include_contact_information: bool = False,
        process_all_forms: bool = False,
    ) -> None:
        """Instantiate the processor and connect it to the eimerdb instance.

        Args:
            database_name: Name of the eimerdb database to insert into.
            storage_location: Location for the eimerdb to insert into.
            ra_number: The form number 'RA-xxxx'.
            delreg_nr: The SFU delregisternummer for the collection, used to populate the 'enheter' table.
            xml_period_mapping: Mapping between the names you want for your period variables and the name of the fields in the xml files.
            suv_period_mapping: Mapping between the names you want for your period variables and the name of the fields in the suv data.
            path_to_form_folder: Path to the folder where the altinn form data (xml, json and pdf) are stored. Must be '/buckets/' path.
            xml_ident_field: The name of the field in altinn form xml to use as the value for 'ident'.
            suv_ident_field: The name of the 'ident' value in the suv...
            process_all_forms: Boolean to decide if the insertion code should run for all forms during instantiation of the class.

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
        self.isee_transform_mapping = isee_transform_mapping
        self.include_contact_information = include_contact_information

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

    def get_datatype_template(self, table):
        """Method for generating a template to insert into the 'datatyper' table."""
        ...

    def get_refnr(self) -> pd.DataFrame:
        """Gets the reference number (refnr)."""
        file = self.data
        refnr = file.loc[file["FELTNAVN"] == "altinnReferanse"]["FELTVERDI"].item()
        return refnr

    def get_date_received(self):
        """Gets the date_received for the Altinn form."""
        file = self.data
        date_received = file.loc[file["FELTNAVN"] == "altinnTidspunktLevert"][
            "FELTVERDI"
        ].item()
        return date_received

    def get_form_number(self):
        """Gets the RA-number for the form."""
        file = self.data
        ra_number = file.loc[file["FELTNAVN"] == "InternInfo_raNummer"][
            "FELTVERDI"
        ].item()
        return ra_number

    def get_ident(self):
        file = self.data
        ident = file.loc[file["FELTNAVN"] == self.parquet_ident_field][
            "FELTVERDI"
        ].item()
        return ident

    def get_periods(self):
        """Gets the period value(s) from the form."""
        file = self.data
        period_dict = {}
        for period, period_name in self.period_parquet_fields.items():
            period_dict[period] = file.loc[file["FELTNAVN"] == period_name][
                "FELTVERDI"
            ].item()
        return period_dict

    def process_skjemamottak(self):
        skjemamottak_record = self.get_periods() | {
            "skjema": self.get_form_number(),
            "ident": self.get_ident(),
            "refnr": self.get_refnr(),
            "dato_mottatt": self.get_date_received(),
            "editert": False,
            "kommentar": "",
            "aktiv": True,
        }
        skjemamottak_record = pd.DataFrame([skjemamottak_record])
        self.insert_or_save_data(
            data=skjemamottak_record,
            keys=[*self.periods, "skjema", "refnr"],
            table_name="skjemamottak",
        )

    def process_kontaktinfo(self):
        kontaktinfo_record = self.get_periods() | {
            # periods
            "skjema": self.get_form_number(),
            "ident": self.get_ident(),
            "refnr": self.get_refnr(),
        }
        kontaktinfo_record = pd.DataFrame([kontaktinfo_record])
        self.insert_or_save_data(
            data=kontaktinfo_record,
            keys=[*self.periods, "skjema", "refnr"],
            table_name="kontaktinfo",
        )

    def process_enheter(self):
        enheter_record = self.get_periods() | {
            # periods
            "ident": self.get_ident(),
            "skjema": self.get_form_number(),
        }
        enheter_record = pd.DataFrame([enheter_record])
        self.insert_or_save_data(
            data=enheter_record,
            keys=[*self.periods, "ident", "skjema"],
            table_name="enheter",
        )

    def process_skjemadata(self):
        logger.warning("No method defined for processing skjemadata.")
        pass

    def process_altinn_form(self, form: str) -> None:
        """Processes a specific form.

        Args:
            form: Path to the xml file for the form.
        """
        logger.debug(f"Processing: {form}")
        self.data = None  # Sikre at det "nullstilles", sikkert unødvendig
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
        for form in glob.glob(f"{self.form_folder}/**/form_*.xml", recursive=True):
            self.process_altinn_form(f"{form}")

    def process_enheter_suv(self) -> None:
        """This method will create a table containing information about the survey sample and which form each participant should answer.

        Uses dapla-suv-tools to get information about the sample and which form(s) they are sent and inserts information into the eimerdb instance.
        """
        # TODO: Make sure it works with surveys that have more than one form.
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
