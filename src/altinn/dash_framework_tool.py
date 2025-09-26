"""Module for processing Altinn 3 data.

TODO: Make a method that takes the xml and json files and creates a parquet file containing everything necessary for insertion, in case the user doesn't want to transfer xml and json to the prod bucket.

If a more diverse set of alternative data storage technologies become available, might be an idea to make AltinnFormProcessor into an abstract base class and make some more tailored variants.
"""

import glob
import json
import logging
from logging.handlers import RotatingFileHandler


import eimerdb as db
import pandas as pd
from dapla_suv_tools.suv_client import SuvClient
from .flatten import isee_transform

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)  # Set to DEBUG for more verbose output

# Create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# File handler (rotating)
file_handler = RotatingFileHandler(
    'app.log', maxBytes=1024*1024, backupCount=5, encoding='utf8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def convert_to_int(value):
    try:
        num = float(str(value).replace(',', '.'))
        return int(num) if num.is_integer() else int(num)
    except ValueError:
        return value

class AltinnFormProcessor:
    """Tool for transferring Altinn3 data to an editing ready eimerdb instance.

    Has methods for processing a single form, all forms in a folder and a method for inserting data into an eimerdb table without creating duplicates.

    Notes:
        Notice that you can use inheritance to reuse parts of this class while adapting it to suit your specific needs. An example of this would be if you don't use eimerdb, you can overwrite the 'insert_into_database()' method to save the data in a way that suits your needs, while reusing the rest of the code.
        If you want to process the skjemadata part of the xml using this class you can write your own implementation as a method called 'process_skjemadata()' and it will be run during the 'process_altinn_form()'
    """

    def __init__(
        self,
        database_name: str,
        storage_location: str,
        path_to_form_folder: str,
        ra_number: str,  # Should maybe also support list?
        delreg_nr: str,
        xml_period_mapping: dict[str, str],
        suv_period_mapping: dict[str, str] | None = None,
        xml_ident_field: str = "reporteeOrgNr",
        suv_ident_field: str | None = None,
        isee_transform_mapping=None,
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
        self.ra_number = ra_number
        self.delreg_nr = delreg_nr
        self.xml_ident_field = xml_ident_field
        self.suv_ident_field = suv_ident_field
        self.database_name = database_name
        self.storage_location = storage_location
        self.xml_period_mapping = xml_period_mapping
        self.suv_period_mapping = suv_period_mapping
        self.period_xml_fields = [x for x in xml_period_mapping.values()]
        self.periods = [x for x in xml_period_mapping.keys()]
        self.form_folder = path_to_form_folder
        self.isee_transform_mapping = isee_transform_mapping

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

    def connect_to_database(self) -> None:
        """Method for establishing a connection to an eimerdb instance.

        Can be overwritten if another database type is used.
        """
        self.conn = db.EimerDBInstance(self.storage_location, self.database_name)

    def insert_into_database(
        self, data: pd.DataFrame, keys: list[str], table_name: str
    ) -> None:
        """Inserts dataframe contents into eimerdb instance.

        Checks for duplicates on keys before inserting into table.

        Args:
            data: A dataframe containing the columns specified in the eimerdb table schema with rows to insert.
            keys: Columns to use when checking existing data for duplicates before inserting new data.
            table_name: The table to insert data into.

        Note:
            This method can be used to insert into other tables in the eimerdb database.

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

    def extract_json(self) -> pd.DataFrame:
        """Gets the reference number (refnr) and timestamp for submission from the json file."""
        if self.json_path is None:
            raise ValueError("'self.json_path' cannot be None.")
        with open(
            self.json_path,
            encoding="utf-8",
        ) as fil:
            data = json.load(fil)

        # Løsning 1: Hvis du kun har én post og vil ha en DataFrame med én rad:
        json_content = pd.DataFrame([data])
        json_content = json_content.rename(
            columns={
                "altinnTidspunktLevert": "dato_mottatt",
                "altinnReferanse": "refnr",
            }
        )
        return json_content

    def process_all_forms(self) -> None:
        """Processes all forms found in the bucket path."""
        print("Starting")
        if self.suv_ident_field and self.suv_period_mapping:
            print("Using suv")
            self.process_enheter_suv()
        else:
            print("Using default")
            self.process_enheter()
        for form in glob.glob(f"{self.form_folder}/**/form_*.xml", recursive=True):
            logger.info(f"Processing: {form}")
            self.process_altinn_form(f"{form}")

    def process_altinn_form(self, form: str) -> None:
        """Processes a specific form.

        Args:
            form: Path to the xml file for the form.
        """
        self.xml_path = None  # Sikre at det "nullstilles", sikkert unødvendig
        self.json_path = None
        self.xml_path = form
        self.json_path = form.replace(".xml", ".json").replace("form_", "meta_")
        self.process_skjemamottak()
        self.process_kontaktinfo()
        self.process_skjemadata()

    def process_skjemadata(self) -> None:
        xml_content = pd.read_xml(self.xml_path)
        data = isee_transform(self.xml_path, mapping=self.isee_transform_mapping)
        xml_content = pd.DataFrame(
            [
                xml_content.apply(  # Collapses the dataframe into a single row consisting of the first non-NaN value in each column.
                    lambda col: (
                        col.dropna().iloc[0] if not col.dropna().empty else None
                    ),
                    axis=0,
                )
            ]
        )
        xml_content[self.xml_ident_field] = (
            xml_content[self.xml_ident_field].astype(int).astype(str)
        )
        for period_field in self.period_xml_fields:
            xml_content[period_field] = xml_content[period_field].astype(int)
        data = pd.concat([xml_content, data], axis=1)
        column_renaming = {
            "SKJEMA_ID": "skjema",
            self.xml_ident_field: "ident",
            "VERSION_NR": "refnr",
            "FELTNAVN": "variabel",
            "FELTVERDI": "verdi",
        } | {v: k for k, v in self.xml_period_mapping.items()}
        data = data.rename(columns=column_renaming)[
            [x for x in column_renaming.values()]
        ]
        data[["ident", *self.xml_period_mapping.keys()]] = data[
            ["ident", *self.xml_period_mapping.keys()]
        ].ffill()
        data = data.loc[
            ~data["variabel"].isin(
                ["ALTINNREFERANSE", "ALTINNTIDSPUNKTLEVERT", "ANGIVER_ID"]
            )
        ]
        data[self.periods] = data[self.periods].astype(int)

        data["verdi"] = data["verdi"].apply(convert_to_int)

        self.insert_into_database(
            data, [*self.periods, "skjema", "refnr", "variabel"], "skjemadata_hoved"
        )

    def process_enheter_suv(self) -> None:
        """This method will create a table containing information about the survey sample and which form each participant should answer.

        Uses dapla-suv-tools to get information about the sample and which form(s) they are sent and inserts information into the eimerdb instance.
        """
        # TODO: Make sure it works with surveys that have more than one form.
        client = SuvClient()
        form_id = {
            x["id"]
            for x in client.get_skjema_by_ra_nummer(
                ra_nummer="RA-0571", max_results=0, versjon=1, latest_only=False
            )
        }
        if len(form_id) != 1:
            raise ValueError(f"Should only have one 'id', found: {form_id}")
        form_id = next(iter(form_id))
        results = client.get_perioder_by_skjema_id(skjema_id=form_id)
        suv_periods = [
            results[0][period] for period in self.suv_period_mapping.values()
        ]
        results = client.get_utvalg_from_sfu(delreg_nr="97018", ra_nummer="RA-0571")
        for record in results:
            data = pd.DataFrame(
                [[*suv_periods, record[self.suv_ident_field], record["skjema_type"]]],
                columns=[*self.suv_period_mapping.keys(), "ident", "skjema"],
            )
            self.insert_into_database(data, [*self.periods, "ident"], "enheter")

    def process_enheter(self):
        for form in glob.glob(f"{self.form_folder}/**/form_*.xml", recursive=True):
            logger.info(f"Processing form: {form}")
            xml_content = pd.read_xml(form)
            xml_content = pd.DataFrame(
                [
                    xml_content.apply(  # Collapses the dataframe into a single row consisting of the first non-NaN value in each column.
                        lambda col: (
                            col.dropna().iloc[0] if not col.dropna().empty else None
                        ),
                        axis=0,
                    )
                ]
            )
            xml_content[self.xml_ident_field] = (
                xml_content[self.xml_ident_field].astype(float).astype(int).astype(str)
            )
            xml_content = xml_content[
                [*self.period_xml_fields, self.xml_ident_field, "raNummer"]
            ].rename(
                columns={"raNummer": "skjema", self.xml_ident_field: "ident"}
                | {v: k for k, v in self.xml_period_mapping.items()}
            )
            self.insert_into_database(xml_content, keys=[*self.periods, "ident", "skjema"], table_name="enheter")

    def process_skjemamottak(self) -> None:
        """Creates the table 'skjemamottak' based on altinn forms.

        This table is used for keeping track of which forms has been sent from which participant. In some cases the same form is sent multiple times from the same participant, which leads to duplicated information that needs to be handled. The table created by this method provides the structure to keep track of each forms content and has variables to show which form is 'active'.
        """
        data = self.extract_skjemamottak()
        self.insert_skjemamottak(data)

    def extract_skjemamottak_xml(self) -> pd.DataFrame:
        """Extracts the necessary information for creating the 'skjemamottak' table from the xml file."""
        if self.xml_path is None:
            raise ValueError("'self.json_path' cannot be None.")
        xml_content = pd.read_xml(self.xml_path)
        xml_content = xml_content[
            ["raNummer", self.xml_ident_field, *self.period_xml_fields]
        ]
        xml_content = xml_content.dropna().reset_index(drop=True)
        for period_field in self.period_xml_fields:
            xml_content[period_field] = xml_content[period_field].astype(int)
        xml_content[self.xml_ident_field] = (
            xml_content[self.xml_ident_field].astype(int).astype(str)
        )
        xml_content = xml_content.rename(
            columns={
                self.xml_ident_field: "ident",
                "raNummer": "skjema",
            }
            | {v: k for k, v in self.xml_period_mapping.items()}
        )
        return xml_content

    def extract_skjemamottak(self) -> pd.DataFrame:
        """Uses data from the xml file combined with the json file to create a one-row dataframe to insert into 'skjemamottak'."""
        data = pd.concat(
            [self.extract_skjemamottak_xml(), self.extract_json()], axis=1
        ).reset_index(drop=True)

        data["kommentar"] = ""
        data["aktiv"] = True
        data["editert"] = False

        data["ident"] = data["ident"].astype(str)
        data[self.periods] = data[self.periods].astype(int)
        data["dato_mottatt"] = pd.to_datetime(data["dato_mottatt"])
        data["dato_mottatt"] = data["dato_mottatt"].dt.floor("s")
        return data[
            [
                *self.periods,
                "skjema",
                "ident",
                "refnr",
                "dato_mottatt",
                "editert",
                "kommentar",
                "aktiv",
            ]
        ]

    def insert_skjemamottak(self, data: pd.DataFrame) -> None:
        """Inserts the new row into 'skjemamottak' table."""
        self.insert_into_database(
            data, [*self.periods, "skjema", "refnr"], "skjemamottak"
        )

    def process_kontaktinfo(self) -> None:
        """Creates the table 'kontaktinfo' based on altinn forms."""
        data = self.extract_kontaktinfo()
        self.insert_kontaktinfo(data)

    def extract_kontaktinfo(self) -> pd.DataFrame:
        """Extracts the necessary information for creating the 'kontaktinfo' table from the xml and json files."""
        if self.xml_path is None:
            raise ValueError("'self.json_path' cannot be None.")
        data = pd.read_xml(self.xml_path)
        necessary_columns = [
            "kontaktPersonNavn",
            "kontaktPersonEpost",
            "kontaktPersonTelefon",
            "kontaktInfoBekreftet",
            "kontaktInfoKommentar",
            "forklarKrevendeForh",
        ]
        for column in necessary_columns:
            if column not in data.columns:
                data[column] = ""
        data = data[
            [
                *self.period_xml_fields,
                self.xml_ident_field,
                "raNummer",
                "kontaktPersonNavn",
                "kontaktPersonEpost",
                "kontaktPersonTelefon",
                "kontaktInfoBekreftet",
                "kontaktInfoKommentar",
                "forklarKrevendeForh",
            ]
        ].rename(
            columns={
                self.xml_ident_field: "ident",
                "raNummer": "skjema",
                "kontaktPersonNavn": "kontaktperson",
                "kontaktPersonEpost": "epost",
                "kontaktPersonTelefon": "telefon",
                "kontaktInfoBekreftet": "bekreftet_kontaktinfo",
                "kontaktInfoKommentar": "kommentar_kontaktinfo",
                "forklarKrevendeForh": "kommentar_krevende",
            }
            | {v: k for k, v in self.xml_period_mapping.items()}
        )
        data = pd.concat(
            [data, self.extract_json().drop(columns="dato_mottatt")], axis=1
        ).reset_index(drop=True)
        data = pd.DataFrame(
            [
                data.apply(  # Collapses the dataframe into a single row consisting of the first non-NaN value in each column.
                    lambda col: (
                        col.dropna().iloc[0] if not col.dropna().empty else None
                    ),
                    axis=0,
                )
            ]
        )
        for column in ["ident", "telefon", "bekreftet_kontaktinfo"]:
            try:
                data[column] = data[column].astype(int)
            except ValueError as e:
                logger.debug(
                    f"Something went wrong with setting {column} .astype(int)\n",
                    exc_info=e,
                )
            except TypeError as e:
                logger.debug(
                    f"Something went wrong with setting {column} .astype(int)\n",
                    exc_info=e,
                )
            data[column] = data[column].astype(str)
        data[self.periods] = data[self.periods].astype(int)
        return data

    def insert_kontaktinfo(self, data: pd.DataFrame) -> None:
        """Inserts the new row into 'kontaktinfo' table."""
        self.insert_into_database(
            data, [*self.periods, "skjema", "refnr"], "kontaktinfo"
        )
