import logging

from dapla_suv_tools.suv_client import SuvClient
import eimerdb as db
import pandas as pd

logger = logging.getLogger(__name__)


class AltinnEimerdbProcessor:
    def __init__(
        self,
        database_name: str,
        storage_location: str,
        xml_period_mapping: dict[str, str],
        suv_period_mapping: dict[str, str],
        path_to_form_folder: str,
        xml_ident_field: str = "reporteeOrgNr",
        suv_ident_field: str = "orgnr",
        process_all_forms: bool = True,
    ) -> None:
        """Tool for"""
        self.xml_ident_field = xml_ident_field
        self.suv_ident_field = suv_ident_field
        self.database_name = database_name
        self.storage_location = storage_location
        self.xml_period_mapping = xml_period_mapping
        self.suv_period_mapping = suv_period_mapping
        self.period_xml_fields = [x for x in xml_period_mapping.values()]
        self.periods = [x for x in xml_period_mapping.keys()]
        self.form_folder = path_to_form_folder

        self.conn = db.EimerDBInstance(self.storage_location, self.database_name)
        self._is_valid()
        if process_all_forms:
            self.process_all_forms()

    def _is_valid(self) -> None:
        """Should check:
        - That all self.periods are in the database table schemas.
        - That the tables 'kontaktinfo', 'skjemamottak' and 'enheter' exists.
        - That the schemas are correct.
        - That the path to fields is a 'buckets/' path
        - storage_location doesn't start with 'gs://'
        """
        pass

    def insert_into_eimerdb(self, data: pd.DataFrame, keys: list[str], table_name: str):
        """Checks for duplicates on selected keys"""
        existing = self.conn.query(f"SELECT * FROM {table_name}")
        data = data.merge(existing[keys], on=keys, how="left", indicator=True)
        new_data = data[data["_merge"] == "left_only"]
        if not new_data.empty:
            self.conn.insert(table_name, new_data)
            logger.info(f"Inserted new row into '{table_name}'.")
        else:

            logger.info(f"Already exists in '{table_name}', skipping record.")

    def extract_json(self):
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

    def process_all_forms(self):
        self.table_enheter()
        for form in glob.glob(f"{self.form_folder}/**/form_*.xml", recursive=True):
            logger.info(f"Processing: {form}")
            self.process_altinn_form(f"{form}")

    def process_altinn_form(self, form):
        """"""
        self.xml_path = None  # Sikre at det "nullstilles", sikkert unødvendig
        self.json_path = None
        self.xml_path = form
        self.json_path = form.replace(".xml", ".json").replace("form_", "meta_")
        self.table_skjemamottak()
        self.table_kontaktinfo()

    def table_enheter(self):
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
                columns=[*self.suv_period_mapping.keys(), "ident", "skjemaer"],
            )
            self.insert_into_eimerdb(data, [*self.periods, "ident"], "enheter")

    def table_skjemamottak(self):
        data = self.extract_skjemamottak()
        self.insert_skjemamottak(data)

    def extract_skjemamottak_xml(self):
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

    def extract_skjemamottak(self):
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
        self.insert_into_eimerdb(
            data, [*self.periods, "skjema", "refnr"], "skjemamottak"
        )

    def table_kontaktinfo(self) -> None:
        data = self.extract_kontaktinfo()
        self.insert_kontaktinfo(data)

    def extract_kontaktinfo(self) -> pd.DataFrame:
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
        data = pd.concat([data, self.extract_json()], axis=1).reset_index(drop=True)
        data = data.apply(
            lambda col: col.dropna().iloc[0] if not col.dropna().empty else None, axis=0
        )
        data = pd.DataFrame([data])
        for column in ["ident", "telefon", "bekreftet_kontaktinfo"]:
            try:
                data[column] = data[column].astype(int)
            except ValueError as e:
                logger.debug(
                    f"Something went wrong with setting {column} .astype(int)\n",
                    exc_info=e,
                )
            data[column] = data[column].astype(str)
        data[self.periods] = data[self.periods].astype(int)
        return data

    def insert_kontaktinfo(self, data: pd.DataFrame) -> None:
        self.insert_into_eimerdb(
            data, [*self.periods, "skjema", "refnr"], "kontaktinfo"
        )
