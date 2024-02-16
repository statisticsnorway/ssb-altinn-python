"""For flattening Altinn3 xml-files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. These generic
functions currently supports xml files that do not contain kodelister. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""

import re
import xml.etree.ElementTree as ET
from collections.abc import MutableMapping
from typing import Any
from typing import Optional

import pandas as pd
import xmltodict
from dapla import FileClient

from altinn import utils


def _extract_counter(value: str) -> list[str]:
    """Extracts counter values from a string.

    Args:
        value: The input string containing counter values.

    Returns:
        A list of counter values extracted from the input string.

    Example:
        >>> _extract_counter('£3$ £2$ £1$')
        ['3', '2', '1']
    """
    matches = re.findall(r"£(.*?)\$", value)
    return matches


def _flatten_dict(d: Any, parent_key: str = "", sep: str = "_") -> Any:
    """Flatten a nested dictionary with an optional separator for keys.

    Args:
        d: The input dictionary.
        parent_key: The prefix to be added to each flattened key. Defaults to ''.
        sep: The separator to be used between keys. Defaults to '_'.

    Returns:
        The flattened dictionary.
    """
    items: list[tuple[str, str]] = []
    counter = 0

    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k

        if isinstance(v, MutableMapping):
            counter += 1
            items.extend(
                _flatten_dict(v, "£" + str(counter) + "$" + new_key, sep=sep).items()
            )

        elif isinstance(v, list):
            for element in v:
                counter += 1
                if isinstance(element, MutableMapping):
                    items.extend(
                        _flatten_dict(
                            element, "£" + str(counter) + "$" + new_key, sep=sep
                        ).items()
                    )
            counter = 0

        else:
            items.append((new_key, v))

        counter = 0

    return dict(items)


def _validate_interninfo(file_path: str) -> bool:
    """Validate interninfo.

    Validates the presence of required keys
    ('enhetsIdent', 'enhetsType', 'delregNr')
    within the 'interninfo' dictionary of an XML file converted
    to a dictionary.

    Args:
        file_path: The file path to the XML file.

    Returns:
        True if all required keys exist in the 'interninfo'
        dictionary, False otherwise.
    """
    xml_dict = _read_single_xml_to_dict(file_path)
    root_element = next(iter(xml_dict.keys()))

    required_keys = ["enhetsIdent", "enhetsType", "delregNr"]

    missing_keys = [
        key for key in required_keys if key not in xml_dict[root_element]["InternInfo"]
    ]

    if missing_keys:
        print("The following required keys are missing in ['InternInfo']:")
        for key in missing_keys:
            print(key)
        print("No output will be produced")

        return False
    else:
        return True


def _read_single_xml_to_dict(file_path: str) -> dict[str, Any]:
    """Reads XML-file from GCS or local file, and transforms it to a dictionary.

    Args:
        file_path: The path to the XML file

    Returns:
        A dictionary with data from a XML
    """
    if utils.is_gcs(file_path):
        fs = FileClient.get_gcs_file_system()

        with fs.open(file_path, mode="r") as xml_file:
            data_dict = xmltodict.parse(xml_file.read())

    else:
        with open(file_path) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())

    return data_dict


def _extract_angiver_id(file_path: str) -> str | None:
    """Collects angiver_id from the filepath.

    Args:
        file_path: The path to the XML file

    Returns:
        String with extracted_text (angiver_id)
    """
    start_index = file_path.find("/form_") + len("/form_")
    end_index = file_path.find(".xml", start_index)
    if start_index != -1 and end_index != -1:
        extracted_text = file_path[start_index:end_index]
        return extracted_text
    else:
        return None


def _make_angiver_row_df(file_path: str) -> pd.DataFrame:
    """Makes a Dataframe with a single row containg info on ANGIVERID.

    A DataFrame that will be concatenated on the end of the ISSE-DataFrame

    Args:
        file_path: The path to the XML file

    Returns:
        A DataFrame with a single row containing infor on ANGIVER_ID in ISEE-format

    """
    xml_dict = _read_single_xml_to_dict(file_path)
    root_element = next(iter(xml_dict.keys()))
    angiver_id_row = {
        "FELTNAVN": "ANGIVER_ID",
        "FELTVERDI": _extract_angiver_id(file_path),
        "IDENT_NR": xml_dict[root_element]["InternInfo"]["enhetsIdent"],
        "VERSION_NR": _extract_angiver_id(file_path),
        "DELREGNR": xml_dict[root_element]["InternInfo"]["delregNr"],
        "ENHETS_TYPE": xml_dict[root_element]["InternInfo"]["enhetsType"],
        "SKJEMA_ID": xml_dict[root_element]["InternInfo"]["raNummer"],
    }

    return pd.DataFrame([angiver_id_row])


def _create_levels_col(row: Any) -> int:
    """Create a 'LEVELS' column based on the length of the 'COUNTER' list in a row.

    Args:
        row: A dictionary representing a row of a DataFrame.

    Returns:
        The level value assigned to the row based on the 'COUNTER' list length.
    """
    if isinstance(row["COUNTER"], list) and len(row["COUNTER"]) > 1:
        return 2
    elif isinstance(row["COUNTER"], list) and len(row["COUNTER"]) == 1:
        return 1
    else:
        return 0


def _add_lopenr(df: pd.DataFrame) -> pd.DataFrame:
    """Add a running number to the 'FELTNAVN' column.

    Args:
        df: The input DataFrame.

    Returns:
        DataFrame with added running numbers.
    """
    complex_values = set(df.loc[df["LEVELS"] > 1, "FELTNAVN"].tolist())

    if complex_values:
        print("\033[91m" + "XML-inneholder kompliserte strukturer (Tabell i tabell).")
        print(
            "Det kan være nødvendig med ytterligere behandling av datagrunnlaget før innlasting til ISEE."
        )
        print(
            "Disse FELTNAVN har ikke fått påkoblet løpenummer på gjentagende verdier: \033[0m"
        )
        for var in complex_values:
            print(var)

    for index, row in df.iterrows():
        if row["LEVELS"] > 0:
            last_counter_value = df.at[index, "COUNTER"][-1]
            df.at[index, "FELTNAVN"] += "_" + last_counter_value.zfill(3)

    df = df.drop(["COUNTER", "LEVELS"], axis=1)

    return df


def isee_transform(
    file_path: str, mapping: Optional[dict[str, str]] = None
) -> pd.DataFrame:
    """Transforms a XML to ISEE-format using xmltodict.

    Transforms the XML to ISEE-format by using xmltodict to transform the XML
    to a dictionary. Traverses/scans the key/values in dictionary for lists,
    dicts and simple values.
    Stores the results in a list of dictionaries, that converts to a DataFrame

    Args:
        file_path: The path to the XML file.
        mapping: The mapping dictionary to map variable names in the
            'feltnavn' column. The default value is an empty dictionary
            (if mapping is not needed).

    Returns:
        pandas.DataFrame: A transformed DataFrame which aligns with the
        ISEE dynarev format.

    Raises:
        ValueError: If invalid gcs-file or xml-file.
    """
    if utils.is_valid_xml(file_path):
        if _validate_interninfo(file_path):
            if mapping is None:
                mapping = {}

            xml_dict = _read_single_xml_to_dict(file_path)
            root_element = next(iter(xml_dict.keys()))
            input_dict = xml_dict[root_element]["SkjemaData"]

            final_dict = _flatten_dict(input_dict)

            final_df = pd.DataFrame(
                list(final_dict.items()), columns=["FELTNAVN", "FELTVERDI"]
            )

            final_df["IDENT_NR"] = xml_dict[root_element]["InternInfo"]["enhetsIdent"]
            final_df["VERSION_NR"] = _extract_angiver_id(file_path)
            final_df["DELREGNR"] = xml_dict[root_element]["InternInfo"]["delregNr"]
            final_df["ENHETS_TYPE"] = xml_dict[root_element]["InternInfo"]["enhetsType"]
            final_df["SKJEMA_ID"] = xml_dict[root_element]["InternInfo"]["raNummer"]

            final_df = final_df[~final_df["FELTNAVN"].str.contains("@xsi:nil")]

            final_df = pd.concat(
                [final_df, _make_angiver_row_df(file_path)], ignore_index=True
            )

            final_df["COUNTER"] = final_df["FELTNAVN"].apply(_extract_counter)

            final_df["FELTNAVN"] = final_df["FELTNAVN"].str.replace(
                r"£.*?\$", "", regex=True
            )

            final_df["LEVELS"] = final_df.apply(_create_levels_col, axis=1)

            if mapping is not None:
                final_df["FELTNAVN"] = final_df["FELTNAVN"].replace(mapping)

            final_df = _add_lopenr(final_df)

            return final_df

    else:
        error_message = f"File is not a valid XML-file: {file_path}"
        raise ValueError(error_message)

    return pd.DataFrame()  # Should never reach this point, but need a return value


def xml_transform(file_path: str) -> pd.DataFrame:
    """Transforms a XML to a pd.Dataframe using xmltodict.

    Transforms the XML to a dataframe, using xmltodict to transform the XML
    to a dictionary. Traverses/scans the key/values in dictionary for lists,
    dicts and simple values.
    Stores the results in a list of dictionaries, that converts to a DataFrame

    Args:
        file_path: The path to the XML file.

    Returns:
        pandas.DataFrame: A transformed DataFrame that contains all values
        from the XML

    Raises:
        ValueError: If invalid gcs-file or xml-file.
    """
    if utils.is_valid_xml(file_path):

        xml_dict = _read_single_xml_to_dict(file_path)
        root_element = next(iter(xml_dict.keys()))
        input_dict = xml_dict[root_element]

        final_dict = _flatten_dict(input_dict)

        final_df = pd.DataFrame(
            list(final_dict.items()), columns=["FELTNAVN", "FELTVERDI"]
        )

        final_df["COUNTER"] = final_df["FELTNAVN"].apply(_extract_counter)
        final_df["LEVEL"] = final_df["COUNTER"].apply(lambda x: x[::-1])
        final_df["FELTNAVN"] = final_df["FELTNAVN"].str.replace(
            r"£.*?\$", "", regex=True
        )

        final_df = final_df.drop(["COUNTER"], axis=1)

        return final_df

    else:
        error_message = f"File is not a valid XML-file: {file_path}"
        raise ValueError(error_message)


def create_isee_filename(file_path: str) -> str | None:
    """Creates a filename based on the contents of an XML file and the provided file path.

    Args:
        file_path: The path to the XML file.

    Returns:
        The generated filename if successful, otherwise None.
    """
    # Read XML-file
    if utils.is_gcs(file_path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(file_path, mode="r") as f:
            xml_content = f.read()

    else:
        with open(file_path) as f:
            xml_content = f.read()

    # Parse the XML content
    root = ET.fromstring(xml_content)

    # Find the value of raNummer
    ra_nummer_element = root.find(".//InternInfo/raNummer")
    if ra_nummer_element is not None:
        ra_nummer_value = ra_nummer_element.text

    # find angiver_id
    angiver_id = _extract_angiver_id(file_path)

    # Create the filename
    filename = f"{ra_nummer_value}A3_{angiver_id}.csv"

    return filename
