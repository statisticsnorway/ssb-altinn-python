"""For flattening Altinn3 xml-files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. These generic
functions currently supports xml files that do not contain kodelister. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""

import json
import os
import re
import xml.etree.ElementTree as ET
from collections.abc import MutableMapping
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

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
    items: list[tuple[Any, list[Any]]] = []

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

                else:
                    items.append((new_key, v))

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

    # Safely accessing 'InternInfo'
    intern_info = xml_dict[root_element].get("InternInfo", {})

    missing_keys = [key for key in required_keys if key not in intern_info]

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
    FORM_STRING = "/form_"
    start_index = file_path.find(FORM_STRING) + len(FORM_STRING)
    end_index = file_path.find(".xml", start_index)
    if start_index > len(FORM_STRING) - 1 and end_index != -1:
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
    angiver_id_row = {
        "FELTNAVN": "ANGIVER_ID",
        "FELTVERDI": _extract_angiver_id(file_path),
    }

    return pd.DataFrame([angiver_id_row])


def _create_levels_col(row: Any) -> int:
    """Create a 'LEVELS' column based on the length of the 'COUNTER' list in a row.

    Args:
        row: A dictionary representing a row of a DataFrame.

    Returns:
        The level value assigned to the row based on the 'COUNTER' list length.
    """
    # Safely access 'COUNTER' with a default empty list if the key is missing
    counter = row.get("COUNTER", [])

    if isinstance(counter, list) and len(counter) > 1:
        return 2
    elif isinstance(counter, list) and len(counter) == 1:
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


def _transform_checkbox_var(
    df: pd.DataFrame,
    checkbox_var: str,
    unique_code: bool = False,
    new_value: str = "1",
) -> pd.DataFrame:
    """transform_dict_code_vars.

    Transform a dictionary by removing a key and using its value as a new key with a new value.

    Args:
        df: The DataFrame to be transformed.
        checkbox_var: The value to remove from the DataFrame and flatten into seperate rows.
        unique_code: Bool for if you are using unique codes from Klass or not.
        new_value: Optional new value to add, default is 1 as str.

    Returns:
        df: The transformed DataFrame.
    """
    if checkbox_var in df["FELTNAVN"].values:

        checkbox_df = df[df["FELTNAVN"] == checkbox_var].copy()

        df = df[df["FELTNAVN"] != checkbox_var]

        for _, row in checkbox_df.iterrows():

            value = row["FELTVERDI"]
            values = utils._split_string(value)

            for value in values:

                new_row = row.copy()

                if unique_code is False:
                    new_row["FELTNAVN"] = checkbox_var + value
                    new_row["FELTVERDI"] = new_value
                else:
                    new_row["FELTNAVN"] = value
                    new_row["FELTVERDI"] = new_value

                df = pd.concat(
                    [df, pd.DataFrame([new_row]).reset_index(drop=True)],
                    ignore_index=True,
                )

    return df


def _pad_microseconds_to_six(date_str: str) -> str:
    """Ensures the datetime string has exactly six digits for microseconds.

    This function takes an ISO 8601 datetime string and ensures the fractional
    seconds part (after the dot) is exactly six digits long. If the string already
    includes fractional seconds, it pads with zeros or truncates as needed.
    If the string lacks microseconds but ends with 'Z', it adds '.000000' before the 'Z'.

    Args:
        date_str (str): An ISO 8601 datetime string, typically ending in 'Z' or a timezone offset.

    Returns:
        str: The datetime string with exactly six digits after the decimal point.

    Example:
        _pad_microseconds_to_six("2025-03-20T15:54:40.637Z")
        -> "2025-03-20T15:54:40.637000Z"
    """
    if "." in date_str:
        before_dot, after_dot = date_str.split(".", 1)
        fractional, suffix = re.split(r"[Z+-]", after_dot, maxsplit=1)
        suffix = after_dot[len(fractional) :]  # capture the 'Z' or '+00:00'

        # Pad or truncate to 6 digits
        fractional = (fractional + "000000")[:6]
        return f"{before_dot}.{fractional}{suffix}"
    else:
        # Add .000000 if missing
        return date_str.replace("Z", ".000000Z")


def _convert_to_oslo_time(utc_time_str: str) -> str:
    """Converts a UTC time string to Europe/Oslo-time.

    Converts a UTC time string with high-precision microseconds to a time string
    in the 'Europe/Oslo' timezone, truncating microseconds to six digits if necessary.

    This function handles ISO 8601 formatted strings that may end with 'Z' (indicative of UTC).
    It truncates microseconds to the first six digits for compatibility with Python's datetime parsing,
    adjusts for the timezone, and handles daylight saving changes applicable to Oslo.

    Args:
        utc_time_str: The UTC time string in ISO 8601 format, potentially ending with 'Z'.

    Returns:
        The time string converted to the 'Europe/Oslo' timezone in ISO 8601 format.
    """
    # ensure rigth format on date-string to avoid invalid iso-format ValueError
    utc_time_str = _pad_microseconds_to_six(utc_time_str)

    # Handle 'Z' and truncate microseconds to six digits if necessary
    if utc_time_str.endswith("Z"):
        utc_time_str = utc_time_str[:-1] + "+00:00"  # Convert 'Z' to '+00:00' for UTC

    # Truncate to six decimal places for seconds
    dot_index = utc_time_str.find(".")
    if dot_index != -1:
        # Ensure only six digits in microseconds part, plus handle remainder of string format
        utc_time_str = (
            utc_time_str[: dot_index + 7] + utc_time_str[utc_time_str.rfind("+") :]
        )

    # Convert to datetime with timezone aware
    utc_datetime = datetime.fromisoformat(utc_time_str)
    oslo_timezone = ZoneInfo("Europe/Oslo")
    oslo_datetime = utc_datetime.astimezone(oslo_timezone)

    return oslo_datetime.isoformat()


def _make_meta_df(meta_dict: dict[str, str]) -> pd.DataFrame:
    """Creates a pandas DataFrame from a dictionary of metadata.

    This function iterates over a dictionary, converting each key-value pair into a row in a DataFrame.
    The keys are transformed to uppercase and used as column names. For specific fields, such as
    'ALTINNTIDSPUNKTLEVERT', the function also modifies the content of the DataFrame by applying
    a conversion function to adjust the time to the Oslo time zone.

    Parameters:
        meta_dict: A dictionary where each key-value pair represents the field name and its value.

    Returns:
        pd.DataFrame: A DataFrame where each row represents one key-value pair from the input dictionary,
        with 'FELTNAVN' and 'FELTVERDI' as column headers.
    """
    rows = []

    for key, value in meta_dict.items():
        rows.append({"FELTNAVN": key.upper(), "FELTVERDI": value})

    df = pd.DataFrame(rows)

    df.loc[df["FELTNAVN"] == "ALTINNTIDSPUNKTLEVERT", "FELTVERDI"] = df.loc[
        df["FELTNAVN"] == "ALTINNTIDSPUNKTLEVERT", "FELTVERDI"
    ].apply(_convert_to_oslo_time)

    return df


def _read_json_meta(file_path: str) -> Any | None:
    """Reads a JSON file into a Dict.

    Converting the file path from an XML file path to a JSON file path
    by replacing 'form_' with 'meta_' and '.xml' with '.json'.

    Args:
        file_path: The original XML file path.

    Returns:
        The content of the JSON file as a Dict, or None if the file does not exist.
    """
    json_file_path = file_path.replace("form_", "meta_").replace(".xml", ".json")

    if utils.is_gcs(json_file_path):

        fs = FileClient.get_gcs_file_system()

        if fs.exists(json_file_path):
            try:
                with fs.open(json_file_path, "r", encoding="utf-8") as file:
                    return json.load(file)

            except json.JSONDecodeError as e:
                print(f"Error reading JSON file: {e}")
                return None

        else:
            return None

    else:
        if os.path.exists(json_file_path):
            with open(json_file_path, encoding="utf-8") as file:
                return json.load(file)
        else:
            return None


def _validate_file(file_path: str) -> None:
    """Validates that the provided XML file is valid and contains required InternInfo keys.

    This function checks whether the file at `file_path` is a valid XML file and
    whether it contains the necessary keys in the `InternInfo` section:
    'enhetsIdent', 'enhetsType', and 'delregNr'.

    Args:
        file_path : Path to the XML file to validate.

    Raises:
        ValueError: If the file is not a valid XML file.
        ValueError: If the file does not contain all required InternInfo keys.
    """
    if not utils.is_valid_xml(file_path):
        raise ValueError(f"File is not a valid XML-file: {file_path}")

    if not _validate_interninfo(file_path):
        raise ValueError(
            f"File is missing one or more of the required keys in InternInfo "
            f"['enhetsIdent', 'enhetsType', 'delregNr']: {file_path}"
        )


def _parse_tag_elements(
    xml_dict: dict[str, Any], root_element: str, tag_list: list[str]
) -> pd.DataFrame:
    """Parses specified tag elements from a nested XML dictionary, flattens their contents, and returns them as a concatenated DataFrame.

    Args:
        xml_dict : The dictionary representation of the XML data.
        root_element : The root element key within the XML dictionary to search for tags.
        tag_list : A list of tag names to extract and flatten from the root element.

    Returns:
        pd.DataFrame: A DataFrame containing the concatenated flattened tag data with columns "FELTNAVN" and "FELTVERDI".
                      Returns an empty DataFrame if none of the tags are found.
    """
    dfs = []
    for tag in tag_list:
        tag_data = xml_dict[root_element].get(tag)
        if tag_data:
            tag_dict = _flatten_dict(tag_data)
            tag_df = pd.DataFrame(tag_dict.items(), columns=["FELTNAVN", "FELTVERDI"])
            dfs.append(tag_df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _attach_metadata(file_path: str) -> pd.DataFrame:
    """Reads metadata from a JSON file at the specified file path and returns it as a pandas DataFrame.

    Args:
        file_path : The path to the JSON metadata file.

    Returns:
        pd.DataFrame: A DataFrame containing the metadata if available; otherwise, an empty DataFrame.
    """
    meta_dict = _read_json_meta(file_path)
    return _make_meta_df(meta_dict) if meta_dict else pd.DataFrame()


def _add_interninfo_columns(
    df: pd.DataFrame, xml_dict: dict[str, Any], root_element: str, file_path: str
) -> pd.DataFrame:
    """Adds internal information columns to the given DataFrame based on the provided XML dictionary and file path.

    Parameters:
        df : The DataFrame to which internal information columns will be added.
        xml_dict : A dictionary representation of the XML data.
        root_element : The root element key in the XML dictionary.
        file_path : The file path used to extract the version number.

    Returns:
        pd.DataFrame: The modified DataFrame with additional internal information columns.

    Notes:
        - Adds columns: 'IDENT_NR', 'VERSION_NR', 'DELREG_NR', 'ENHETS_TYPE', 'SKJEMA_ID', 'COUNTER', and 'LEVELS'.
        - Removes rows where 'FELTNAVN' contains '@xsi:nil'.
        - Cleans and processes 'FELTNAVN' and 'FELTVERDI' columns.
        - Relies on helper functions: _extract_angiver_id, _extract_counter, and _create_levels_col.
    """
    interninfo = xml_dict[root_element]["InternInfo"]
    df["IDENT_NR"] = interninfo["enhetsIdent"]
    df["VERSION_NR"] = _extract_angiver_id(file_path)
    df["DELREG_NR"] = interninfo["delregNr"]
    df["ENHETS_TYPE"] = interninfo["enhetsType"]
    df["SKJEMA_ID"] = interninfo["raNummer"] + "A3"
    df = df[~df["FELTNAVN"].str.contains("@xsi:nil")].copy()
    df.loc[:, "COUNTER"] = df["FELTNAVN"].apply(_extract_counter)
    df["FELTNAVN"] = df["FELTNAVN"].str.replace(r"£.*?\$", "", regex=True).str.strip()
    if "FELTVERDI" in df.columns:
        df["FELTVERDI"] = df["FELTVERDI"].astype(str).str.replace("\n", " ")
    df["LEVELS"] = df.apply(_create_levels_col, axis=1)
    return df


def isee_transform(
    file_path: str,
    mapping: dict[str, str] | None = None,
    tag_list: list[str] | None = None,
    checkbox_vars: list[str] | None = None,
    unique_code: bool = False,
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
        tag_list: A list containing the tags in the XML that will be flatten
            The default value is ['SkjemaData']
        checkbox_vars: Optional list of str for elements from xml containing KLASS codes.
        unique_code: Bool for if you are using unique codes from Klass or not.

    Returns:
        pandas.DataFrame: A transformed DataFrame which aligns with the
        ISEE dynarev format.
    """
    _validate_file(file_path)
    if mapping is None:
        mapping = {}

    if tag_list is None:
        tag_list = ["SkjemaData"]

    xml_dict = _read_single_xml_to_dict(file_path)
    root_element = next(iter(xml_dict.keys()))

    final_df = _parse_tag_elements(xml_dict, root_element, tag_list)

    final_df = pd.concat(
        [final_df, _attach_metadata(file_path)], axis=0, ignore_index=True
    )

    final_df = pd.concat([final_df, _make_angiver_row_df(file_path)], ignore_index=True)

    final_df = _add_interninfo_columns(final_df, xml_dict, root_element, file_path)

    if checkbox_vars is not None:
        for checkbox_var in checkbox_vars:
            final_df = _transform_checkbox_var(final_df, checkbox_var, unique_code)

    if mapping is not None:
        final_df["FELTNAVN"] = final_df["FELTNAVN"].replace(mapping)

    final_df = _add_lopenr(final_df)

    columns_order = [
        "SKJEMA_ID",
        "DELREG_NR",
        "IDENT_NR",
        "ENHETS_TYPE",
        "FELTNAVN",
        "FELTVERDI",
        "VERSION_NR",
    ]

    final_df = final_df[columns_order]

    return final_df


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
