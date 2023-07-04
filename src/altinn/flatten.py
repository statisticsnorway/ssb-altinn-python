"""For flattening Altinn3 xml.files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. This are
generic functions that should support all types of xml-forms. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""

import re

import pandas as pd
from dapla import AuthClient
from dapla import FileClient
from defusedxml import ElementTree as etree


def altinn3_flatten(file):
    """
    Retrieves and processes an altinn3 XML file from the specified bucket.
    Flattens the XML data and returns a DataFrame.

    Args:
        file (str): The bucket and filename of the XML file, in one string.

    Returns:
        pandas.DataFrame: A DataFrame with flattened and transposed data from the XML file which contains internal info, contact info and form data.
    """
    fs = FileClient.get_gcs_file_system()

    try:
        with fs.open(file, mode="r") as f:
            xml_data = f.read()
    except FileNotFoundError:
        raise FileNotFoundError("The specified XML file was not found.")

    element_counts = {}
    regex_pattern = r"<([^/][^>]*)>"
    matches = re.findall(regex_pattern, xml_data)
    for match in matches:
        if match not in element_counts:
            element_counts[match] = 1
        else:
            element_counts[match] += 1

    for element, count in element_counts.items():
        if count > 1:
            suffix_count = 1
            for i, match in enumerate(matches):
                if match == element:
                    suffix = str(suffix_count)
                    if re.search(r"\d+$", element):
                        suffix = re.findall(r"\d+$", element)[0] + suffix
                    old_tag = f"<{element}>"
                    new_tag = f'<{element.rstrip("0123456789")}{suffix}>'
                    xml_data = xml_data.replace(old_tag, new_tag, 1)

                    old_end_tag = f"</{element}>"
                    new_end_tag = f'</{element.rstrip("0123456789")}{suffix}>'
                    xml_data = xml_data.replace(old_end_tag, new_end_tag, 1)

                    suffix_count += 1

    xml_data = xml_data.replace('<?xml version="1.0" encoding="utf-8"?>', "")
    root = etree.fromstring(xml_data)

    intern_info = root.find("InternInfo")
    intern_info_dict = {
        e.tag: e.text for e in intern_info.iter() if e.text and e.text.strip()
    }

    kontakt = root.find("Kontakt")
    kontakt_dict = {e.tag: e.text for e in kontakt.iter() if e.text and e.text.strip()}

    skjemadata = root.find("SkjemaData")
    skjemadata_dict = {
        e.tag: e.text for e in skjemadata.iter() if e.text and e.text.strip()
    }

    start_index = file.rfind("_") + 1
    end_index = file.rfind(".xml")
    angiver = file[start_index:end_index]

    internkontakt_df = pd.DataFrame({**intern_info_dict, **kontakt_dict}, index=[0])
    skjemadata_df = pd.DataFrame({**skjemadata_dict}, index=[0])
    skjemadata_df["ANGIVER_ID"] = angiver

    transposed = skjemadata_df.transpose().reset_index()
    transposed.columns = ["feltnavn", "feltverdi"]
    transposed["key"] = "A"
    internkontakt_df["key"] = "A"
    internkontakt_df["skjemaVersjon"] = angiver

    output = transposed.merge(internkontakt_df, how="left", on="key").drop(
        columns=["key"]
    )

    for element, count in element_counts.items():
        if count > 1:
            matching_values = output.feltnavn[output.feltnavn.str.startswith(element)]
            for value in matching_values:
                output["feltnavn"] = output["feltnavn"].apply(
                    lambda x: re.sub(r"\d+$", "", x) if x.startswith(element) else x
                )

    return output


def isee_transform(df, mapping={}):
    """
    Transforms the given DataFrame by selecting certain columns and renaming them to align with the ISEE format. Optionally renames the feltnavn values to the correct ISEE variable names.

    Args:
        df (pandas.DataFrame): A DataFrame containing flattened data extracted from an altinn3 XML file.
        mapping (dict): The mapping dictionary to map variable names in the 'feltnavn' column. The default value is an empty dictionary (if mapping is not needed).

    Returns:
        pandas.DataFrame: A transformed DataFrame which aligns with the ISEE format.
    """
    df = df.rename(
        columns={
            "raNummer": "Skjema_id",
            "delregNr": "Delreg_nr",
            "enhetsIdent": "Ident_nr",
            "enhetsType": "Enhets_type",
            "skjemaVersjon": "version_nr",
        }
    )[
        [
            "Skjema_id",
            "Delreg_nr",
            "Ident_nr",
            "Enhets_type",
            "feltnavn",
            "feltverdi",
            "version_nr",
        ]
    ]

    df["feltnavn"] = df["feltnavn"].replace(mapping, inplace=False)

    return df
