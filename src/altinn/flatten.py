"""For flattening Altinn3 xml.files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. These generic
functions currently supports xml files that do not contain kodelister. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""

from .parser import ParseSingleXml


def isee_transform(file_path, mapping=None):
    """Transforms XML to ISEE-format.

    Transforms the XML file to align with the ISEE dynarev format by
    flattening the file, selecting necessary columns and renaming
    them. Optionally renames the feltnavn values to
    the correct ISEE variable names.

    Args:
        file_path (str): The path to the XML file.
        mapping (dict): The mapping dictionary to map variable names in the
            'feltnavn' column. The default value is an empty dictionary
            (if mapping is not needed).

    Returns:
        pandas.DataFrame: A transformed DataFrame which aligns with the
        ISEE dynarev format.
    """
    if mapping is None:
        mapping = {}

    xml_parser = ParseSingleXml(file_path)
    df = xml_parser.to_dataframe()

    def extract_angiver_id():
        """Extracts angiver_id from file_path."""
        start_index = file_path.find("/form_") + len("/form_")
        end_index = file_path.find(".xml", start_index)
        if start_index != -1 and end_index != -1:
            extracted_text = file_path[start_index:end_index]
            return extracted_text
        else:
            return None

    angiver_id = extract_angiver_id()

    df["SkjemaData_ANGIVER_ID"] = angiver_id

    skjemadata_cols = df.filter(regex="^SkjemaData").columns.tolist()

    intern_cols = [
        "InternInfo_raNummer",
        "InternInfo_delregNr",
        "InternInfo_enhetsIdent",
        "InternInfo_enhetsType",
    ]

    df_isee = df[intern_cols + skjemadata_cols]

    isee_rename = {
        "InternInfo_raNummer": "Skjema_id",
        "InternInfo_delregNr": "Delreg_nr",
        "InternInfo_enhetsIdent": "Ident_nr",
        "InternInfo_enhetsType": "Enhets_type",
    }

    df_isee = df_isee.melt(
        intern_cols, var_name="feltnavn", value_name="feltverdi"
    ).rename(columns=isee_rename)

    df_isee["feltnavn"] = (
        df_isee["feltnavn"]
        .str.removeprefix("SkjemaData_")
        .str.rstrip("0123456789")
        .str.removesuffix("_")
    )
    if mapping is not None:
        df_isee["feltnavn"].replace(mapping, inplace=True)

    df_isee["version_nr"] = angiver_id
    return df_isee
