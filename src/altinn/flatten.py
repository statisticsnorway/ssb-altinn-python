"""For flattening Altinn3 xml.files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. This are
generic functions that should support all types of xml-forms. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""

from .parser import ParseSingleXml


def isee_transform(file_path, mapping=None):
    """Transforms dataframe to ISEE-format.

    Transforms the given DataFrame by selecting certain columns and renaming
    them to align with the ISEE format. Optionally renames the feltnavn values
    to the correct ISEE variable names.

    Args:
        file_path (str): The path to the XML file.
        mapping (dict): The mapping dictionary to map variable names in the
            'feltnavn' column. The default value is an empty dictionary
            (if mapping is not needed).

    Returns:
        pandas.DataFrame: A transformed DataFrame which aligns with the ISEE
            format.
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

    skjemadata_df = df.filter(regex="^Skjemadata_")

    skjemadata_df = skjemadata_df.assign(ANGIVER_ID=angiver_id)

    skjemadata_df_columns = skjemadata_df.columns.tolist()

    skjemadata_df_columns.remove("ANGIVER_ID")

    all_others_df = df.drop(columns=skjemadata_df_columns)

    all_others_df = all_others_df.assign(skjemaVersjon=angiver_id)

    skjemadata_transposed = skjemadata_df.transpose().reset_index()

    skjemadata_transposed.columns = ["feltnavn", "feltverdi"]

    return skjemadata_transposed
