"""For flattening Altinn3 xml.files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. This are
generic functions that should support all types of xml-forms. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""
import pandas as pd

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

    df = ParseSingleXml.to_dataframe(file_path)

    def extract_angiver_id():
        """Extracts the text after "/form_" and before ".xml" in string."""
    
        start_index = file_path.find("/form_") + len("/form_")
        end_index = file_path.find(".xml", start_index)
        if start_index != -1 and end_index != -1:
            extracted_text = file_path[start_index:end_index]
            return extracted_text
        else:
            return None
    
    angiver_id = extract_angiver_id()

    df = df.assign(angiver_id=angiver_id)

    return df
