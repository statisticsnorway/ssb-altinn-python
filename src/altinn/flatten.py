"""For flattening Altinn3 xml.files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. This are
generic functions that should support all types of xml-forms. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""


def isee_transform(df, mapping=None):
    """Transforms dataframe to ISEE-format.

    Transforms the given DataFrame by selecting certain columns and renaming
    them to align with the ISEE format. Optionally renames the feltnavn values
    to the correct ISEE variable names.

    Args:
        df (pandas.DataFrame): A DataFrame containing flattened data extracted
        from an altinn3 XML file.
        mapping (dict): The mapping dictionary to map variable names in the
        'feltnavn' column. The default value is an empty dictionary
        (if mapping is not needed).

    Returns:
        pandas.DataFrame: A transformed DataFrame which aligns with the ISEE
        format.
    """
    if mapping is None:
        mapping = {}

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
