"""For flattening Altinn3 xml.files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. This are
generic functions that should support all types of xml-forms. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""

import pandas as pd
from dapla import FileClient, AuthClient
from google.cloud import storage
from lxml import etree
import codecs


def altinn3_flatten(bucket, filename):
    """
    Retrieves and processes an altinn3 XML file from the specified bucket.
    Flattens the XML data and returns a DataFrame.

    Args:
        bucket (str): The name of the bucket where the file is stored.
        filename (str): The name of the file that contains XML data.

    Returns:
        pandas.DataFrame: A DataFrame with flattened and transposed data from the XML file which contains internal info, contact info and form data.
    """
    token = AuthClient.fetch_google_credentials()
    client = storage.Client(credentials=token)
    bucket_obj = client.get_bucket(bucket)
    blob = bucket_obj.blob(filename)
    blob_data = blob.download_as_bytes()

    xml_data = codecs.decode(blob_data, 'utf-8')
    xml_data = xml_data.replace('<?xml version="1.0" encoding="utf-8"?>', '')

    root = etree.fromstring(xml_data)

    intern_info = root.find("InternInfo")
    intern_info_dict = {e.tag: e.text for e in intern_info.iter() if e.text and e.text.strip()}

    kontakt = root.find("Kontakt")
    kontakt_dict = {e.tag: e.text for e in kontakt.iter() if e.text and e.text.strip()}

    skjemadata = root.find("SkjemaData")
    skjemadata_dict = {e.tag: e.text for e in skjemadata.iter() if e.text and e.text.strip()}

    start_index = filename.rfind('_') + 1
    end_index = filename.rfind('.xml')
    angiver = filename[start_index:end_index]

    internkontakt_df = pd.DataFrame({**intern_info_dict, **kontakt_dict}, index=[0])
    internkontakt_df = internkontakt_df
    skjemadata_df = pd.DataFrame({**skjemadata_dict}, index=[0])
    skjemadata_df['ANGIVER_ID'] = angiver

    transposed = skjemadata_df.transpose().reset_index()
    transposed.columns = ['feltnavn', 'feltverdi']
    transposed['key'] = 'A'
    internkontakt_df['key'] = 'A'
    internkontakt_df['skjemaVersjon'] = angiver

    output = transposed.merge(internkontakt_df, how='left', on='key').drop(columns=['key'])

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
    df = df.rename(columns={'raNummer': 'Skjema_id', 
                            'delregNr': 'Delreg_nr', 
                            'enhetsIdent': 'Ident_nr', 
                            'enhetsType': 'Enhets_type', 
                            'skjemaVersjon': 'version_nr'})[['Skjema_id', 'Delreg_nr', 'Ident_nr', 'Enhets_type', 'feltnavn', 'feltverdi', 'version_nr']]

    df['feltnavn'] = df['feltnavn'].replace(mapping, inplace=False)

    return df