"""For flattening Altinn3 xml.files for Dynarev-base in Oracle.

This module contains the functions for flattening the Altinn3 xml-files that
should be loaded into our on-prem Oracle database for Dynarev-base. These generic
functions currently supports xml files that do not contain kodelister. It requires
the user to specify how to recode old fieldnames of Altinn2 to the new names
of Altinn3. This is done in a separate file.
"""


import pandas as pd
import xmltodict
from dapla import FileClient
from altinn import utils


def validate_interninfo(file_path):
    """Validate interninfo.

    Validates the presence of required keys 
    ('enhetsIdent', 'enhetsType', 'delregNr') 
    within the 'interninfo' dictionary of an XML file converted
    to a dictionary.

    Args:
    - file_path (str): The file path to the XML file.

    Returns:
    - bool: True if all required keys exist in the 'interninfo' 
    dictionary, False otherwise.
    """
    xml_dict = read_single_xml_to_dict(file_path)
    root_element = list(xml_dict.keys())[0]

    required_keys = ['enhetsIdent', 'enhetsType', 'delregNr']

    missing_keys = [key for key in required_keys if key not in xml_dict[root_element]['interninfo']]

    if missing_keys:
        print("The following required keys are missing in ['interninfo']:")
        for key in missing_keys:
              print(key) 
        print("No output will be produced")

        return False
    else:
        return True
    

def read_single_xml_to_dict(file_path):
    """Reads XML-file from GCS and transforms it to a dictionary.

    Args:
        file_path (str): The path to the XML file

    Returns:
        A dictionary with data from a XML
    """
    fs = FileClient.get_gcs_file_system()

    with fs.open(file_path, mode="r") as xml_file:
        data_dict = xmltodict.parse(xml_file.read())

    return data_dict

def extract_angiver_id(file_path):
    """Collects angiver_id from the filepath.

    Args:
        file_path (str): The path to the XML file

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

    
def make_isee_dict(
    dict_key, dict_value, counter, subcounter, key_level1, key_level2, key_level3, level
):
    """Makes a dictionary.

    that contains key/values to build a Dataframe in ISEE-format.

    Takes several args and builds a dict that contains key/vaules similar to columns 
    in a ISSE-dataframe.
    Appends to a list of dicts

    Args:
        dict_key (str): refers to 'FELTNAVN' in ISEE
        dict_value (str): refers to 'FELTVERDI' in ISEE
        counter (int): keeps track of levels in ISEE
        subcounter (int): keeps track of sublevels in ISEE
        key_level1 (int): is used to keep track of the origin/level of the value
        key_level2 (int): is used to keep track of the origin/level of the value
        key_level3 (int): is used to keep track of the origin/level of the value
        level (int): controls the number of levels to be added to 'CHILD_OF'

    Returns:
        dict: Dictionary containing ISSE-columns
    """
    data_dict = {
        "FELTNAVN": dict_key,
        "FELTVERDI": dict_value,
        "RAD_NR": counter,
        "REP_NR": subcounter,
        "CHILD_OF": "SkjemaData"
        if level == 0
        else f"SkjemaData_{key_level1}"
        if level == 1
        else f"SkjemaData_{key_level1}_{key_level2}"
        if level == 2
        else f"SkjemaData_{key_level1}_{key_level2}_{key_level3}",
    }

    return data_dict


def make_angiver_row_df(file_path):
    """Makes a Dataframe with a single row containg info on ANGIVERID.

    A DataFrame that will be concatinated on the end of the ISSE-DataFrame

    Args:
        file_path (str): The path to the XML file

    Returns:
        A DataFrame with a single row containing infor on ANGIVER_ID in ISEE-format

    """
    xml_dict = read_single_xml_to_dict(file_path)
    root_element = list(xml_dict.keys())[0]
    angiver_id_row = {
        "FELTNAVN": "ANGIVER_ID",
        "FELTVERDI": extract_angiver_id(file_path),
        "RAD_NR": 0,
        "REP_NR": 0,
        "IDENT_NR": xml_dict[root_element]["interninfo"]["enhetsIdent"],
        "VERSION_NR": extract_angiver_id(file_path),
        "DELREGNR": xml_dict[root_element]["interninfo"]["delregNr"],
        "ENHETS_TYPE": xml_dict[root_element]["interninfo"]["enhetsType"],
        "SKJEMA_ID": xml_dict[root_element]['interninfo']['raNummer']
    }

    return pd.DataFrame([angiver_id_row])


def add_lopenr(df):
    """Add lopenr.

    Adds a suffix to the 'FELTNAVN' column based on conditions related to 'RAD_NR' and 'REP_NR'.
    Checks if input df contains complex structures (tabell i tabell), lists values of FELTNAVN that is not processed.
    Removes columns RAD_NR and REP_NR

    Args:
    - df (pandas.DataFrame): The input DataFrame must contain columns 'FELTNAVN', 'RAD_NR', and 'REP_NR'.

    Returns:
    - pandas.DataFrame: The DataFrame with modifications to the 'FELTNAVN' column based on conditions:
        1. If 'RAD_NR' is 0 and 'REP_NR' is greater than 0, appends '_REP_NR' to 'FELTNAVN'.
        2. If 'RAD_NR' is greater than 0 and 'REP_NR' is 0, appends '_RAD_NR' to 'FELTNAVN'.
    """
    df.loc[(df['RAD_NR'] == 0) & (df['REP_NR'] > 0), 'FELTNAVN'] = df['FELTNAVN'] + '_' + df['REP_NR'].astype(str)
    df.loc[(df['RAD_NR'] > 0) & (df['REP_NR'] == 0), 'FELTNAVN'] = df['FELTNAVN'] + '_' + df['RAD_NR'].astype(str)


    rad_nr_gt_0 = (df['RAD_NR'] > 0).any()
    rep_nr_gt_0 = (df['REP_NR'] > 0).any()

    if rad_nr_gt_0 and rep_nr_gt_0:

        duplicate_feltnavn = set(df[df.duplicated('FELTNAVN')]['FELTNAVN'])

        if duplicate_feltnavn:
            print('\033[91m' + "XML-inneholder kompliserte strukturer (Tabell i tabell), det kan være nødvendig med ytterligere behandling av datagrunnlaget før innlasting til ISEE.")
            print("Ikke alle gjentagende FELTNAVN har fått påkoblet løpenummer:" + '\033[0m')
            for var in duplicate_feltnavn:
                print(var)

    df.drop(columns=['RAD_NR', 'REP_NR'], inplace=True)  # Drop RAD_NR and REP_NR columns

    return df


def isee_transform(file_path, mapping=None):
    """Transforms a XML to ISEE-format using xmltodict.
        
    Transforms the XML to ISEE-format by using xmltodict to transform the XML 
    to a dictionary. Traverses/scans the key/values in dictionary for lists, 
    dicts and simple values.
    Stores the results in a list of dictionaries, that converts to a DataFrame
        
    Args:
        file_path (str): The path to the XML file 
        mapping (dict): The mapping dictionary to map variable names in the
            'feltnavn' column. The default value is an empty dictionary
            (if mapping is not needed). 
            
    Returns:
        pandas.DataFrame: A transformed DataFrame which aligns with the 
        ISEE dynarev format.
    """
    
   
    if utils.is_gcs(file_path):
        
        if utils.is_valid_xml(file_path):

            if validate_interninfo(file_path):

                if mapping is None:
                    mapping = {}

                xml_dict = read_single_xml_to_dict(file_path)
                root_element = list(xml_dict.keys())[0]
                input_dict = xml_dict[root_element]['SkjemaData']

                #pprint(input_dict, sort_dicts=False, width=200, indent=2)
                
                
                final_list = []
                for key, value in input_dict.items():
                    counter = 0
                    subcounter = 0
                    
                    if isinstance(value, list):
                        for element in value:
                            counter += 1
                            
                            if isinstance(element, dict):
                                for subkey, subvalue in element.items():
                                   
                                    if not isinstance(subvalue, (list, dict)):
                                        final_list.append(make_isee_dict(subkey, subvalue, counter, 0, key, None, None, 1))

                                    if isinstance(subvalue, list):
                                        subcounter = 1
                                        for subelement in subvalue:
                                            
                                            for subsubkey, subsubvalue in subelement.items():
                                                
                                                if not isinstance(subsubvalue, (list, dict)):
                                                    final_list.append(make_isee_dict(subsubkey, subsubvalue, counter, subcounter, key, subkey, None, 2))

                                                if isinstance(subsubvalue, (dict)):
                                                    for _, dictsubsubvalue in subsubvalue.items():

                                                        final_list.append(make_isee_dict(subsubkey, dictsubsubvalue, counter, subcounter, key, subkey, None, 2))

                                            subcounter += 1

                                    if isinstance(subvalue, dict):
                                        subcounter = 1
                                        
                                        for subsubkey, subsubvalue in subvalue.items():
                                            if not isinstance(subsubvalue, (list, dict)):
                                                
                                                final_list.append(make_isee_dict(subsubkey, subsubvalue, counter, subcounter, key, subkey, None, 2))

                                            if isinstance(subsubvalue, (dict)):
                                                
                                                for subsubsubkey, subsubsubvalue in subsubvalue.items():
                                                    
                                                    final_list.append(make_isee_dict(subsubsubkey, subsubsubvalue, counter, subcounter, key, subkey, subsubsubkey, 3))

                                        subcounter += 1




                    elif isinstance(value, dict):

                        for sub_dict_key, sub_dict_value in value.items():
                            counter = 1
                            

                            if not isinstance(sub_dict_value, (dict, list)):
                                final_list.append(make_isee_dict(sub_dict_key, sub_dict_value, counter, 0, key, None, None, 1))

                            if isinstance(sub_dict_value, list):
                                subcounter = 1
                                for subelement in sub_dict_value:

                                    for subkey, subvalue in subelement.items():
                                       
                                        if not isinstance(subvalue, (dict, list)):
                                            final_list.append(make_isee_dict(subkey, subvalue, counter, subcounter, key, sub_dict_key, None, 2))

                                        if isinstance(subvalue, dict):
                                            for dict_dict_key, dict_dict_value in subvalue.items():
                                                final_list.append(make_isee_dict(dict_dict_key, dict_dict_value, counter, subcounter, key, sub_dict_key, None, 2))

                                            subcounter += 1
                                counter += 1

                            if isinstance(sub_dict_value, dict):
                                for dict_dict_key, dict_dict_value in sub_dict_value.items():

                                    if not isinstance(dict_dict_value, (list, dict)):
                                        final_list.append(make_isee_dict(dict_dict_key, dict_dict_value, counter, 0, key, sub_dict_key, None, 2))

                                    if isinstance(dict_dict_value, dict):
                                        for dict_dict_dict_key,  dict_dict_dict_value in dict_dict_value.items():
                                            
                                            final_list.append(make_isee_dict(dict_dict_dict_key, dict_dict_dict_value, counter, 0, key, sub_dict_key, dict_dict_key, 3))

                    elif not isinstance(value, (dict, list)):
                        final_list.append(make_isee_dict(key, value, counter, 0, None, None, None, 0))        

                        
                final_df = pd.DataFrame(final_list)
                final_df['IDENT_NR'] = xml_dict[root_element]['interninfo']['enhetsIdent']
                final_df['VERSION_NR'] = extract_angiver_id(file_path)
                final_df['DELREGNR'] = xml_dict[root_element]['interninfo']['delregNr']
                final_df['ENHETS_TYPE'] = xml_dict[root_element]['interninfo']['enhetsType']
                final_df['SKJEMA_ID'] = xml_dict[root_element]['interninfo']['raNummer']

                final_df = final_df[final_df['FELTNAVN'] != '@xsi:nil']

                final_df['FELTNAVN'] = final_df['CHILD_OF'] + '_' + final_df['FELTNAVN']
                final_df = final_df.drop(['CHILD_OF'], axis=1)

                final_df = pd.concat([final_df, make_angiver_row_df(file_path)], ignore_index=True)

                final_df["FELTNAVN"] = (final_df["FELTNAVN"].str.removeprefix("SkjemaData_"))


                if mapping is not None:
                    final_df["FELTNAVN"].replace(mapping, inplace=True)

                final_df = add_lopenr(final_df)   

                return final_df
        
        else:
            print(f"File is not a valid XML-file: {file_path}")
    else:
        print(f"File is not a valid GCS-file: {file_path}")
