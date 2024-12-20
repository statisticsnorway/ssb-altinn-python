# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %%
import logging

import numpy as np
import pandas as pd

import altinn as alt

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    # datefmt='%Y-%m-%d %H:%M:%S',
    encoding="utf-8",
)
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


# %%
def transform_table_in_table(
    table_data: pd.DataFrame,
    rest_of_data: pd.DataFrame,
    table_fields: list[str],
    row_fields: list[str],
    counter_starting_value: int = 1,
):
    """Function for transforming a 'table in table' part of a flattened Altinn form into a shape that ISEE accepts as a dynamic list.

    Args:
        table_data (pd.DataFrame): Part of the form representing a 'table in table' component.
        rest_of_data (pd.DataFrame):
        counter_starting_value (int): Sets the lopenr to this value for the first part of the table object. If there are multiple table objects that belong in the same dynamic list, adjust this value to avoid duplicate FELTNAVN.

    Notes:
        A simple way to split data is using .loc to filter out relevant rows to a new dataframe, and then using the index to remove them from the primary dataset.

    Example:
            table_data = df.loc[df[FELTNAVN].str.startswith("tabell_prefix")]
            rest_of_data = df.drop(table_data.index, axis=0)
            assert df.shape[0] == table_data.shape[0]+rest_of_data.shape[0]
    """
    table_data = table_data[["FELTNAVN", "FELTVERDI"]]
    pattern = "|".join(table_fields)
    df_meta = table_data[
        table_data["FELTNAVN"].str.contains(pattern, case=False, na=False)
    ]
    pattern = "|".join(row_fields)
    df_row = table_data[
        table_data["FELTNAVN"].str.contains(pattern, case=False, na=False)
    ]
    relevante_lopenr = list(df_row["FELTNAVN"].str.split("_").str[-1].unique())
    df_meta = df_meta.loc[
        df_meta["FELTNAVN"].str.split("_").str[-1].isin(relevante_lopenr)
    ]
    flattened_table_data: pd.DataFrame = (
        pd.DataFrame()
    )  # Dataframe to be filled with flattened data.
    for lopenr in relevante_lopenr:
        row_data: dict = {}
        for felt in row_fields:
            row_data[felt] = tuple(
                df_row.loc[
                    (df_row["FELTNAVN"].str.endswith(lopenr))
                    & (df_row["FELTNAVN"].str.contains(felt))
                ]["FELTVERDI"].values
            )
        max_length = max(len(values) for values in row_data.values())
        test = pd.DataFrame(  # TODO: Må kanskje endres
            {
                key: list(values) + [np.nan] * (max_length - len(values))
                for key, values in row_data.items()
            }
        )
        table_data: dict = {}
        for felt in table_fields:
            logger.debug(f"df_meta for felt {felt}: \n{df_meta}")
            verdi = df_meta.loc[
                (df_meta["FELTNAVN"].str.endswith(lopenr))
                & (df_meta["FELTNAVN"].str.contains(felt))
            ]["FELTVERDI"].item()
            test[felt] = verdi

        current_count = (
            max(flattened_table_data["FELTNAVN"].str[-3:].astype(int)) + 1
            if "FELTNAVN" in flattened_table_data.columns
            else counter_starting_value
        )  # Finds the current number of rows in the dynamic list so the counting can continue.
        dynamic_list_rows = (
            test.reset_index()
            .assign(
                lopenr=lambda x: range(current_count, current_count + len(test))
            )  # This needs to be changed to count upwards for each row that exists, so they are not overwritten/become duplicates.
            .drop(columns="index")
            .melt(id_vars="lopenr", value_name="FELTVERDI")
            .assign(
                FELTNAVN=lambda x: x["variable"]
                + "_"
                + x["lopenr"].astype(str).str.zfill(3)
            )[["FELTNAVN", "FELTVERDI"]]
        )

        flattened_table_data = pd.concat([flattened_table_data, dynamic_list_rows])

    # Re-adding form metadata to flattened_table_data
    skjema_id = list(rest_of_data["SKJEMA_ID"].unique())[0]
    delreg_nr = list(rest_of_data["DELREG_NR"].unique())[0]
    ident_nr = list(rest_of_data["IDENT_NR"].unique())[0]
    enhets_type = list(rest_of_data["ENHETS_TYPE"].unique())[0]
    version_nr = list(rest_of_data["VERSION_NR"].unique())[0]
    flattened_table_data = (
        flattened_table_data.assign(SKJEMA_ID=skjema_id)
        .assign(DELREG_NR=delreg_nr)
        .assign(IDENT_NR=ident_nr)
        .assign(ENHETS_TYPE=enhets_type)
        .assign(VERSION_NR=version_nr)
    )

    return pd.concat([rest_of_data, flattened_table_data]).reset_index(drop=True)


# %%
source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/test/2024/12/4/a9b867d4a91c_e9df2f21-fec7-42e8-8473-33789acbcbba/form_a9b867d4a91c.xml"

# %%
df = alt.isee_transform(source_file)

table_data = df.loc[df["FELTNAVN"].str.startswith("AnVekstMineralGjodsGroup_")]
rest_of_data = df.drop(table_data.index, axis=0)
assert df.shape[0] == table_data.shape[0] + rest_of_data.shape[0]

output = flatten_table_in_table(
    table_data=table_data,
    rest_of_data=rest_of_data,
    table_fields=["Navn"],
    row_fields=["Aarstid", "Areal", "Mengde"],
)

# %%
alt.FileInfo(f"{source_file}").pretty_print()
