# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: egentesting
#     language: python
#     name: egentesting
# ---

# %%
print("Test")
import logging

import altinn as alt
from flatten import transform_table_in_table

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    # datefmt='%Y-%m-%d %H:%M:%S',
    encoding="utf-8",
)
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

# %%
# source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/test/2024/12/4/a9b867d4a91c_e9df2f21-fec7-42e8-8473-33789acbcbba/form_a9b867d4a91c.xml"
# source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/2025/1/21/77ded7fb41b0_284e7d2c-087c-46b2-9ed0-dfcd70d1130c/form_77ded7fb41b0.xml"
# source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/2025/1/22/cf67b3770f5f_311a7bf6-10fe-4888-8e43-f1e4a80142cc/form_cf67b3770f5f.xml"
source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/2025/1/8/006149bef144_79f17de1-665a-4477-a589-2d60d5536c29/form_006149bef144.xml"
source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/2024/12/12/a7b963e8aa6b_6abd2d38-590b-4222-baea-a7fa45565bb9/form_a7b963e8aa6b.xml"
source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/2024/12/18/7d350df20e11_e6d0b91d-28cb-40b5-b1c1-d80617416a36/form_7d350df20e11.xml"


# %%
def get_counter_value(data):
    if data is None:
        return 1
    try:
        return (
            max(
                data.loc[data["FELTNAVN"].str.match(r"Areal_\d{3}")]["FELTNAVN"]
                .str[-3:]
                .astype(int)
            )
            + 1
        )
    except ValueError:
        return 1


# %%
tabell_i_tabell_felter = [
    "NyEngHusdyrGjodsGroup_",
    "NyEngMineralGjodsGroup_",
    "EtaEngMineralGjodsGroup_",
    "EtaEngHusdyrGjodsGroup_",
    "EtaEngAnnetGjodsGroup_",
    "AnVekstHusdyrGjodsGroup_",
    "AnVekstMineralGjodsGroup_",
    "AnVekstAnnetGjodsGroup_",
    "AnVekstMinBladGjodsGroup_",
]

# %% [raw]
# import dapla as dp
# for i in dp.FileClient.get_gcs_file_system().glob(f"gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/**/*.xml"):
#     df = alt.isee_transform(f"gs://{i}")
#     for i in tabell_i_tabell_felter:
#         table_data = df.loc[df["FELTNAVN"].str.startswith(i)]
#         rest_of_data = df.drop(table_data.index, axis=0)
#         assert df.shape[0] == table_data.shape[0] + rest_of_data.shape[0], f"Mismatch between length of input data {df.shape[0]} and the split data.\n rows in table_data: {table_data.shape[0]}\n rows in rest_of_data: {rest_of_data.shape[0]}"
#         df = transform_table_in_table(
#             table_data=table_data,
#             rest_of_data=rest_of_data,
#             table_fields=["Navn", "Nitrogen", "Kalium", "Fosfor"],
#             row_fields=["Aarstid", "Areal", "Mengde"],
#             counter_starting_value=get_counter_value(df),
#         )

# %%
df = alt.isee_transform(source_file)

for i in tabell_i_tabell_felter:
    table_data = df.loc[df["FELTNAVN"].str.startswith(i)]
    rest_of_data = df.drop(table_data.index, axis=0)
    assert (
        df.shape[0] == table_data.shape[0] + rest_of_data.shape[0]
    ), f"Mismatch between length of input data {df.shape[0]} and the split data.\n rows in table_data: {table_data.shape[0]}\n rows in rest_of_data: {rest_of_data.shape[0]}"
    df = transform_table_in_table(
        table_data=table_data,
        rest_of_data=rest_of_data,
        table_fields=["Navn", "Nitrogen", "Kalium", "Fosfor"],
        row_fields=["Aarstid", "Areal", "Mengde"],
        counter_starting_value=get_counter_value(df),
    )

# %%
df.tail(40)

# %%
alt.FileInfo(f"{source_file}").pretty_print()

# %%
