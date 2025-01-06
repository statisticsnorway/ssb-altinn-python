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
source_file = "gs://ssb-primaer-j-skjema-data-kilde-prod/lbrund/altinn/test/2024/12/4/a9b867d4a91c_e9df2f21-fec7-42e8-8473-33789acbcbba/form_a9b867d4a91c.xml"

# %%
df = alt.isee_transform(source_file)

table_data = df.loc[df["FELTNAVN"].str.startswith("NyEngMineralGjodsGroup_")]
rest_of_data = df.drop(table_data.index, axis=0)
assert df.shape[0] == table_data.shape[0] + rest_of_data.shape[0]

output = transform_table_in_table(
    table_data=table_data,
    rest_of_data=rest_of_data,
    table_fields=["Navn"],
    row_fields=["Aarstid", "Areal", "Mengde"],
)


# %%
output.tail(40)

# %%
table_data = output.loc[output["FELTNAVN"].str.startswith("NyEngHusdyrGjodsGroup_")]
rest_of_data = output.drop(table_data.index, axis=0)
assert output.shape[0] == table_data.shape[0] + rest_of_data.shape[0]
output = transform_table_in_table(
    table_data=table_data,
    rest_of_data=rest_of_data,
    table_fields=["Navn"],
    row_fields=["Aarstid", "Areal", "Mengde"],
    counter_starting_value=max(
        output.loc[output["FELTNAVN"].str.match(r"Areal_\d{3}")]["FELTNAVN"]
        .str[-3:]
        .astype(int)
    )
    + 1,
)

# %%
output.tail(40)

# %%

# %%
alt.FileInfo(f"{source_file}").pretty_print()
