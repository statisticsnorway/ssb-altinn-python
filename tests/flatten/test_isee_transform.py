from pathlib import Path

import pandas as pd
from pandas import testing as tm

from altinn.flatten import isee_transform


def test_isee_transform() -> None:
    xml_file = Path(__file__).parent / "data" / "form_373a35bb8808.xml"
    df = isee_transform(str(xml_file))
    print(df.tail())
    print(len(df))

    assert len(df) == 63


def test_isee_transform_tags_checkbox() -> None:
    xml_file = Path(__file__).parent / "data" / "form_1e7d1c3e69c0.xml"

    tags = ["SkjemaData", "Kontakt"]
    checkboxList = ["kontraktType"]

    df = isee_transform(
        str(xml_file), tag_list=tags, checkbox_vars=checkboxList, unique_code=False
    )
    print(df.tail(10))
    print(len(df))

    assert len(df) == 23


def test_isee_transform_simple_df() -> None:
    facit_file = Path(__file__).parent / "data" / "fascit_simple.parquet"
    facit_df = pd.read_parquet(facit_file)

    xml_file = Path(__file__).parent / "data" / "form_1e7d1c3e69c0.xml"

    df = isee_transform(str(xml_file))
    tm.assert_frame_equal(df, facit_df)


def test_isee_transform_tags_checkbox_df() -> None:
    facit_file = Path(__file__).parent / "data" / "fascit_tags_checkbox.parquet"
    facit_df = pd.read_parquet(facit_file)

    xml_file = Path(__file__).parent / "data" / "form_1e7d1c3e69c0.xml"

    tags = ["SkjemaData", "Kontakt"]
    checkboxList = ["kontraktType"]

    df = isee_transform(
        str(xml_file), tag_list=tags, checkbox_vars=checkboxList, unique_code=False
    )

    tm.assert_frame_equal(df, facit_df)
