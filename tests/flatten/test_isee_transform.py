from pathlib import Path

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
