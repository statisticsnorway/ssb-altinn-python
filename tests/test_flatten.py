from pathlib import Path

from altinn.flatten import isee_transform
from altinn.flatten import xml_transform


def test_isee_transform() -> None:
    xml_file = Path(__file__).parent / "data" / "form_373a35bb8808.xml"
    df = isee_transform(str(xml_file))
    print(df.head())
    print(len(df))

    assert len(df) == 61


def test_xml_transform() -> None:
    xml_file = Path(__file__).parent / "data" / "form_373a35bb8808.xml"
    df = xml_transform(str(xml_file))
    print(df.head())
    print(len(df))

    assert len(df) == 107
