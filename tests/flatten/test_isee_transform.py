from pathlib import Path

from altinn.flatten import isee_transform


def test_isee_transform() -> None:
    xml_file = Path(__file__).parent / "data" / "form_373a35bb8808.xml"
    df = isee_transform(str(xml_file))
    print(df.tail())
    print(len(df))

    assert len(df) == 63
