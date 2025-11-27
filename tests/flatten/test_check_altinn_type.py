from pathlib import Path

from altinn.flatten import _check_altinn_type


def test_check_altinn_type_rs() -> None:
    xml_file = Path(__file__).parent / "data" / "form_40b39a0b5ebd.xml"
    ra_type = _check_altinn_type(str(xml_file))
    assert ra_type == "RS"


def test_check_altinn_type_ra() -> None:
    xml_file = Path(__file__).parent / "data" / "form_373a35bb8808.xml"
    ra_type = _check_altinn_type(str(xml_file))
    assert ra_type == "RA"
