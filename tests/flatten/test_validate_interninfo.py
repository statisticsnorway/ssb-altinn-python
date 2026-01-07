# Import the function under test
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch

from altinn.flatten import _validate_interninfo

# Anta at funksjonen ligger i modul `validators.py`.
# Juster importene til ditt faktiske modulnavn/sti.


def make_xml_dict(
    root: str = "Root", intern_info: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Hjelpefunksjon for å lage en xml_dict-struktur som funksjonen forventer."""
    return {root: {"InternInfo": intern_info} if intern_info is not None else {}}


@pytest.mark.parametrize(
    "altinn_type, intern_info",
    [
        ("RA", {"enhetsIdent": "123", "enhetsType": "ABC", "delregNr": "01"}),
        ("RS", {"enhetsOrgNr": "987654321", "enhetsType": "DEF", "delregNr": "02"}),
    ],
)
def test_validate_returns_true_for_valid_keys(
    monkeypatch: MonkeyPatch,
    altinn_type: str,
    intern_info: dict[str, Any],
) -> None:
    """Skal returnere True når alle påkrevde nøkler finnes, for både RA og RS."""
    xml_dict = make_xml_dict(intern_info=intern_info)

    # Monkeypatch avhengigheter
    monkeypatch.setattr(
        "altinn.flatten._read_single_xml_to_dict", lambda path: xml_dict
    )
    monkeypatch.setattr("altinn.flatten._check_altinn_type", lambda path: altinn_type)

    assert _validate_interninfo("dummy.xml") is True


@pytest.mark.parametrize(
    "altinn_type, intern_info, expected_missing",
    [
        # RA mangler enhetsIdent
        ("RA", {"enhetsType": "ABC", "delregNr": "01"}, ["enhetsIdent"]),
        # RA mangler enhetsType + delregNr
        ("RA", {"enhetsIdent": "123"}, ["enhetsType", "delregNr"]),
        # RS mangler enhetsOrgNr
        ("RS", {"enhetsType": "DEF", "delregNr": "02"}, ["enhetsOrgNr"]),
        # RS mangler enhetsType + delregNr
        ("RS", {"enhetsOrgNr": "987654321"}, ["enhetsType", "delregNr"]),
    ],
)
def test_validate_raises_value_error_on_missing_keys(
    monkeypatch: MonkeyPatch,
    altinn_type: str,
    intern_info: dict[str, Any],
    expected_missing: list[str],
) -> None:
    """Skal kaste ValueError med liste over manglende nøkler når InternInfo ikke er komplett."""
    xml_dict = make_xml_dict(intern_info=intern_info)

    monkeypatch.setattr(
        "altinn.flatten._read_single_xml_to_dict", lambda path: xml_dict
    )
    monkeypatch.setattr("altinn.flatten._check_altinn_type", lambda path: altinn_type)

    with pytest.raises(ValueError) as excinfo:
        _validate_interninfo("dummy.xml")

    msg = str(excinfo.value)
    # Sjekk at alle forventede manglende nøkler nevnes i feilmeldingen
    for missing in expected_missing:
        assert missing in msg

    assert "Manglende påkrevde felter i 'InternInfo'" in msg


def test_validate_raises_value_error_on_invalid_type(
    monkeypatch: MonkeyPatch,
) -> None:
    """Skal kaste ValueError hvis skjematype ikke er RA eller RS."""
    xml_dict = make_xml_dict(intern_info={"enhetsType": "ABC"})  # innhold uviktig

    monkeypatch.setattr(
        "altinn.flatten._read_single_xml_to_dict", lambda path: xml_dict
    )
    monkeypatch.setattr("altinn.flatten._check_altinn_type", lambda path: "XX")

    with pytest.raises(ValueError) as excinfo:
        _validate_interninfo("dummy.xml")

    assert "Ugyldig skjematype" in str(excinfo.value)


def test_validate_raises_when_interninfo_missing(
    monkeypatch: MonkeyPatch,
) -> None:
    """Skal kaste ValueError når 'InternInfo' mangler helt (dvs. tom dict)."""
    xml_dict = make_xml_dict(intern_info=None)  # Ingen InternInfo-nøkkel

    monkeypatch.setattr(
        "altinn.flatten._read_single_xml_to_dict", lambda path: xml_dict
    )
    monkeypatch.setattr("altinn.flatten._check_altinn_type", lambda path: "RA")

    with pytest.raises(ValueError) as excinfo:
        _validate_interninfo("dummy.xml")

    msg = str(excinfo.value)
    # For RA uten InternInfo mangler alle tre
    for key in ["enhetsIdent", "enhetsType", "delregNr"]:
        assert key in msg
    assert "Manglende påkrevde felter i 'InternInfo'" in msg


def test_validate_message_contains_altinn_context(
    monkeypatch: MonkeyPatch,
) -> None:

    xml_dict = make_xml_dict(intern_info={"enhetsType": "ABC", "delregNr": "01"})
    monkeypatch.setattr(
        "altinn.flatten._read_single_xml_to_dict", lambda path: xml_dict
    )
    monkeypatch.setattr("altinn.flatten._check_altinn_type", lambda path: "RA")

    with pytest.raises(ValueError) as excinfo:
        _validate_interninfo("dummy.xml")

    msg = str(excinfo.value)

    assert "Manglende påkrevde felter i 'InternInfo'" in msg
