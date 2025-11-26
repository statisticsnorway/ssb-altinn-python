from pathlib import Path
from unittest.mock import patch

import pytest

from altinn.flatten import _validate_file


@pytest.fixture
def sample_path(tmp_path: Path) -> str:
    """Create a dummy XML file path for tests that need a real file path."""
    file_path = tmp_path / "test.xml"  # pyright: ignore[reportOperatorIssue]
    file_path.write_text("<root></root>")
    return str(file_path)


#
# --- BASIC SUCCESS CASE ---
#


def test_validate_file_success(sample_path: str) -> None:
    """Valid XML + valid InternInfo → no exception."""
    with (
        patch("altinn.flatten.utils.is_valid_xml", return_value=True),
        patch("altinn.flatten._validate_interninfo", return_value=True),
    ):
        _validate_file(sample_path)


#
# --- INVALID XML CASES ---
#


def test_invalid_xml_flagged_by_validator(sample_path: str) -> None:
    """is_valid_xml returns False → ValueError."""
    with patch("altinn.flatten.utils.is_valid_xml", return_value=False):
        with pytest.raises(ValueError) as exc:
            _validate_file(sample_path)
        assert "valid XML-file" in str(exc.value)


# def test_is_valid_xml_raises_error(sample_path):
#     """If utils.is_valid_xml raises ANY exception → propagate as ValueError."""
#     with patch("altinn.flatten.utils.is_valid_xml", side_effect=Exception("boom")):
#         with pytest.raises(Exception):
#             _validate_file(sample_path)
def test_is_valid_xml_raises_error(sample_path: str) -> None:
    """If utils.is_valid_xml raises ANY exception → propagate as ValueError."""
    with patch("altinn.flatten.utils.is_valid_xml", side_effect=Exception("boom")):
        with pytest.raises(ValueError):
            _validate_file(sample_path)


#
# --- MISSING INTERNINFO KEYS ---
#


@pytest.mark.parametrize(
    "altinn_type, expected_ident",
    [
        ("RA", "enhetsIdent"),
        ("RS", "enhetsOrgnr"),
        ("XYZ", "enhetsIdent/enhetsOrgnr"),  # unexpected value
        (None, "enhetsIdent/enhetsOrgnr"),  # None case
        (123, "enhetsIdent/enhetsOrgnr"),  # numeric case
    ],
)
def test_missing_interninfo_keys(
    sample_path: str, altinn_type: str, expected_ident: str
) -> None:
    """Missing InternInfo should raise ValueError and reference correct ident."""
    with (
        patch("altinn.flatten.utils.is_valid_xml", return_value=True),
        patch("altinn.flatten._validate_interninfo", return_value=False),
        patch("altinn.flatten._check_altinn_type", return_value=altinn_type),
    ):

        with pytest.raises(ValueError) as exc:
            _validate_file(sample_path)

        msg = str(exc.value)
        assert expected_ident in msg
        assert "required keys in InternInfo" in msg


# def test_validate_interninfo_raises(sample_path):
#     """If _validate_interninfo raises, it should propagate."""
#     with (
#         patch("altinn.flatten.utils.is_valid_xml", return_value=True),
#         patch("altinn.flatten._validate_interninfo", side_effect=Exception("x")),
#     ):
#         with pytest.raises(Exception):
#             _validate_file(sample_path)
def test_validate_interninfo_raises(sample_path: str) -> None:
    """If _validate_interninfo raises, it should propagate."""
    with (
        patch("altinn.flatten.utils.is_valid_xml", return_value=True),
        patch("altinn.flatten._validate_interninfo", side_effect=Exception("x")),
    ):
        with pytest.raises(Exception, match="x"):
            _validate_file(sample_path)


#
# --- FILE PATH EDGE CASES ---
#


def test_empty_file_path() -> None:
    """Empty path should be treated as invalid XML."""
    with patch("altinn.flatten.utils.is_valid_xml", return_value=False):
        with pytest.raises(ValueError):
            _validate_file("")


def test_nonexistent_file() -> None:
    """Nonexistent path: depends on is_valid_xml behavior; we simulate failure."""
    with patch("altinn.flatten.utils.is_valid_xml", return_value=False):
        with pytest.raises(ValueError):
            _validate_file("does_not_exist.xml")


def test_directory_instead_of_file(tmp_path: Path) -> None:
    """Passing a directory path should fail XML validation."""
    dir_path = str(tmp_path)
    with patch("altinn.flatten.utils.is_valid_xml", return_value=False):
        with pytest.raises(ValueError):
            _validate_file(dir_path)


#
# --- UNUSUAL XML CONTENT CASES ---
#


def test_empty_xml_file(tmp_path: Path) -> None:
    """Valid file path but empty content → assume is_valid_xml returns False."""
    file_path = tmp_path / "empty.xml"
    file_path.write_text("")
    with patch("altinn.flatten.utils.is_valid_xml", return_value=False):
        with pytest.raises(ValueError):
            _validate_file(str(file_path))


def test_malformed_xml_file(tmp_path: Path) -> None:
    """Malformed XML should be caught by is_valid_xml."""
    file_path = tmp_path / "bad.xml"
    file_path.write_text("<root><broken></root>")
    with patch("altinn.flatten.utils.is_valid_xml", return_value=False):
        with pytest.raises(ValueError):
            _validate_file(str(file_path))


#
# --- EDGE CASE: interninfo valid but check_altinn_type called anyway ---
# (Should never happen logically, but we test robustness)
#


def test_interninfo_valid_but_altinn_type_weird(sample_path: str) -> None:
    """If InternInfo is valid, _check_altinn_type should not affect success."""
    with (
        patch("altinn.flatten.utils.is_valid_xml", return_value=True),
        patch("altinn.flatten._validate_interninfo", return_value=True),
        patch("altinn.flatten._check_altinn_type", return_value="RS"),
    ):
        # Should still succeed
        _validate_file(sample_path)
