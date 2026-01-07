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
