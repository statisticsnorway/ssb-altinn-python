import pytest

from altinn import flatten


class DummyUtils:
    @staticmethod
    def is_valid_xml(file_path: str) -> bool:
        # Simulate valid/invalid XML based on file_path
        return file_path != "invalid.xml"


def dummy_validate_interninfo_true(file_path: str) -> bool:
    return True


def dummy_validate_interninfo_false(file_path: str) -> bool:
    return False


def test_validate_file_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patch utils and _validate_interninfo to simulate valid XML and valid interninfo
    monkeypatch.setattr(flatten, "utils", DummyUtils)
    monkeypatch.setattr(flatten, "_validate_interninfo", dummy_validate_interninfo_true)
    # Should not raise
    flatten._validate_file("valid.xml")


def test_validate_file_invalid_xml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(flatten, "utils", DummyUtils)
    monkeypatch.setattr(flatten, "_validate_interninfo", dummy_validate_interninfo_true)
    with pytest.raises(ValueError, match="File is not a valid XML-file: invalid.xml"):
        flatten._validate_file("invalid.xml")


def test_validate_file_missing_interninfo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(flatten, "utils", DummyUtils)
    monkeypatch.setattr(
        flatten, "_validate_interninfo", dummy_validate_interninfo_false
    )
    with pytest.raises(
        ValueError,
        match="File is missing one or more of the required keys in InternInfo",
    ):
        flatten._validate_file("valid.xml")
