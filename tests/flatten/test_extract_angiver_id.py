import pytest
from altinn.flatten import _extract_angiver_id

@pytest.mark.parametrize(
    "file_path, expected_output",
    [
        ("path/to/form_12345.xml", "12345"),
        ("path/to/form_67890.xml/more", "67890"),
        ("path/no_form_here.xml", None),
        ("path/to/form_.xml", ""),
        ("", None),
        ("path/to/incorrect_form_123.xml_part/form_456.xml", "456"),
        ("path/to/.xml/form_789.xml", "789"),
    ],
)
def test_extract_angiver_id(file_path: str, expected_output: str | None) -> None:
    assert _extract_angiver_id(file_path) == expected_output