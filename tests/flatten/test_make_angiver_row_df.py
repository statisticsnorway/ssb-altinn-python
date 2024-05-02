from unittest.mock import patch

from altinn.flatten import _make_angiver_row_df


def test_make_angiver_row_df() -> None:
    # Mock data returned by the helper functions and XML parsing
    mock_file_path = "path/to/form_12345.xml"
    mock_xml_dict = {
        "RootElement": {
            "InternInfo": {
                "enhetsIdent": "12345",
                "delregNr": "67890",
                "enhetsType": "TypeA",
                "raNummer": "RA001",
            }
        }
    }
    expected_id = "12345"

    with patch(
        "altinn.flatten._read_single_xml_to_dict", return_value=mock_xml_dict
    ), patch("altinn.flatten._extract_angiver_id", return_value=expected_id):

        df = _make_angiver_row_df(mock_file_path)

        # Assertions to check DataFrame content
        assert df.loc[0, "FELTNAVN"] == "ANGIVER_ID"
        assert df.loc[0, "FELTVERDI"] == expected_id
