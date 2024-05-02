import pandas as pd

from altinn.flatten import _make_meta_df


def test_make_meta_df() -> None:
    meta_dict = {"name": "example", "altinntidspunktlevert": "2023-05-01T12:00:00Z"}
    expected_df = pd.DataFrame(
        [
            {"FELTNAVN": "NAME", "FELTVERDI": "example"},
            {
                "FELTNAVN": "ALTINNTIDSPUNKTLEVERT",
                "FELTVERDI": "2023-05-01T14:00:00+02:00",
            },
        ]
    )

    result_df = _make_meta_df(meta_dict)

    assert result_df.equals(expected_df), "The DataFrames do not match!"
