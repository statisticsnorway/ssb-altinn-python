import pandas as pd

from altinn.flatten import _transform_checkbox_var


def test_transform_checkbox_var_basic() -> None:
    df = pd.DataFrame(
        {
            "FELTNAVN": ["checkbox_var", "other_var"],
            "FELTVERDI": ["option1,option2", "value"],
        }
    )
    result_df = _transform_checkbox_var(df, "checkbox_var", unique_code=False)

    assert "checkbox_varoption1" in result_df["FELTNAVN"].values
    assert "checkbox_varoption2" in result_df["FELTNAVN"].values
    assert all(
        value == "1"
        for value in result_df[result_df["FELTNAVN"].str.contains("checkbox_var")][
            "FELTVERDI"
        ]
    )


def test_transform_checkbox_var_unique_code() -> None:
    df = pd.DataFrame(
        {
            "FELTNAVN": ["checkbox_var", "other_var"],
            "FELTVERDI": ["option1,option2", "value"],
        }
    )
    result_df = _transform_checkbox_var(df, "checkbox_var", unique_code=True)

    assert "option1" in result_df["FELTNAVN"].values
    assert "option2" in result_df["FELTNAVN"].values
    assert all(
        value == "1"
        for value in result_df[result_df["FELTNAVN"].str.startswith("option")][
            "FELTVERDI"
        ]
    )


def test_transform_checkbox_var_no_entry() -> None:
    df = pd.DataFrame({"FELTNAVN": ["other_var"], "FELTVERDI": ["value"]})
    result_df = _transform_checkbox_var(df, "checkbox_var", unique_code=True)

    assert df.equals(result_df)  # Should be unchanged as 'checkbox_var' does not exist
