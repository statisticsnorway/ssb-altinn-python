import pandas as pd
import pytest

from altinn.flatten import _add_lopenr


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "FELTNAVN": ["field1", "field2", "field3", "field4"],
            "COUNTER": [["001"], ["002"], ["003", "004"], ["005"]],
            "LEVELS": [0, 1, 2, 0],
        }
    )


def test_add_lopenr(sample_df: pd.DataFrame) -> None:
    result_df = _add_lopenr(sample_df)

    # Check if suffix is correctly added
    assert result_df.at[1, "FELTNAVN"] == "field2_002"
    assert (
        result_df.at[2, "FELTNAVN"] == "field3_004"
    )  # Assumes using last item from 'COUNTER'

    # Check if rows with LEVELS 0 remain unchanged
    assert "field1" == result_df.at[0, "FELTNAVN"]
    assert "field4" == result_df.at[3, "FELTNAVN"]

    # Verify COUNTER and LEVELS columns are removed
    assert "COUNTER" not in result_df.columns
    assert "LEVELS" not in result_df.columns
