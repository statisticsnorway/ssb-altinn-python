import pandas as pd
import pytest

from altinn.flatten import _reorder_dataframe_columns


class TestReorderDataframeColumns:
    """Test suite for _reorder_dataframe_columns function."""

    @pytest.fixture
    def sample_df_without_level(self) -> pd.DataFrame:
        """Create a sample DataFrame without LEVEL column."""
        return pd.DataFrame(
            {
                "SKJEMA_ID": [1, 2, 3],
                "DELREG_NR": [10, 20, 30],
                "IDENT_NR": [100, 200, 300],
                "ENHETS_TYPE": ["A", "B", "C"],
                "FELTNAVN": ["field1", "field2", "field3"],
                "FELTVERDI": ["value1", "value2", "value3"],
                "VERSION_NR": [1, 2, 3],
            }
        )

    @pytest.fixture
    def sample_df_with_level(self) -> pd.DataFrame:
        """Create a sample DataFrame with LEVEL column."""
        return pd.DataFrame(
            {
                "SKJEMA_ID": [1, 2, 3],
                "DELREG_NR": [10, 20, 30],
                "IDENT_NR": [100, 200, 300],
                "ENHETS_TYPE": ["A", "B", "C"],
                "FELTNAVN": ["field1", "field2", "field3"],
                "FELTVERDI": ["value1", "value2", "value3"],
                "VERSION_NR": [1, 2, 3],
                "LEVEL": [1, 2, 3],
            }
        )

    @pytest.fixture
    def sample_df_unordered(self) -> pd.DataFrame:
        """Create a DataFrame with columns in random order."""
        return pd.DataFrame(
            {
                "FELTVERDI": ["value1", "value2", "value3"],
                "SKJEMA_ID": [1, 2, 3],
                "LEVEL": [1, 2, 3],
                "ENHETS_TYPE": ["A", "B", "C"],
                "DELREG_NR": [10, 20, 30],
                "FELTNAVN": ["field1", "field2", "field3"],
                "IDENT_NR": [100, 200, 300],
                "VERSION_NR": [1, 2, 3],
            }
        )

    def test_reorder_without_level(self, sample_df_without_level: pd.DataFrame) -> None:
        """Test reordering DataFrame without LEVEL column."""
        result: pd.DataFrame = _reorder_dataframe_columns(
            sample_df_without_level, include_level=False
        )

        expected_columns: list[str] = [
            "SKJEMA_ID",
            "DELREG_NR",
            "IDENT_NR",
            "ENHETS_TYPE",
            "FELTNAVN",
            "FELTVERDI",
            "VERSION_NR",
        ]

        assert list(result.columns) == expected_columns
        assert len(result) == 3
        pd.testing.assert_frame_equal(result, sample_df_without_level[expected_columns])

    def test_reorder_with_level(self, sample_df_with_level: pd.DataFrame) -> None:
        """Test reordering DataFrame with LEVEL column."""
        result: pd.DataFrame = _reorder_dataframe_columns(
            sample_df_with_level, include_level=True
        )

        expected_columns: list[str] = [
            "SKJEMA_ID",
            "DELREG_NR",
            "IDENT_NR",
            "ENHETS_TYPE",
            "FELTNAVN",
            "FELTVERDI",
            "VERSION_NR",
            "LEVEL",
        ]

        assert list(result.columns) == expected_columns
        assert len(result) == 3
        pd.testing.assert_frame_equal(result, sample_df_with_level[expected_columns])

    def test_reorder_unordered_dataframe(
        self, sample_df_unordered: pd.DataFrame
    ) -> None:
        """Test that function correctly reorders columns regardless of input order."""
        result: pd.DataFrame = _reorder_dataframe_columns(
            sample_df_unordered, include_level=True
        )

        expected_columns: list[str] = [
            "SKJEMA_ID",
            "DELREG_NR",
            "IDENT_NR",
            "ENHETS_TYPE",
            "FELTNAVN",
            "FELTVERDI",
            "VERSION_NR",
            "LEVEL",
        ]

        assert list(result.columns) == expected_columns

    def test_default_parameter(self, sample_df_without_level: pd.DataFrame) -> None:
        """Test that include_level defaults to False."""
        result: pd.DataFrame = _reorder_dataframe_columns(sample_df_without_level)

        assert "LEVEL" not in result.columns
        assert len(result.columns) == 7

    def test_data_integrity(self, sample_df_with_level: pd.DataFrame) -> None:
        """Test that data values are preserved after reordering."""
        result: pd.DataFrame = _reorder_dataframe_columns(
            sample_df_with_level, include_level=True
        )

        # Check that all original data is preserved
        assert result["SKJEMA_ID"].tolist() == [1, 2, 3]
        assert result["DELREG_NR"].tolist() == [10, 20, 30]
        assert result["LEVEL"].tolist() == [1, 2, 3]

    def test_missing_column_raises_error(
        self, sample_df_without_level: pd.DataFrame
    ) -> None:
        """Test that KeyError is raised when required column is missing."""
        with pytest.raises(KeyError):
            _reorder_dataframe_columns(sample_df_without_level, include_level=True)

    def test_empty_dataframe(self) -> None:
        """Test function with empty DataFrame."""
        empty_df: pd.DataFrame = pd.DataFrame(
            {
                "SKJEMA_ID": [],
                "DELREG_NR": [],
                "IDENT_NR": [],
                "ENHETS_TYPE": [],
                "FELTNAVN": [],
                "FELTVERDI": [],
                "VERSION_NR": [],
            }
        )

        result: pd.DataFrame = _reorder_dataframe_columns(empty_df, include_level=False)

        assert len(result) == 0
        assert list(result.columns) == [
            "SKJEMA_ID",
            "DELREG_NR",
            "IDENT_NR",
            "ENHETS_TYPE",
            "FELTNAVN",
            "FELTVERDI",
            "VERSION_NR",
        ]

    def test_single_row_dataframe(self) -> None:
        """Test function with single row DataFrame."""
        single_row_df: pd.DataFrame = pd.DataFrame(
            {
                "SKJEMA_ID": [1],
                "DELREG_NR": [10],
                "IDENT_NR": [100],
                "ENHETS_TYPE": ["A"],
                "FELTNAVN": ["field1"],
                "FELTVERDI": ["value1"],
                "VERSION_NR": [1],
                "LEVEL": [1],
            }
        )

        result: pd.DataFrame = _reorder_dataframe_columns(
            single_row_df, include_level=True
        )

        assert len(result) == 1
        assert result["SKJEMA_ID"].iloc[0] == 1
