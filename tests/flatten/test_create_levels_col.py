from typing import Any

import pytest

from altinn.flatten import _create_levels_col


@pytest.mark.parametrize(
    "row, expected_level",
    [
        ({"COUNTER": [1, 2, 3]}, 2),  # More than one element in the list
        ({"COUNTER": [1]}, 1),  # Exactly one element in the list
        ({"COUNTER": []}, 0),  # An empty list
        ({"COUNTER": "not a list"}, 0),  # Not a list type
        ({}, 0),  # 'COUNTER' key missing
    ],
)
def test_create_levels_col(row: dict[str, Any], expected_level: int) -> None:
    assert _create_levels_col(row) == expected_level
