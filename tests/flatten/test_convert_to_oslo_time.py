import pytest
from altinn.flatten import _convert_to_oslo_time


def test_convert_to_oslo_time() -> None:
    input_time = "2023-05-01T12:00:00Z"
    expected_result = "2023-05-01T14:00:00+02:00"
    
    result_time = _convert_to_oslo_time(input_time)
    
    assert result_time == expected_result