import pytest

from altinn.flatten import _extract_counter


@pytest.mark.parametrize(
    "input_string, expected_output",
    [
        ("£3$ £2$ £1$", ["3", "2", "1"]),
        ("£a$ £b$ £c$", ["a", "b", "c"]),
        ("", []),
        ("No counters here", []),
        ("£3$£2$£1$", ["3", "2", "1"]),
        ("£123$", ["123"]),
        ("£nested£3$$$", ["nested£3"]),
    ],
)
def test_extract_counter(input_string: str, expected_output: list[str]) -> None:
    assert _extract_counter(input_string) == expected_output
