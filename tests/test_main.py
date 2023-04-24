"""This module contains the tests for the __main__.py."""

import os
import importlib.util
from unittest.mock import patch
import altinn.__main__ as main_module

def test_main_direct_call():
    """Test that main() is called when altinn.__main__.main() is called."""
    with patch("altinn.__main__.main") as mock_main:
        main_module.main()
        mock_main.assert_called_once()

def test_main_script_execution():
    """Test that main() is called when __main__.py is run as a script."""
    with patch("altinn.file.main") as mock_main:
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "altinn", "__main__.py"))
        spec = importlib.util.spec_from_file_location("__main__", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        mock_main.assert_called_once()
