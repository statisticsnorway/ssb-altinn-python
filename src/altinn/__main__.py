"""Main entry point for the altinn package."""

from altinn.file import main as file_main
from altinn.parser import main as parser_main

if __name__ == "__main__":
    file_main()
    parser_main()
