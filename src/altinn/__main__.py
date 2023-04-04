"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """SSB Altinn Python."""


if __name__ == "__main__":
    main(prog_name="ssb-altinn-python")  # pragma: no cover
