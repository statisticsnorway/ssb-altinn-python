# SSB Altinn Python

[![PyPI](https://img.shields.io/pypi/v/ssb-altinn-python.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/ssb-altinn-python.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-altinn-python)][python version]
[![License](https://img.shields.io/pypi/l/ssb-altinn-python)][license]

[![Read the documentation at https://ssb-altinn-python.readthedocs.io/](https://img.shields.io/readthedocs/ssb-altinn-python/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/statisticsnorway/ssb-altinn-python/workflows/Tests/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/statisticsnorway/ssb-altinn-python/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/ssb-altinn-python/
[status]: https://pypi.org/project/ssb-altinn-python/
[python version]: https://pypi.org/project/ssb-altinn-python
[read the docs]: https://ssb-altinn-python.readthedocs.io/
[tests]: https://github.com/statisticsnorway/ssb-altinn-python/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/statisticsnorway/ssb-altinn-python
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

## Features

This is work-in-progress Python-package for dealing with xml-data from Altinn3. Here are some examples of how it can be used:

### Get information about a file

```python
from altinn import FileInfo

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/form_dc551844cd74.xml"

# Create an instance of FileInfo
form = FileInfo(file)

# Get file filename without '.xml'-postfix
form.filename()
# Returns: 'form_dc551844cd74'

# Print an unformatted version of the file. Does not require the file to be parseable by an xml-library. Useful for inspecting unvalid xml-files.
form.print()

# Print a nicely formatted version of the file
form.pretty_print()

# Check if xml-file is valid. Useful to inspect xml-files with formal errors in the xml-schema.
form.validate()
# Returns True og False
```

### Parse xml-file

If you want to transform an Altinn3 xml-file to a Pandas Dataframe, you can use the ParseSingleXml-class.

```python
from altinn import ParseSingleXml

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/form_dc551844cd74.xml"

form_content=ParseSingleXml(file)

# Get a Pandas Dataframe representation of the contents of the file
df=form_content.to_dataframe()

df.head()
```

### Tramsform to ISEE-Dynarev format

If you want to transform an Altinn3 xml-file to a Pandas Dataframe, in the same form as the ISEE Dynarev database in our on-prem environment, you can use the `isee_transform`-function.

```python
from altinn import isee_transform

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/form_dc551844cd74.xml"

isee_transform(file)
```

The resulting object is a Pandas Dataframe with the following columns:

- `Skjema_id`
- `Delreg_nr`
- `Ident_nr`
- `Enhets_type`
- `feltnavn`
- `feltverdi`
- `version_nr`

This dataframe can be written to csv and uploaded to the ISEE Dynarev database.

## Requirements

- dapla-toolbelt >=1.6.2,<2.0.0
- defusedxml >=0.7.1,<0.8.0
- pytest >=7.2.2,<8.0.0

## Installation

You can install _SSB Altinn Python_ via [poetry] from [PyPI]:

```console
poetry add ssb-altinn-python
```

To install this in the Jupyter-environment on Dapla, where it is ment to be used, it is required to install it in an virtual environment. It is recommended to do this in an [ssb-project](https://manual.dapla.ssb.no/jobbe-med-kode.html) where the preferred tool is [poetry](https://python-poetry.org/).

## Usage

Please see the [Command-line Reference] for details.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_SSB Altinn Python_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz
[pypi]: https://pypi.org/
[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python
[file an issue]: https://github.com/skars82/ssb-altinn-python/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/skars82/ssb-altinn-python/blob/main/LICENSE
[contributor guide]: https://github.com/skars82/ssb-altinn-python/blob/main/CONTRIBUTING.md
[command-line reference]: https://ssb-altinn-python.readthedocs.io/en/latest/usage.html
