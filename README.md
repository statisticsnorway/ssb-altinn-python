# SSB Altinn Python

[![PyPI](https://img.shields.io/pypi/v/ssb-altinn-python.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/ssb-altinn-python.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-altinn-python)][pypi status]
[![License](https://img.shields.io/pypi/l/ssb-altinn-python)][license]

[![Documentation](https://github.com/statisticsnorway/ssb-altinn-python/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/ssb-altinn-python/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-altinn-python&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-altinn-python&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/ssb-altinn-python/
[documentation]: https://statisticsnorway.github.io/ssb-altinn-python
[tests]: https://github.com/statisticsnorway/ssb-altinn-python/actions?workflow=Tests

[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-altinn-python
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-altinn-python
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

## Features

This is work-in-progress Python-package for dealing with xml-data from Altinn3. Here are some examples of how it can be used:

### Get information about a file

```python
from altinn import FileInfo

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/RA-0595/2023/2/6/810409282_460784f978a2_ebc7af7e-4ebe-4883-b844-66ee6292a93a/form_460784f978a2.xml"

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

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/RA-0595/2023/2/6/810409282_460784f978a2_ebc7af7e-4ebe-4883-b844-66ee6292a93a/form_460784f978a2.xml"

form_content=ParseSingleXml(file)

# Get a Pandas Dataframe representation of the contents of the file
df=form_content.to_dataframe()

df.head()
```

### Transform to ISEE-Dynarev format

If you want to transform an Altinn3 xml-file to a Pandas Dataframe, in the same form as the ISEE Dynarev database in our on-prem environment, you can use the `isee_transform`-function.

```python
from altinn import isee_transform

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/RA-0595/2023/2/6/810409282_460784f978a2_ebc7af7e-4ebe-4883-b844-66ee6292a93a/form_460784f978a2.xml"

isee_transform(file)
```

If you want to recode/map names in the FELTNAVN-column, you can use a dictionary with the original names from the xml as keys, and the new names as values. And then pass the dictionary as an argument when running the function isee_transform(file, mapping).

```python
from altinn import isee_transform

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/RA-0595/2023/2/6/810409282_460784f978a2_ebc7af7e-4ebe-4883-b844-66ee6292a93a/form_460784f978a2.xml"

mapping = {'kontAmbulForeDispJaNei':'ISEE_VAR1',
           'kontAmbulForeDispAnt':'ISEE_VAR2',
           'kontAmbulForeDriftAnt':'ISEE_VAR3',}

isee_transform(file, mapping)
```
The function handles flat structures and 'tables' in the XML. If the XML contains repeating values, it puts a suffix containig a number at the end of the FELTNAVN-column. If the XML-contains more complex structures as 'table in table' if will give a warning with a list of which values in FELTNAVN that needs to be further processed before it can be used in ISEE.

The XML needs to contain certain fields in the 'InternInfo'-block, The required filds are:
- 'enhetsIdent'
- 'enhetsType'
- 'delregNr'

If one or more of these fields are missing in the XML, the processing will stop, giving a message with witch fields that are missing.


The resulting object is a Pandas Dataframe with the following columns:

- `SKJEMA_ID`
- `DELREG_NR`
- `IDENT_NR`
- `ENHETS_TYPE`
- `FELTNAVN`
- `FELTVERDI`
- `VERSION_NR`

This dataframe can be written to csv and uploaded to the ISEE Dynarev database.

## Requirements

- dapla-toolbelt >=1.6.2
- defusedxml >=0.7.1
- xmltodict >=0.13.0
- pandas >= 2.2.0

## Installation

You can install _SSB Altinn Python_ via [poetry] from [PyPI]:

```console
poetry add ssb-altinn-python
```

To install this in the Jupyter-environment on Dapla, where it is ment to be used, it is required to install it in an virtual environment. It is recommended to do this in an [ssb-project](https://manual.dapla.ssb.no/jobbe-med-kode.html) where the preferred tool is [poetry](https://python-poetry.org/).

## Usage

Please see the [Reference Guide] for details.

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

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/ssb-altinn-python/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/ssb-altinn-python/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/ssb-altinn-python/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/ssb-altinn-python/reference.html
