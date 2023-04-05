# SSB Altinn Python

[![PyPI](https://img.shields.io/pypi/v/ssb-altinn-python.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/ssb-altinn-python.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-altinn-python)][python version]
[![License](https://img.shields.io/pypi/l/ssb-altinn-python)][license]

[![Read the documentation at https://ssb-altinn-python.readthedocs.io/](https://img.shields.io/readthedocs/ssb-altinn-python/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/skars82/ssb-altinn-python/workflows/Tests/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/skars82/ssb-altinn-python/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/ssb-altinn-python/
[status]: https://pypi.org/project/ssb-altinn-python/
[python version]: https://pypi.org/project/ssb-altinn-python
[read the docs]: https://ssb-altinn-python.readthedocs.io/
[tests]: https://github.com/skars82/ssb-altinn-python/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/skars82/ssb-altinn-python
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

## Features

This is work-in-progress Python-package for dealing with xml-data from Altinn3.

```python
from altinn import __main__

x = __main__.XmlFile(
    "gs://ssb-prod-dapla-felles-data-delt/altinn3/form_dc551844cd74.xml"
)

x.filename()

```

## Requirements

- TODO

## Installation

You can install _SSB Altinn Python_ via [pip] from [PyPI]:

```console
pip install ssb-altinn-python
```

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
