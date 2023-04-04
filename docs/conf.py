"""Sphinx configuration."""
project = "SSB Altinn Python"
author = "Øyvind Bruer-Skarsbø"
copyright = "2023, Øyvind Bruer-Skarsbø"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
