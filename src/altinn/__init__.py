"""SSB Altinn Python."""

from .file import FileInfo
from .flatten import isee_transform
from .parser import ParseSingleXml

__all__ = ["FileInfo", "ParseSingleXml", "isee_transform"]
