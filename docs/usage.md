# Usage

```{eval-rst}
.. click:: altinn.__main__:main
    :prog: ssb-altinn-python
    :nested: full
```

```python
from altinn import FileInfo, ParseSingleXml

file = "gs://ssb-prod-dapla-felles-data-delt/altinn3/form_dc551844cd74.xml"

# Create an instance of FileInfo
form = FileInfo(file)

# Create an instance of ParseSingleXml
form_content = ParseSingleXml(file)

# Get file filename without '.xml'-postfix
form.filename()
# Returns: 'form_dc551844cd74'

# Print a nicely formatted version of the file
form.pretty_print()

# Print an unformatted version of the file. Does not require the file to be parseable by an xml-library. Useful for inspecting unvalid xml-files.
form.print()

# Check if xml-file is valid. Useful to inspect xml-files with formal errors in the xml-schema.
form.validate()
# Returns True og False

# Get a dictionary representation of the contents of the file
form_content.to_dict()

# Get a Pandas Dataframe representation of the contents of the file
form_content.to_dataframe()
```
