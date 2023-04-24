"""Utilities for working with Altinn-data in Python."""

import os
from typing import Optional


def is_dapla() -> bool:
    """Check whether the current environment is running a Dapla JupyterLab instance.

    Returns:
        bool: True if the current environment is running a Dapla JupyterLab instance,
        False otherwise.
    """
    jupyter_image_spec: Optional[str] = os.environ.get("JUPYTER_IMAGE_SPEC")
    return bool(jupyter_image_spec and "dapla-jupyterlab" in jupyter_image_spec)
