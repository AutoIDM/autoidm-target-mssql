"""Stream class for target-mssql."""
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk.stream import Stream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class MSSQLStream(Stream):
    """Stream class for MSSQL streams."""

