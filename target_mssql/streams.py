"""Stream class for target-mssql."""
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk.stream import Stream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class MSSQLStream(Stream):
  """Stream class for MSSQL streams."""
  def persist_record(self, record):
    print(f"would have persisted: {record}")
    #print(f"name: {self.name} , key_properties: {self.key_properties}, schema: {self.schema}")
