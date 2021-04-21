"""MSSQL target class."""
from pathlib import Path
from typing import List
from singer_sdk.target import Target
from .streams import MSSQLStream
class TargetMSSQL(Target):
  """MSSQL tap class."""
  STREAM_TYPES = [
    MSSQLStream,
  ]   
    
  name = "target-mssql"

  # TODO: Update this section with the actual config values you expect:
  #config_jsonschema = PropertiesList(
  #    Property("host", StringType, required=True),
  #    Property("user", StringType, required=True),
  #    Property("port", IntegerType, required=False, default=1521),
  #    Property("password", StringType, required=True),
  #    Property("database", StringType, required=True),
  #).to_dict()
  def streams(self):
    return [stream_class(target=self) for stream_class in STREAM_TYPES]
    
# CLI Execution:

cli = TargetMSSQL.cli()
