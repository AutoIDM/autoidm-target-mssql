"""MSSQL target class."""
from pathlib import Path
from typing import List
from singer_sdk.target import Target
from .streams import MSSQLStream
STREAM_TYPES = [
  MSSQLStream,
]  
streams = [] 
class TargetMSSQL(Target):
  """MSSQL tap class."""
    
  name = "target-mssql"
  # TODO: Update this section with the actual config values you expect:
  #config_jsonschema = PropertiesList(
  #    Property("host", StringType, required=True),
  #    Property("user", StringType, required=True),
  #    Property("port", IntegerType, required=False, default=1521),
  #    Property("password", StringType, required=True),
  #    Property("database", StringType, required=True),
  #).to_dict()

  #TODO not a fan of streams not being required by the BaseTarget class here, as it's referenced in the class
  def streams(self):
    return self.streams 
    #return [stream_class(target=self) for stream_class in STREAM_TYPES]
  
  #TODO this is a silly way to do this
  def streamclass(self, *args, **kwargs):
    return MSSQLStream(*args, **kwargs)
# CLI Execution:

cli = TargetMSSQL.cli()
