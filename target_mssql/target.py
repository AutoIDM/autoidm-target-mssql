"""MSSQL target class."""
from pathlib import Path
from typing import List
from .singer_sdk.target import Target
from .streams import MSSQLStream
import pymssql

#STREAM_TYPES = [
#  MSSQLStream,
#]  
class TargetMSSQL(Target):
  """MSSQL tap class."""
  name = "target-mssql"

  def __init__(self, config, *args, **kwargs):
    super().__init__(config, *args, **kwargs)
    assert self.config["host"]
    self.conn = pymssql.connect(server=self.config["host"], 
                                user=self.config["user"],
                                password=self.config["password"],
                                database=self.config["database"],
                                port=self.config["port"])
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
    return self.streamslist 
    #return [stream_class(target=self) for stream_class in STREAM_TYPES]
  
  #TODO this is a silly way to do this
  def streamclass(self, *args, **kwargs):
    return MSSQLStream(conn=self.conn, *args, **kwargs)
# CLI Execution:

cli = TargetMSSQL.cli()
