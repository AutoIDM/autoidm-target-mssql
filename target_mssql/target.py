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
    self.conn = pymssql.connect(host=self.config["host"], 
                                user=self.config.get("user"),
                                password=self.config.get("password"),
                                database=self.config.get("database"),
                                port=self.config.get("port", 1433) #If port is None, Windows Authentication will fail due to defaults in pymssql. https://github.com/pymssql/pymssql/blob/ff84c14157cf7b1cc528f68d72f07c2d0fb7d290/src/pymssql/_mssql.pyx#L613 
                                )

  #TODO not a fan of streams not being required by the BaseTarget class here, as it's referenced in the class
  def streams(self):
    return self.streamslist 
    #return [stream_class(target=self) for stream_class in STREAM_TYPES]
  
  #TODO this is a silly way to do this
  def streamclass(self, *args, **kwargs):
    schema=self.config.get("schema")
    return MSSQLStream(conn=self.conn, schema_name=schema, *args, **kwargs)
# CLI Execution:

cli = TargetMSSQL.cli()
