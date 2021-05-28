"""MSSQL target class."""
from pathlib import Path
from typing import List
from .singer_sdk.target import Target
from .streams import MSSQLStream
import pyodbc 

#STREAM_TYPES = [
#  MSSQLStream,
#]  
class TargetMSSQL(Target):
  """MSSQL tap class."""
  name = "target-mssql"

  def __init__(self, config, *args, **kwargs):
    super().__init__(config, *args, **kwargs)
    assert self.config["host"]
    driver = self.config.get("driver", "{ODBC Driver 17 for SQL Server}")
    server = self.config["host"]+ "," + str(self.config.get("port", 1433)) 
    if (self.config.get("trusted_connection")=="yes"):
        self.conn = pyodbc.connect( driver=driver,
                                    server=server,
                                    trusted_connection=self.config.get("trusted_connection"),
                                    database=self.config.get("database"),
                                    )
    else:
        self.conn = pyodbc.connect( driver=driver,
                                    server=server,
                                    uid=self.config.get("user"),
                                    pwd=self.config.get("password"),
                                    database=self.config.get("database"),
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
