"""Stream class for target-mssql."""
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from .singer_sdk.stream import Stream
import logging
from decimal import Decimal
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

#TODO: Use logging 
#TODO: Opening Database conneciton on a per stream basis seems like a no-go
class MSSQLStream(Stream):
  
  """Stream class for MSSQL streams."""
  def __init__(self, conn, schema_name, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.conn = conn
    #TODO: Turn off autocommit and deal with batching
    self.conn.autocommit=True
    #TODO Think about the right way to handle this when restructuring classes re https://pymssql.readthedocs.io/en/stable/pymssql_examples.html#important-note-about-cursors
    self.cursor = self.conn.cursor() 
    self.full_table_name = self.generate_full_table_name(self.name, schema_name)
    self.temp_full_table_name = self.generate_full_table_name(f"{self.name}_temp", schema_name)
    self.table_handler()

  def generate_full_table_name(self, streamname, schema_name):
    table_name = streamname
    table_name = streamname.replace("-","_")
    if schema_name: table_name = schema_name + "." + table_name
    return f"[{table_name}]"

  #TODO this method seems needless, should probably just call methods directly
  def table_handler(self):
    #TODO it is not safe to assume you can truncate a table in this situation
    
    #TODO How do we know all of the data is through and we are ready to drop and merge data into the table?
    
    ddl = self.schema_to_temp_table_ddl(self.schema, self.temp_full_table_name)
    self.sql_runner(ddl)
  
  def schema_to_temp_table_ddl(self, schema, table_name) -> str:
    primary_key=None
    try:
      if(self.key_properties[0]):
          primary_key = self.key_properties[0]
    except AttributeError:
      primary_key=None

    properties=self.schema["properties"]
    
    #TODO better system for detecting tables
    
    #TODO Need be using named parameters for SQL to avoid potential injection, and to be clean
    sql = f"DROP TABLE IF EXISTS {table_name} CREATE TABLE {table_name}("
   
    #Key Properties
    #TODO can you assume only 1 primary key?
    if (primary_key):
      pk_type=self.ddl_json_to_mssqlmapping(self.schema["properties"][primary_key])
      #TODO Can't assume this is an INT always
      #TODO 450 is silly
      pk_type=pk_type.replace("MAX","450") #TODO hacky hacky
      sql += f"[{primary_key}] {pk_type} NOT NULL PRIMARY KEY,"
      properties.pop(primary_key, None) #Don't add the primary key to our DDL again

    
    #Loop through all properties of stream to add them to our DDL
    first=True
    for name, shape in properties.items():
      mssqltype=self.ddl_json_to_mssqlmapping(shape)
      if (mssqltype is None): continue #Empty Schemas
      mssqltype=self.ddl_json_to_mssqlmapping(shape)
      if(first): 
        sql+= f" [{name}] {mssqltype}"
        first=False
      else: 
        sql+= f", [{name}] {mssqltype}"

    
    sql += ");"
    return sql
  
   #TODO clean up / make methods like this static
   #TODO what happens with multiple types
   #TODO what happens if the string type I want isn't first
  def ddl_json_to_mssqlmapping(self, shape:dict) -> str:
    #TODO this is not solid, need to harden
    #if (type(shape["type"]) == str): jsontype = shape["type"]
    #TODO need to prioritize which type first
    #else: 
    if ("type" not in shape): return None 
    jsontype = shape["type"]
    #  jsontype.sort()
    #  jsontype.reverse()
    #  jsontype=jsontype[0]
    #  print(jsontype)
    mssqltype : str = None
    if ("string" in jsontype): mssqltype = "VARCHAR(MAX)"
    elif ("number" in jsontype): mssqltype = "BIGINT" #TODO is int always the right choice?
    elif ("integer" in jsontype): mssqltype = "BIGINT" #TODO is int always the right choice?
    elif ("boolean" in jsontype): mssqltype = "BIT"
     #not tested
    elif ("null" in jsontype): raise NotImplementedError("Can't set columns as null in MSSQL")
    elif ("array" in jsontype): raise NotImplementedError("Currently haven't implemented dealing with arrays")
    elif ("object" in jsontype): raise NotImplementedError("Currently haven't implemented dealing with objects")
    else: raise NotImplementedError(f"Haven't implemented dealing with this type of data. jsontype: {jsontype}") 
     
    return mssqltype

  def convert_data_to_params(self, datalist) -> list:
      parameters = []
      for noop in datalist:
          parameters.append("?")
      return parameters 
     
  #TODO when this is batched how do you make sure the column ordering stays the same (data class probs)
  #Columns is seperate due to data not necessairly having all of the correct columns
  def record_to_dml(self, table_name:str, data:dict) -> str:
    #TODO this is a bit gross, could refactor to make this easier to read
    column_list="],[".join(data.keys())
    sql = f"INSERT INTO {table_name} ([{column_list}])"

    paramaters = self.convert_data_to_params(data.values())
    sqlparameters = ",".join(paramaters)
    sql += f" VALUES ({sqlparameters})"
    return sql

  def sql_runner(self, sql):
    try:
      print(f"Running SQL: {sql}")
      self.cursor.execute(sql)
    except Exception as e:
        logging.error(f"Caught exception whie running sql: {sql}")
        raise e

  def sql_runner_withparams(self, sql, paramaters):
    try:
      print(f"Running SQL: {sql} . Parameters: {paramaters}")
      self.cursor.execute(sql, paramaters)
    except Exception as e:
        logging.error(f"Caught exception whie running sql: {sql} . Parameters: {paramaters}")
        raise e

  def persist_record(self, record):
    dml = self.record_to_dml(table_name=self.temp_full_table_name,data=record)    
    self.sql_runner_withparams(dml, tuple(record.values()))

  def clean_up(self):
      #We are good to go, drop table if it exists
      sql = f"DROP TABLE IF EXISTS {self.full_table_name}"
      self.sql_runner(sql)
      #Rename our temp table to the correct table
      sql = f"SELECT * INTO {self.full_table_name} from {self.temp_full_table_name}"
      self.sql_runner(sql)
      #Remove temp table
      sql = f"DROP TABLE IF EXISTS {self.temp_full_table_name}"
      self.sql_runner(sql)
  #def flush_stream(self)
  #  sql = tempdb_to_actualdb_sql(temp_db_name, actual_db_name)
  #  sql_runner(sql)
  #  drop_tempdb = tempdb_drop_sql(temp_db_name)
  #  sql_runner(drop_tempdb)
  #  sql_runner(complete_transaction())
