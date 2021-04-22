"""Stream class for target-mssql."""
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk.stream import Stream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

#TODO: Opening Database conneciton on a per stream basis seems like a no-go
class MSSQLStream(Stream):
  """Stream class for MSSQL streams."""
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.table_handler()

  def table_handler(self):
    #TODO it is not safe to assume you can truncate a table in this situation
    
    #TODO How do we know all of the data is through and we are ready to drop and merge data into the table?
    
    ddl = self.schema_to_temp_table_ddl(self.schema)
    self.sql_runner(ddl)
    print("would have created table")
  
  #TODO error handling. If there's not a key_propertie what kind of failure do we want?
  def schema_to_temp_table_ddl(self, schema) -> str:
    #TODO Can't assume this is an INT always
    primay_key= self.key_properties[0]
    table_name = self.name
    columns_types = {}

    #TODO Need be using named parameters for SQL to avoid potential injection, and to be clean
    sql = f"CREATE TABLE {table_name}_temp ("
    #TODO can you assume only 1 primary key?
    pk_type=self.json_to_mssqlmapping(self.schema["properties"]["id"])
    sql += f"{primay_key} {pk_type} NOT NULL PRIMARY KEY"
    properties=self.schema["properties"]
    print(properties)
    json_to_column_type={}
    for name, shape in properties.items():
      mssqltype=self.json_to_mssqlmapping(shape)
      sql+= f", {name} {mssqltype}"
      #print(self.json_to_mssqlmapping(shape))
    
    #CREATE TABLE {stream_name}_temp (
    #  ID int NOT NULL PRIMARY KEY,
    #  displayName varchar(max),
    #  }
    sql += ");"
    return sql
  
   #TODO clean up / make methods like this static
   #TODO what happens with multiple types
   #TODO what happens if the string type I want isn't first
  def json_to_mssqlmapping(self, shape:dict) -> str:
    jsontype : str = shape["type"][0]
    mssqltype : str = None
    if (jsontype=="string"): mssqltype = "VARCHAR(MAX)"
    elif (jsontype=="number"): mssqltype = "INT" #TODO is int always the right choice?
    elif (jsontype=="boolean"): mssqltype = "BOOL"
     #not tested
    elif (jsontype=="null"): raise NotImplementedError
    elif (jsontype=="array"): raise NotImplementedError
    elif (jsontype=="object"): raise NotImplementedError
    else: raise NotImplementedError
     
    return mssqltype
  
  def data_json_to_mssqlmapping(self, data) -> str:
    if(type(data) == str): returnvalue = f"\"{data}\""
    #Could have imported NoneType instead but meh
    elif(data is None): returnvalue = "NULL"
    #TODO clean this up a bit, expressions in python?
    elif(type(data) == bool): 
      if(data): returnvalue = "1" 
      else: returnvalue = "0"
    elif(type(data) == int): returnvalue = f"{data}"
    else: raise NotImplementedError(f"Data Type: {data}")
    return returnvalue 
     
  #TODO when this is batched how do you make sure the column ordering stays the same (data class probs)
  #Columns is seperate due to data not necessairly having all of the correct columns
  def record_to_dml(self, table_name:str, data:dict) -> str:
    column_list=",".join(data.keys())
    sql = f"INSERT INTO {table_name} ({column_list})"
    #TODO can make this more pythonic using lambda, list comprehension, or some collections schtuff
    canonical_data = []
    for rec in data.values():
      canonical_data.append(self.data_json_to_mssqlmapping(rec))
    datalist = ",".join(canonical_data)
    sql += f"VALUES ({datalist})" 
    return sql

  def sql_runner(self, sql):
    print(f"Would have ran: {sql}")

  #def tempdb_to_actualdb_sql(self, temp_db_name, actual_db_name)
  #def tempdb_drop_sql(self, tempdb_name)
  #def start_transaction(self) -> str: 
  #def complete_transaction(self)

  def persist_record(self, record):
    #print(f"would have persisted: {record}")
    #print(f"name: {self.name} , key_properties: {self.key_properties}, schema: {self.schema}")
    #TODO shouldn't manually generate the table name here
    dml = self.record_to_dml(table_name=f"{self.name}_temp",data=record)    
    #print(dml)
    self.sql_runner(dml)
  
  #def flush_stream(self)
  #  sql = tempdb_to_actualdb_sql(temp_db_name, actual_db_name)
  #  sql_runner(sql)
  #  drop_tempdb = tempdb_drop_sql(temp_db_name)
  #  sql_runner(drop_tempdb)
  #  sql_runner(complete_transaction())
