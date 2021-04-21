"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import pytest
#from singer_sdk.testing import get_standard_tap_tests
from target_mssql.target import TargetMSSQL

#Config from Docker File Setup
SAMPLE_CONFIG = {
  "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
  "host": "localhost",
  "port": 1521,
  "database": "temp_db",
  "user": "sa",
  "password": "SAsa12345678!"
}


# Run standard built-in tap tests from the SDK:
# def test_standard_tap_tests():
#    """Run standard tap tests from the SDK."""
#    tests = get_standard_tap_tests(
#        TargetMSSQL,
#        config=SAMPLE_CONFIG
#    )
#    for test in tests:
#        test()

# TODO: State of MSSQL database does need to be handled before going too far here
def testdata_to_mssql(source_data):
  target = TargetMSSQL(config=SAMPLE_CONFIG)
  target.process_messages(source_data); #test data

@pytest.fixture
def source_data():
  data  = """\
{"type": "SCHEMA", "stream": "employees", "schema": {"type": "object", "properties": {"id": {"type": ["string", "null"]}, "displayName": {"type": ["string", "null"]}, "firstName": {"type": ["string", "null"]}, "lastName": {"type": ["string", "null"]}, "gender": {"type": ["string", "null"]}, "jobTitle": {"type": ["string", "null"]}, "workPhone": {"type": ["string", "null"]}, "workPhoneExtension": {"type": ["string", "null"]}, "skypeUsername": {"type": ["string", "null"]}, "preferredName": {"type": ["string", "null"]}, "mobilePhone": {"type": ["string", "null"]}, "workEmail": {"type": ["string", "null"]}, "department": {"type": ["string", "null"]}, "location": {"type": ["string", "null"]}, "division": {"type": ["string", "null"]}, "linkedIn": {"type": ["string", "null"]}, "photoUploaded": {"type": ["boolean", "null"]}, "photoUrl": {"type": ["string", "null"]}, "canUploadPhoto": {"type": ["number", "null"]}}, "required": []}, "key_properties": ["id"]}
  {"type": "RECORD", "stream": "employees", "record": {"id": "119", "displayName": "Alexa Aberdean", "firstName": "Alexa", "lastName": "Aberdean", "preferredName": null, "jobTitle": "CNC Machinist", "workPhone": null, "mobilePhone": null, "workEmail": "aaberdean@autoidm.com", "department": "Customer Experience", "location": "Kalamazoo", "division": "abc", "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.878203Z"}
  {"type": "STATE", "value": {"bookmarks": {"employees": {}}}}
  {"type": "RECORD", "stream": "employees", "record": {"id": "120", "displayName": "Lisa Boyer", "firstName": "Lisa", "lastName": "Boyer", "preferredName": null, "jobTitle": "HR Generalist", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Human Resources", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.878919Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "117", "displayName": "Abby Lamar", "firstName": "Abby", "lastName": "Lamar", "preferredName": null, "jobTitle": "Customer Experience Champion", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Customer Experience", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879049Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "111", "displayName": "Patty Mae", "firstName": "Patty", "lastName": "Mae", "preferredName": null, "jobTitle": "Executive", "workPhone": null, "mobilePhone": null, "workEmail": "b@autoidm.com", "department": "Leadership", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879185Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "122", "displayName": "Mickey Mouse", "firstName": "Mickey", "lastName": "Mouse", "preferredName": null, "jobTitle": "CNC Programmer", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Engineering", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879338Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "115", "displayName": "Elon Musky", "firstName": "Elon", "lastName": "Musky", "preferredName": null, "jobTitle": "Process Engineer", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Engineering", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879454Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "116", "displayName": "Jeff Razer", "firstName": "Jeff", "lastName": "Razer", "preferredName": null, "jobTitle": "Customer Experience Champion", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Customer Experience", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879566Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "113", "displayName": "Tyler Sawyer", "firstName": "Tyler", "lastName": "Sawyer", "preferredName": null, "jobTitle": "Marketing Analyst", "workPhone": "5555555555", "mobilePhone": null, "workEmail": null, "department": "Sales/Marketing", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": "1", "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879670Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "110", "displayName": "Sam Smith", "firstName": "Sam", "lastName": "Smith", "preferredName": null, "jobTitle": "Sales Representative", "workPhone": null, "mobilePhone": "1234567891", "workEmail": "ssmith@autoidm.com", "department": "Sales/Marketing", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879805Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "114", "displayName": "Tim Teebow", "firstName": "Tim", "lastName": "Teebow", "preferredName": null, "jobTitle": "Network Engineer", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Engineering", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879902Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "121", "displayName": "Jake Vicks", "firstName": "Jake", "lastName": "Vicks", "preferredName": null, "jobTitle": "CNC Machinist", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Engineering", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.879997Z"}
  {"type": "STATE", "value": {"bookmarks": {"employees": {}}}}
  {"type": "RECORD", "stream": "employees", "record": {"id": "109", "displayName": "Derek Visch", "firstName": "Derek", "lastName": "Visch", "preferredName": null, "jobTitle": "Executive", "workPhone": null, "mobilePhone": null, "workEmail": "dvisch@autoidm.com", "department": "Leadership", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.880110Z"}
  {"type": "RECORD", "stream": "employees", "record": {"id": "118", "displayName": "Lexi Weatherton", "firstName": "Lexi", "lastName": "Weatherton", "preferredName": null, "jobTitle": "CNC Machinist", "workPhone": null, "mobilePhone": null, "workEmail": null, "department": "Manufacturing", "location": "Kalamazoo", "division": null, "linkedIn": null, "workPhoneExtension": null, "photoUploaded": false, "photoUrl": "https://resources.bamboohr.com/images/photo_person_150x150.png", "canUploadPhoto": 1}, "time_extracted": "2021-04-15T19:16:24.880206Z"}
  {"type": "STATE", "value": {"bookmarks": {"employees": {}}}}"""
  return data  
