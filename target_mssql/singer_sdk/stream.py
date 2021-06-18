#Borrowed from https://gitlab.com/DouweM/target-lunch-money/-/blob/master/target_lunch_money/singer_sdk/stream.py
from jsonschema.validators import Draft4Validator
import decimal 
import singer
logger = singer.get_logger()

class Stream:
    #TODO as of right now without a schema things will fail
    def __init__(self, target, name=None, schema=None, key_properties=None):
        self.target = target
        decimal.getcontext().prec = 50 #TODO is this the right amount of precision? 
        if name:
            self.name = name
        if schema:
            self.schema = schema
            self.validator = Draft4Validator(self.schema)
        if key_properties:
            self.key_properties = key_properties
    
    def process_record(self, record):
        self.validator.validate(record)
        self.persist_record(record)

    def persist_record(self, record):
        raise NotImplementedError
    
    def clean_up(self):
        raise NotImplementedError

