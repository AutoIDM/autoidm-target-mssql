#Borrowed from https://gitlab.com/DouweM/target-lunch-money/-/blob/master/target_lunch_money/singer_sdk/stream.py
from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()


class Stream:
    def __init__(self, target, name=None, schema=None, key_properties=None):
        self.target = target
        if name:
            self.name = name
        if schema:
            self.schema = schema
        if key_properties:
            self.key_properties = key_properties

    def process_record(self, record):
        self.validator.validate(record)
        #TODO Remove println
        print(record)
        self.persist_record(record)

    def persist_record(self, record):
        raise NotImplementedError


class MappingStream(Stream):
    schema = {}

    def __init__(self, target):
        super().__init__(target=target)

        self.mapping = self.target.stream_mapping(self.name)

        self.name_in = self.mapping.get("in", self.name)

        self.validator = Draft4Validator(self.schema)

        self.schema_in = None
        self.key_properties_in = None

        self._validator_in = None

    @property
    def validator_in(self):
        if not self.schema_in:
            return None

        if self._validator_in is None:
            self._validator_in = Draft4Validator(self.schema_in)

        return self._validator_in

    def process_record(self, record):
        if self.validator_in:
            self.validator_in.validate(record)

        if self.mapping:
            record = self.map_record(record)

        return super().process_record(record)

    def map_record(self, record):
        try:
            properties = self.mapping["properties"]
        except KeyError:
            return record

        return {prop: record.get(prop_in) for prop, prop_in in properties.items()}

