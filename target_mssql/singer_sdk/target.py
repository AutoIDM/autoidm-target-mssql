#Borrowed from https://gitlab.com/DouweM/target-lunch-money/-/blob/master/target_lunch_money/singer_sdk/target.py
import io

import click
import simplejson as json
import sys

import singer

logger = singer.get_logger()


class Target:
    def __init__(self, config):
        if isinstance(config, dict):
            self.config = config
        elif isinstance(config, str):
            with open(config) as input_json:
                self.config = json.load(input_json)
        else:
            self.config = {}

        self.state = None

        self.streams_in = {}

    def get_stream(self, name, schema, key_properties):
        #try:
        #    stream = next(stream for stream in self.streams() if stream.name == name)
        #except StopIteration:
        #    raise Exception("Unsupported stream {}".format(name))
        #TODO: This is a really silly way to do this
        #except TypeError:
        #TODO Deal with Existing streams at some point
        stream = self.streamclass(target=self, name=name, schema=schema, key_properties=key_properties)
        stream.schema_in = schema
        stream.key_properties_in = key_properties
        return stream
        #return self.stream_class(
        #    target=self, name=name, schema=schema, key_properties=key_properties
        #)

    def process_schema_message(self, message):
        stream = message["stream"]
        self.streams_in[stream] = self.get_stream(
            stream, message["schema"], message["key_properties"]
        )

    def process_record_message(self, message):
        stream_name = message["stream"]
        try:
            stream = self.streams_in[stream_name]
        except KeyError:
            raise Exception(
                "A record for stream {}"
                "was encountered before a corresponding schema".format(stream_name)
            )

        stream.process_record(message["record"])
        self.state = None

    def process_state_message(self, message):
        self.state = message["value"]
        logger.debug("Set state to {}".format(self.state))

    def process_messages(self, messages):
        message_handlers = {
            "SCHEMA": self.process_schema_message,
            "RECORD": self.process_record_message,
            "STATE": self.process_state_message,
        }

        for raw_message in messages:
            try:
                message = singer.parse_message(raw_message).asdict()
            except json.decoder.JSONDecodeError:
                logger.error("Unable to parse:\n{}".format(raw_message))
                raise

            message_type = message["type"]

            try:
                handler = message_handlers[message_type]
            except KeyError:
                logger.warning(
                    "Unknown message type {} in message {}".format(
                        message_type, message
                    )
                )
                continue

            handler(message)

        self.emit_state()

    def emit_state(self):
        if self.state is None:
            return

        line = json.dumps(self.state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()
    
    def clean_up(self):
        print(self.streams_in) 
        for streamname, stream in self.streams_in.items():
            stream.clean_up()

    @classmethod
    def cli(cls):
        @click.option("--config")
        @click.command()
        def cli(config: str = None):
            target = cls(config=config)

            input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
            target.process_messages(input_messages)
            target.clean_up()
            logger.debug("Exiting normally")

        return cli

#TODO Get rid of this class as it's not used
class MappingTarget(Target):
    def stream_mapping(self, name):
        return self.config.get("mapping", {}).get(name, {})

    def get_stream(self, name, schema, key_properties):
        try:
            stream = next(stream for stream in self.streams() if stream.name_in == name)
        except StopIteration:
            raise Exception("Unsupported stream {}".format(name))

        stream.schema_in = schema
        stream.key_properties_in = key_properties

        return stream

