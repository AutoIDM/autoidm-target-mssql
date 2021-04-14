"""MSSQL target class."""

from pathlib import Path
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

# TODO: Import your custom stream types here:
from target_mssql.streams import (
    MSSQLStream,
    UsersStream,
    GroupsStream,
)


# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    UsersStream,
    GroupsStream,
]

# TODO: Update from Tap to Target
class TargetMSSQL(Tap):
    """MSSQL tap class."""

    name = "target-mssql"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = PropertiesList(
        Property("host", StringType, required=True),
        Property("user", StringType, required=True),
        Property("port", IntegerType, required=False, default=1521),
        Property("password", StringType, required=True),
        Property("database", StringType, required=True),
    ).to_dict()


    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = TargetMSSQL.cli
