"""MSSQL target class."""
from pathlib import Path
from typing import List
from singer_sdk_target import TargetBase
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

class TargetMSSQL(TargetBase):
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

# CLI Execution:

cli = TargetMSSQL.cli
