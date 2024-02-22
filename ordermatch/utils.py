import decimal

import quickfix as qf
from snowflake import SnowflakeGenerator

CUSTOM_SNOWFLAKE_EPOCH = 1420070400000  # Custom Epoch (January 1, 2015 Midnight UTC = 2015-01-01T00:00:00Z)
__SOH__ = chr(1)
SOH = "|"


def get_field_value(field_map: qf.FieldMap, field: qf.FieldBase) -> str:
    field_map.getField(field)
    return field.getValue()


def gen_execID(node_id: int):
    """Generate execID"""
    snowflake = SnowflakeGenerator(node_id, epoch=CUSTOM_SNOWFLAKE_EPOCH)

    def _gen_execID():
        return str(next(snowflake))

    return _gen_execID


def get_float(value: decimal.Decimal) -> float:
    return float(round(value, 2))


def log_message(message: qf.Message) -> str:
    return message.toString().replace(chr(1), SOH)
