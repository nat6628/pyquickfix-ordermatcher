import decimal

import quickfix as qf

__SOH__ = chr(1)
SOH = "|"


def get_field_value(field_map: qf.FieldMap, field: qf.FieldBase) -> str:
    field_map.getField(field)
    return field.getValue()


def gen_execID():
    """Generate execID
    TODO: snowflake-id"""
    execID = 0

    def _gen_execID():
        nonlocal execID
        execID += 1
        return str(execID).zfill(5)

    return _gen_execID


def get_float(value: decimal.Decimal) -> float:
    return float(round(value, 2))


def log_message(message: qf.Message) -> str:
    return message.toString().replace(chr(1), SOH)
