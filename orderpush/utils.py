import csv
from datetime import datetime
from typing import List

import quickfix as qf
import yaml
from snowflake import SnowflakeGenerator

from .models import Symbol

_CUSTOM_SNOWFLAKE_EPOCH = 1420070400000  # Custom Epch (January 1, 2015 Midnight UTC = 2015-01-01T00:00:00Z)


class Configs:
    snowflake_node_id: int
    timeout: int
    delay: float

    account_id: int
    rate_percentage: int

    fix_config: str
    symbol_list: str
    log_path: str

    def __init__(self, config_path: str):
        with open(config_path) as f:
            data: dict = yaml.safe_load(f)

        for name, value in data.items():
            setattr(self, name, self._wrap(value))

    def _wrap(self, value):
        if isinstance(value, (tuple, list, set, frozenset)):
            return type(value)([self._wrap(v) for v in value])
        else:
            return value


def generate_id(node_id: int):
    snowflake = SnowflakeGenerator(node_id, epoch=_CUSTOM_SNOWFLAKE_EPOCH)

    def _gen_id():
        return next(snowflake)

    return _gen_id


def now() -> str:
    return datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


def get_field_value(field_map: qf.FieldMap, field: qf.FieldBase) -> str:
    field_map.getField(field)
    return field.getValue()


def get_symbol_list(path: str) -> List[Symbol]:
    with open(path) as f:
        reader = csv.DictReader(f)
        return [Symbol(**row) for row in reader]
