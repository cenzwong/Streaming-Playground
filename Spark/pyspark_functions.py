from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F


def when_mapping(column, _dict: dict) -> Column:
    base_output = F
    for cond, val in _dict.items():
        base_output = base_output.when(column == cond, val)

    return base_output


def _when_mapping(column: Column, _dict: dict) -> Column:
    return reduce(
        lambda base_output, cond_val: base_output.when(
            column == cond_val[0], cond_val[1]
        ),
        _dict.items(),
        F,
    )
