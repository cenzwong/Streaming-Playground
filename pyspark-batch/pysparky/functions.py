from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F
from pysparky import decorator


@decorator.pyspark_column_or_name_enabler("column_or_name")
def when_mapping(column_or_name, dict_: dict) -> Column:
    base_output = F
    for cond, val in dict_.items():
        base_output = base_output.when(column_or_name == cond, val)

    return base_output
