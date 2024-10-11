"""
This functions module provides utility column function that takes column and return column.
You could treat this module as the extension of the pyspark.sql.functions
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

def when_mapping(column_or_name: Column | str, dict_: dict) -> Column:
    """
    Maps values in a column based on a dictionary of conditions using PySpark.

    Args:
        column_or_name (Column | str): The column or column name to apply the mapping to.
        dict_ (dict): A dictionary where keys are conditions and values are the corresponding
                      values to map to when the condition is met.

    Returns:
        Column: A PySpark Column with the mapped values.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql import functions as F
        >>> spark = SparkSession.builder.appName("example").getOrCreate()
        >>> data = [("A",), ("B",), ("C",)]
        >>> df = spark.createDataFrame(data, ["letter"])
        >>> mapping_dict = {"A": 1, "B": 2, "C": 3}
        >>> df = df.withColumn("mapped", when_mapping(F.col("letter"), mapping_dict))
        >>> df.show()
        +------+------+
        |letter|mapped|
        +------+------+
        |     A|     1|
        |     B|     2|
        |     C|     3|
        +------+------+
    """
    column_or_name = (
        F.col(column_or_name) if isinstance(column_or_name, str) else column_or_name
    )
    base_output = F
    for cond, val in dict_.items():
        base_output = base_output.when(column_or_name == cond, val)

    return base_output
