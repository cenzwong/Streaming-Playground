def expect_unique(col_name):
    """
    A decorator function that ensures the uniqueness of a column in a Spark DataFrame.

    Parameters:
        col_name (str): The column's name.

    Returns:
        function: A decorated function that checks the column's uniqueness.

    Raises:
        AssertionError: If the column's count and distinct count are not equal.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            spark_table_sdf = func(*args, **kwargs)
            spark_table_col_sdf = spark_table_sdf.select(col_name)
            normal_count = spark_table_col_sdf.count()
            distinct_count = spark_table_col_sdf.distinct().count()
            assert (
                normal_count == distinct_count
            ), f"Count and distinct count of column '{col_name}' are not equal"
            print(f"✅: Column '{col_name}' is distinct")
            return spark_table_sdf

        return wrapper

    return decorator


def expect_criteria(criteria):
    """
    A decorator function that ensures a specific criterion on a Spark DataFrame.

    Parameters:
        criteria (pyspark.sql.column.Column): The filter criterion to be applied to the DataFrame.

    Returns:
        function: A decorated function that checks the criterion.

    Raises:
        AssertionError: If the filtered count and unfiltered count of the DataFrame are not equal.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            spark_table_sdf = func(*args, **kwargs)
            filtered_count = spark_table_sdf.filter(criteria).count()
            unfiltered_count = spark_table_sdf.count()
            assert (
                filtered_count == unfiltered_count
            ), f"Filtered count is not equal to unfiltered count {criteria}"
            print(f"✅: Criteria '{criteria}' passed")
            return spark_table_sdf

        return wrapper

    return decorator