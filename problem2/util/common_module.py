from pyspark.sql.functions import sha2, col


def encrypt(df, col_names):
    """
    Encrypts specified columns in the DataFrame using SHA-256.

    Args:
        df (DataFrame): The input Spark DataFrame.
        col_names (list): List of column names to encrypt.

    Returns:
        DataFrame: The DataFrame with specified columns encrypted.
    """
    for column in col_names:
        df = df.withColumn(column, sha2(col(column).cast("string"), 256))
    return df
