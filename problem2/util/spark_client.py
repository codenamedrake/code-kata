from pyspark import StorageLevel
from pyspark.sql import SparkSession


class SparkClient:
    """
    A client to handle reading, processing, and writing data using Apache Spark.

    Attributes:
        spark (SparkSession): The Spark session used for data processing.
    """

    def __init__(self, app_name="Anonymize"):
        """
        Initialize the SparkClient with a given application name.

        Args:
            app_name (str): The name of the Spark application.
        """
        try:
            self.spark = SparkSession.builder.appName(app_name).getOrCreate()
            print("Spark session initiated.")
        except Exception as e:
            print(f"Failed to initiate Spark session: {e}")
            self.spark = None

    def read_data(self, input_path, size):
        """
        Read data from the specified input path based on the size of the dataset.

        Args:
            input_path (str): The path to the input data file.
            size (str): The size of the dataset ('small', 'medium', 'large').

        Returns:
            DataFrame: The loaded Spark DataFrame.

        Raises:
            ValueError: If the size is not 'small', 'medium', or 'large'.
        """
        if self.spark is None:
            raise RuntimeError("Spark session is not available.")

        try:
            if size == 'small':
                df = self.spark.read.csv(input_path, header=True, inferSchema=True)
            elif size == 'medium':
                df = self.spark.read.csv(input_path, header=True, inferSchema=True)
                df = df.repartition(4)
                df.cache()
            elif size == 'large':
                df = self.spark.read.csv(input_path, header=True, inferSchema=True)
                df = df.repartition(100)
                df.write.parquet("path/to/large_dataset.parquet")
                df = self.spark.read.parquet("path/to/large_dataset.parquet")
                df.persist(StorageLevel.MEMORY_AND_DISK)
            else:
                raise ValueError("Size must be 'small', 'medium', or 'large'.")
            print(f"Data read from path: {input_path}.")
            return df
        except Exception as e:
            print(f"Failed to read data from path {input_path}: {e}")
            raise

    def write_data(self, df, output_path):
        """
        Write the given DataFrame to the specified output path in Parquet format.

        Args:
            df (DataFrame): The Spark DataFrame to be written.
            output_path (str): The path to the output data file.
        """
        if self.spark is None:
            raise RuntimeError("Spark session is not available.")

        try:
            df.write.parquet(output_path, mode='overwrite')
            print(f"Data written to path: {output_path}.")
        except Exception as e:
            print(f"Failed to write data to path {output_path}: {e}")
            raise

    def stop(self):
        """
        Stop the Spark session.
        """
        if self.spark is not None:
            try:
                self.spark.stop()
                print("Spark session stopped.")
            except Exception as e:
                print(f"Failed to stop Spark session: {e}")
        else:
            print("Spark session was not initialized.")
