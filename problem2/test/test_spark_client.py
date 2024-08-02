import unittest
from pyspark.sql import Row

from problem2.util.spark_client import SparkClient


class TestSparkClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = SparkClient("TestApp")
        if cls.client.spark is None:
            raise RuntimeError("Failed to initialize SparkClient.")

        # Create temporary small CSV data for testing
        cls.small_data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
        cls.small_df = cls.client.spark.createDataFrame(cls.small_data, ["name", "value"])
        cls.small_df.write.csv("test_small_data.csv", header=True, mode='overwrite')

        # Create temporary medium CSV data for testing
        cls.medium_data = [("David", 4), ("Eva", 5), ("Frank", 6)]
        cls.medium_df = cls.client.spark.createDataFrame(cls.medium_data, ["name", "value"])
        cls.medium_df.write.csv("test_medium_data.csv", header=True, mode='overwrite')

        # Create temporary large CSV data for testing
        cls.large_data = [("George", 7), ("Hannah", 8), ("Ivy", 9)]
        cls.large_df = cls.client.spark.createDataFrame(cls.large_data, ["name", "value"])
        cls.large_df.write.csv("test_large_data.csv", header=True, mode='overwrite')

    @classmethod
    def tearDownClass(cls):
        cls.client.stop()

    def test_read_data_small(self):
        try:
            df = self.client.read_data("test_small_data.csv", "small")
            self.assertEqual(df.count(), len(self.small_data))
        except Exception as e:
            self.fail(f"test_read_data_small failed: {e}")

    def test_read_data_medium(self):
        try:
            df = self.client.read_data("test_medium_data.csv", "medium")
            self.assertEqual(df.count(), len(self.medium_data))
            self.assertEqual(df.rdd.getNumPartitions(), 4)
        except Exception as e:
            self.fail(f"test_read_data_medium failed: {e}")

    def test_read_data_large(self):
        try:
            df = self.client.read_data("test_large_data.csv", "large")
            self.assertEqual(df.count(), len(self.large_data))
            self.assertEqual(df.rdd.getNumPartitions(), 100)
        except Exception as e:
            self.fail(f"test_read_data_large failed: {e}")

    def test_write_data(self):
        try:
            output_path = "test_output.parquet"
            self.client.write_data(self.small_df, output_path)
            df = self.client.spark.read.parquet(output_path)
            self.assertEqual(df.count(), len(self.small_data))
        except Exception as e:
            self.fail(f"test_write_data failed: {e}")


if __name__ == "__main__":
    unittest.main()
