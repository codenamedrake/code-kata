import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col

from problem2.util.common_module import encrypt


class TestEncryptFunction(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestEncrypt").master("local[*]").getOrCreate()

        # Create a sample DataFrame for testing
        cls.data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
        cls.df = cls.spark.createDataFrame(cls.data, ["name", "value"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_encrypt_single_column(self):
        encrypted_df = encrypt(self.df, ["name"])

        # Expected DataFrame
        expected_data = [
            (sha2(col("name").cast("string"), 256).eval("Alice"), 1),
            (sha2(col("name").cast("string"), 256).eval("Bob"), 2),
            (sha2(col("name").cast("string"), 256).eval("Cathy"), 3)
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["name", "value"])

        self.assertEqual(encrypted_df.collect(), expected_df.collect())

    def test_encrypt_multiple_columns(self):
        encrypted_df = encrypt(self.df, ["name", "value"])

        # Expected DataFrame
        expected_data = [
            (sha2(col("name").cast("string"), 256).eval("Alice"), sha2(col("value").cast("string"), 256).eval("1")),
            (sha2(col("name").cast("string"), 256).eval("Bob"), sha2(col("value").cast("string"), 256).eval("2")),
            (sha2(col("name").cast("string"), 256).eval("Cathy"), sha2(col("value").cast("string"), 256).eval("3"))
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["name", "value"])

        self.assertEqual(encrypted_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
