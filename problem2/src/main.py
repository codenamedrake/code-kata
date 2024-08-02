import argparse
import sys

from problem2.util.spark_client import SparkClient
from problem2.util.common_module import encrypt


def extract(spark_client, in_path, size_type):
    df = spark_client.read_data(in_path, size_type)
    return df


def transform(df, _anonimize):
    df = encrypt(df, _anonimize)
    return df


def load(spark_client, df, out_path):
    spark_client.write_data(df, out_path)


def execute(in_path, _anonimize, out_path, dsize):
    spark = SparkClient()
    df = extract(spark, in_path, dsize)
    df = transform(df, _anonimize)
    load(spark, df, out_path)
    spark.stop()


def main(given_input_path, given_col_to_anonimize, given_output_path, data_size):
    execute(given_input_path, given_col_to_anonimize, given_output_path, data_size)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Spark Data Anonymization")
        parser.add_argument('--size', type=str, required=True, help="Size of the dataset: small, medium, large")
        parser.add_argument('--input_path', type=str, required=True, help="Path to the input data file")
        parser.add_argument('--output_path', type=str, required=True, help="Path to save the output data file")
        parser.add_argument('--cols', type=str, nargs='+', required=True, help="Columns to anonymize")
        args = parser.parse_args()
        main(args.input_path, args.cols, args.output_path, args.size)
    except Exception:
        raise Exception("could not process the data")
