import argparse

from problem1.util import generate_spec, parser


def main():
    try:
        arg_parser = argparse.ArgumentParser(description="Spark Data Anonymization")
        arg_parser.add_argument('--specfile', type=str, required=True, help="Specfile containing width info")
        arg_parser.add_argument('--input_path', type=str, required=True, help="Path to the input data file")
        arg_parser.add_argument('--output_path', type=str, required=True, help="Path to save the output data file")
        args = arg_parser.parse_args()
        # Spec Format Example
        # spec = {"employee_name": 10, "Address": 32, "Work_location": 15}

        spec = generate_spec.generate_spec(args.specfile)
        parser.fixed_width_parser(spec, args.input_path, args.output_path)
    except Exception:
        raise Exception("could not parse the data")


if __name__ == "__main__":
    main()
