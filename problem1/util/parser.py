import csv


def fixed_width_parser(spec: dict, input_filename: str, output_filename: str):
    """
    Parses a fixed-width file into a CSV file based on the given specification.

    Parameters:
    spec (dict): A dictionary where keys are column names and values are the fixed widths.
    input_filename (str): The name of the input fixed-width file.
    output_filename (str): The name of the output CSV file.
    """
    try:
        with open(input_filename, 'r') as in_file, open(output_filename, 'w', newline='', encoding='utf-8') as out_file:

            reader = in_file.readlines()
            writer = csv.writer(out_file)

            # Write the header row based on the spec keys
            writer.writerow(list(spec.keys()))

            for line in reader:
                row = []
                start = 0
                for field, length in spec.items():
                    # Extract the field from the fixed-width line and strip any extra whitespace
                    row.append(line[start:start + length].strip())
                    start += length
                # Write the row to the Output CSV file
                writer.writerow(row)
    except Exception:
        raise Exception("Could not process input file!")
