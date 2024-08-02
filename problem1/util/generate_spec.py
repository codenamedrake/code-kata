def generate_spec(spec_filename: str) -> dict:
    """
    Generates a specification dictionary from a given specification file.

    Parameters:
    spec_filename (str): The name of the specification file.

    Returns:
    dict: A dictionary where keys are column names and values are their respective widths.
    """
    spec = {}
    try:
        with open(spec_filename, 'r') as specfile:
            for line in specfile:
                if line.strip():
                    col_name, col_len = line.strip().split(":")
                    spec[col_name] = int(col_len)
    except Exception as e:
        raise Exception(f"Failed to parse spec file: {e}")
    return spec
