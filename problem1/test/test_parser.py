import unittest
from unittest.mock import patch, mock_open


from problem1.util.generate_spec import generate_spec
from problem1.util.parser import fixed_width_parser


class TestFixedWidthParser(unittest.TestCase):

    def test_generate_spec_valid(self):
        mock_spec_content = "Name:10\nAge:3\nCity:15\n"
        expected_spec = {"Name": 10, "Age": 3, "City": 15}

        with patch("builtins.open", mock_open(read_data=mock_spec_content)):
            spec = generate_spec("dummy_spec.txt")
            self.assertEqual(spec, expected_spec)

    def test_generate_spec_invalid_format(self):
        mock_spec_content = "Name-10\nAge-3\nCity-15\n"

        with patch("builtins.open", mock_open(read_data=mock_spec_content)):
            with self.assertRaises(Exception):
                generate_spec("dummy_spec.txt")

    def test_generate_spec_file_not_found(self):
        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(Exception):
                generate_spec("dummy_spec.txt")

    @patch("builtins.open", new_callable=mock_open,
           read_data="John      25 New York       \nJane      30 San Francisco ")
    def test_fixed_width_parser_valid(self, mock_file):
        spec = {"Name": 10, "Age": 3, "City": 15}
        input_filename = "dummy_input.txt"
        output_filename = "dummy_output.csv"

        with patch("csv.writer") as mock_writer:
            mock_writerow = mock_writer.return_value.writerow
            fixed_width_parser(spec, input_filename, output_filename)

            # Check header row
            mock_writerow.assert_any_call(["Name", "Age", "City"])
            # Check data rows
            mock_writerow.assert_any_call(["John", "25", "New York"])
            mock_writerow.assert_any_call(["Jane", "30", "San Francisco"])


if __name__ == "__main__":
    unittest.main()
