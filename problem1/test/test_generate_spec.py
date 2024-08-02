import unittest
from unittest.mock import patch, mock_open
from problem1.util.generate_spec import generate_spec


class TestGenerateSpec(unittest.TestCase):

    def test_generate_spec_valid(self):
        # Mock data for a valid specification file
        mock_spec_content = "Name:10\nAge:3\nCity:15\n"
        expected_spec = {"Name": 10, "Age": 3, "City": 15}

        with patch("builtins.open", mock_open(read_data=mock_spec_content)):
            spec = generate_spec("dummy_spec.txt")
            self.assertEqual(spec, expected_spec)

    def test_generate_spec_invalid_format(self):
        # Mock data for an invalid specification file format
        mock_spec_content = "Name-10\nAge-3\nCity-15\n"

        with patch("builtins.open", mock_open(read_data=mock_spec_content)):
            with self.assertRaises(Exception):
                generate_spec("dummy_spec.txt")

    def test_generate_spec_missing_col_len(self):
        # Mock data for missing column length
        mock_spec_content = "Name:\nAge:3\nCity:15\n"

        with patch("builtins.open", mock_open(read_data=mock_spec_content)):
            with self.assertRaises(Exception):
                generate_spec("dummy_spec.txt")

    def test_generate_spec_file_not_found(self):
        # Mock behavior when the file is not found
        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(Exception):
                generate_spec("dummy_spec.txt")


if __name__ == "__main__":
    unittest.main()
