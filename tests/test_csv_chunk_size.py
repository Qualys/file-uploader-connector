import csv
import io
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from main import CsvUploader


class CsvChunkSizeTests(unittest.TestCase):
    def test_row_size_matches_csv_serialization(self):
        row = ["asset", "value,with,commas\nand a newline"]
        buffer = io.StringIO(newline="")
        csv.writer(buffer, quotechar='"', quoting=csv.QUOTE_MINIMAL).writerow(row)

        self.assertEqual(
            CsvUploader._csv_row_size(row),
            len(buffer.getvalue().encode("utf-8")),
        )

    def test_quoted_row_is_not_measured_as_plain_comma_join(self):
        row = ["asset", "value,with,commas"]
        legacy_size = len(",".join(row).encode("utf-8")) + 1

        self.assertGreater(CsvUploader._csv_row_size(row), legacy_size)


if __name__ == "__main__":
    unittest.main()
