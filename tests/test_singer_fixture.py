import csv
import json
import tempfile
import unittest
from pathlib import Path

import target_csv


class SingerFixtureTest(unittest.TestCase):
    def test_data_singer_is_written_to_csv(self):
        fixture = Path(__file__).parent / "fixtures" / "data.singer"

        with fixture.open(encoding="utf-8") as source:
            messages = list(source)
        expected_records = sum(
            json.loads(message)["type"] == "RECORD" for message in messages
        )
        expected_state = [
            json.loads(message)["value"]
            for message in messages
            if json.loads(message)["type"] == "STATE"
        ][-1]

        with tempfile.TemporaryDirectory() as destination:
            state = target_csv.persist_messages(
                ",", '"', messages, destination, fixed_headers=None, validate=True
            )

            self.assertEqual(expected_state, state)
            output_files = list(Path(destination).glob("suppliers-*.csv"))
            self.assertEqual(1, len(output_files))

            with output_files[0].open(newline="", encoding="utf-8") as output:
                rows = list(csv.DictReader(output))
            self.assertEqual(expected_records, len(rows))
            self.assertEqual("939136", rows[0]["id"])

            with (Path(destination) / "job_metrics.json").open(encoding="utf-8") as metrics_file:
                metrics = json.load(metrics_file)
            self.assertEqual(expected_records, metrics["recordCount"]["suppliers"])
