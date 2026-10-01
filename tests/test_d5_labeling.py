import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import create_labeling_pool


class LabelingPoolSafetyTest(unittest.TestCase):
    def test_rerun_refuses_nonempty_output_and_preserves_labels(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            input_dir = root / "input"
            output = root / "packet"
            input_dir.mkdir()
            output.mkdir()
            labelled = output / "annotator_a.csv"
            labelled.write_text("annotation_id,input_text,label\nkeep,Noi dung,positive\n", encoding="utf-8")
            before = labelled.read_bytes()
            args = ["create_labeling_pool.py", "--input", str(input_dir), "--output", str(output), "--sample-size", "1"]
            with patch("sys.argv", args), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(FileExistsError):
                    create_labeling_pool.main()
            self.assertEqual(before, labelled.read_bytes())


if __name__ == "__main__":
    unittest.main()
