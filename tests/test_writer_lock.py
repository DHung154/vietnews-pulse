import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from src.news_pipeline.writer_lock import single_writer


class WriterLockTest(unittest.TestCase):
    def test_io_error_in_writer_is_not_misreported_as_busy_lock(self):
        with tempfile.TemporaryDirectory() as root:
            lock = Path(root) / "writer.lock"
            with self.assertRaisesRegex(OSError, "disk failure"):
                with single_writer(lock):
                    raise OSError("disk failure")
            with single_writer(lock):
                pass

    def test_crashed_process_does_not_leave_lock_held(self):
        with tempfile.TemporaryDirectory() as root:
            lock = Path(root) / "serving" / ".snapshot_writer.lock"
            code = "import os,sys; sys.path.insert(0, r'E:\\BigData\\src'); from pathlib import Path; from news_pipeline.writer_lock import single_writer; exec(\"with single_writer(Path(sys.argv[1])):\\n    os._exit(9)\")"
            child = subprocess.run([sys.executable, "-c", code, str(lock)], cwd=r"E:\BigData", check=False)
            self.assertEqual(child.returncode, 9)
            with single_writer(lock):
                pass
            self.assertTrue(lock.exists())

    def test_one_serving_path_is_shared_across_processes(self):
        with tempfile.TemporaryDirectory() as root:
            lock = Path(root) / ".snapshot_writer.lock"
            code = "import sys,time; sys.path.insert(0, r'E:\\BigData\\src'); from pathlib import Path; from news_pipeline.writer_lock import single_writer; exec(\"with single_writer(Path(sys.argv[1])):\\n    time.sleep(1.5)\")"
            child = subprocess.Popen([sys.executable, "-c", code, str(lock)], cwd=r"E:\BigData")
            try:
                import time
                time.sleep(0.3)
                with self.assertRaises(RuntimeError):
                    with single_writer(lock):
                        pass
            finally:
                child.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
