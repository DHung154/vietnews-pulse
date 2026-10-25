import subprocess
import sys
import tempfile
import time
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
            ready = Path(root) / 'ready'
            repo = Path(__file__).resolve().parents[1]
            code = "import sys; sys.path.insert(0, sys.argv[3]); from pathlib import Path; from news_pipeline.writer_lock import single_writer; exec(\"with single_writer(Path(sys.argv[1])):\\n    Path(sys.argv[2]).write_text('locked')\\n    sys.stdin.readline()\")"
            child = subprocess.Popen([sys.executable, "-c", code, str(lock), str(ready), str(repo / 'src')], cwd=repo, stdin=subprocess.PIPE)
            try:
                # Readiness is emitted inside the lock; process startup time is not evidence.
                deadline = time.monotonic() + 10
                while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(ready.exists(), 'child did not acquire lock before deadline')
                self.assertIsNone(child.poll(), 'child exited before contention probe')
                with self.assertRaises(RuntimeError):
                    with single_writer(lock):
                        pass
            finally:
                try:
                    child.communicate(b'release\n', timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0)
            with single_writer(lock):
                pass


if __name__ == "__main__":
    unittest.main()
