import tempfile
import unittest
from pathlib import Path
from scripts.start_airflow_lab import archive_stale_pid


class AirflowStartupTest(unittest.TestCase):
    def test_stale_pid_is_preserved_not_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);pid=root/'airflow-webserver.pid';pid.write_text('24\n')
            process=root/'proc/24';process.mkdir(parents=True)
            (process/'cmdline').write_bytes(b'airflow\0triggerer\0')
            archive=archive_stale_pid(pid,root/'proc')
            self.assertFalse(pid.exists())
            self.assertEqual(archive.read_text(),'24\n')

    def test_live_webserver_pid_is_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);pid=root/'airflow-webserver.pid';pid.write_text('24')
            process=root/'proc/24';process.mkdir(parents=True)
            (process/'cmdline').write_bytes(b'gunicorn\0airflow-webserver\0')
            with self.assertRaises(RuntimeError):
                archive_stale_pid(pid,root/'proc')
            self.assertEqual(pid.read_text(),'24')
