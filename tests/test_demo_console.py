import tempfile
import unittest
from pathlib import Path
from streamlit.testing.v1 import AppTest
from dashboard.demo import STEPS, command, evidence


class DemoTest(unittest.TestCase):
    def test_missing_evidence_and_command_allowlist(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                evidence('read',Path(tmp))
        with self.assertRaises(ValueError):
            evidence('../raw')
        for step,_,_ in STEPS:
            argv=command(step)
            self.assertNotIn('run',argv)
            self.assertNotIn('-rm',argv)

    def test_all_stages_render_without_running_jobs(self):
        app=AppTest.from_file('dashboard/demo.py').run(timeout=20)
        app.sidebar.selectbox[0].select('Xem chi tiết 9 bước').run(timeout=20)
        self.assertFalse(app.exception)
        self.assertEqual(app.metric[1].value,'778')
        for index in range(9):
            app.sidebar.radio[0].set_value(index).run(timeout=20)
            self.assertFalse(app.exception)
