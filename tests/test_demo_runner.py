import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from dashboard import demo_runner as runner
from streamlit.testing.v1 import AppTest


class DemoRunnerTest(unittest.TestCase):
    def test_json_with_log_and_busy_run_blocked(self):
        self.assertEqual(runner.json_output('log line\n[{"state":"running"}]')[0]['state'],'running')
        with self.assertRaises(ValueError):
            runner.json_output('service unavailable')
        with tempfile.TemporaryDirectory() as tmp, patch.object(runner,'CONTROL',Path(tmp)), patch.object(runner,'cli',return_value='[{"state":"running"}]') as cli:
            with self.assertRaises(RuntimeError):
                runner.start()
            self.assertEqual(cli.call_count,1)
            self.assertIsNone(runner.current())

    def test_trigger_new_id_and_preserves_identity_on_timeout(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp, patch.object(runner,'CONTROL',Path(tmp)), patch.object(runner,'cli',side_effect=['[]','ok',subprocess.TimeoutExpired('docker',20)]):
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.start()
            info=runner.current()
            self.assertTrue(info['run_id'].startswith('ui_demo_'))
            self.assertEqual(runner.work_path(info).name,info['scope'])

    def test_ui_does_not_trigger_job_on_load_and_displays_failed_state(self):
        with patch.object(runner,'current',return_value=None), patch.object(runner,'start') as start:
            app=AppTest.from_file('dashboard/demo.py').run(timeout=20)
            self.assertFalse(app.exception)
            start.assert_not_called()
        info={'run_id':'ui_demo_test','scope':'unused','started_at':'2026-10-14T00:00:00+00:00'}
        with patch.object(runner,'current',return_value=info), patch.object(runner,'states',return_value=[{'task_id':'prepare','state':'failed'}]):
            app=AppTest.from_file('dashboard/demo.py').run(timeout=20)
            self.assertFalse(app.exception)
            self.assertTrue(any('Pipeline có bước lỗi' in e.value for e in app.error))
