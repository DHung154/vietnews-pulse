import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from dashboard import presentation as p
from streamlit.testing.v1 import AppTest


class PresentationTest(unittest.TestCase):
    def test_session_ids_and_busy_guard(self):
        for value in ('../raw','x;stop','talk_20261021T190000Z_12345678/../x',''):
            with self.assertRaises(ValueError):p.session_path(value)
        with tempfile.TemporaryDirectory() as tmp,patch.object(p,'CONTROL',Path(tmp)),patch.object(p,'current',return_value=dict(status='running')),patch.object(p.subprocess,'Popen') as launch:
            with self.assertRaises(RuntimeError):p.start(False)
            launch.assert_not_called()

    def test_start_once_records_identity_before_child_and_failure(self):
        with tempfile.TemporaryDirectory() as tmp,patch.object(p,'CONTROL',Path(tmp)),patch.object(p.subprocess,'Popen',side_effect=OSError('fixture unavailable')):
            with self.assertRaises(OSError):p.start(False)
            current=p.current()
            self.assertEqual(current['status'],'failed')
            self.assertIn('fixture unavailable',current['error'])
            self.assertFalse(current['terminal_requested'])

    def test_missing_archive_is_unavailable_not_fallback_numbers(self):
        with tempfile.TemporaryDirectory() as tmp,patch.object(p,'ROOT',Path(tmp)),patch.object(p,'CONTROL',Path(tmp)):
            data=p.payload()
            self.assertIsNone(data['archive'])
            self.assertIsNotNone(data['archive_error'])
            self.assertIsNone(data['live'])

    def test_native_terminal_is_opt_in_and_only_one_child(self):
        import os
        with tempfile.TemporaryDirectory() as tmp,patch.object(p,'CONTROL',Path(tmp)),patch.object(p.subprocess,'Popen') as child:
            child.return_value.pid=123
            result=p.start(True)
            self.assertEqual(result['pid'],123)
            child.assert_called_once()
            args,options=child.call_args
            self.assertNotIn('shell',options)
            if os.name=='nt':
                self.assertEqual(options['creationflags'],p.subprocess.CREATE_NEW_CONSOLE)
                self.assertIn('--keep-open',args[0])

    def test_ui_load_never_starts_and_tick_has_no_docker(self):
        with patch.object(p,'archived_evidence',side_effect=FileNotFoundError('fixture no archive')),patch.object(p,'current',return_value=None),patch.object(p,'start') as launch,patch.object(p.subprocess,'run') as run:
            app=AppTest.from_file('dashboard/demo.py').run(timeout=20)
            self.assertFalse(app.exception)
            self.assertEqual(app.sidebar.selectbox[0].value,'Báo cáo 3–4 phút')
            launch.assert_not_called();run.assert_not_called()

    def test_client_is_persistent_safe_and_can_pause(self):
        source=(p.ROOT/'dashboard/presentation_ui/presentation.js').read_text(encoding='utf-8')
        self.assertIn("event.source===window.parent",source)
        self.assertIn("textContent",source)
        self.assertNotIn('setTimeout(',source)
        self.assertIn('prefers-reduced-motion',source)
        self.assertIn('requestAnimationFrame',source)
        self.assertNotIn('eval(',source)
