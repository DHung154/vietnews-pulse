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

    def test_worker_is_hidden_and_terminal_is_a_separate_viewer(self):
        import os
        with tempfile.TemporaryDirectory() as tmp,patch.object(p,'CONTROL',Path(tmp)),patch.object(p.subprocess,'Popen') as child:
            child.return_value.pid=123
            result=p.start(True)
            self.assertEqual(result['pid'],123)
            self.assertEqual(child.call_count,2)
            args,options=child.call_args_list[0]
            self.assertNotIn('shell',options)
            if os.name=='nt':
                self.assertEqual(options['creationflags'],p.subprocess.CREATE_NO_WINDOW)
                self.assertNotIn('--keep-open',args[0])
                viewer_args,viewer_options=child.call_args_list[1]
                self.assertEqual(viewer_options['creationflags'],p.subprocess.CREATE_NEW_CONSOLE)
                self.assertIn('--follow-only',viewer_args[0])
                self.assertIn('--keep-open',viewer_args[0])

    def test_reopen_closed_terminal_reuses_session_without_launching_work(self):
        session='talk_20261029T010000Z_12345678'
        with tempfile.TemporaryDirectory() as tmp,patch.object(p,'CONTROL',Path(tmp)),patch.object(p.subprocess,'Popen') as child,patch.object(p,'_pid_running',side_effect=[True,False]):
            work=p.session_path(session);work.mkdir()
            p.atomic_write_json(work/'state.json',dict(status='success'))
            child.return_value.pid=123
            p.open_terminal(session)
            p.open_terminal(session)
            self.assertEqual(child.call_count,1,'An open viewer is reused')
            child.return_value.pid=456
            reopened=p.open_terminal(session)
            self.assertEqual(reopened['pid'],456)
            self.assertEqual(child.call_count,2)
            for call in child.call_args_list:self.assertIn('--follow-only',call.args[0])
            self.assertEqual(json.loads((work/'state.json').read_text())['status'],'success')

    def test_dead_previous_worker_does_not_leave_demo_permanently_busy(self):
        session='talk_20261029T010000Z_12345678'
        with tempfile.TemporaryDirectory() as tmp,patch.object(p,'CONTROL',Path(tmp)),patch.object(p.subprocess,'Popen') as child,patch.object(p,'_pid_running',return_value=False):
            work=p.session_path(session);work.mkdir()
            p.atomic_write_json(work/'state.json',dict(session_id=session,status='running',pid=123,steps={}))
            p.atomic_write_json(p.CONTROL/'current.json',dict(session_id=session))
            self.assertEqual(p.current()['status'],'interrupted')
            child.return_value.pid=456
            new=p.start(False)
            self.assertNotEqual(new['session_id'],session)
            self.assertEqual(json.loads((work/'state.json').read_text())['status'],'failed')
            child.assert_called_once()

    def test_ui_load_never_starts_and_tick_has_no_docker(self):
        with patch.object(p,'archived_evidence',side_effect=FileNotFoundError('fixture no archive')),patch.object(p,'current',return_value=None),patch.object(p,'start') as launch,patch.object(p.subprocess,'run') as run:
            app=AppTest.from_file('dashboard/demo.py').run(timeout=20)
            self.assertFalse(app.exception)
            self.assertEqual(app.sidebar.selectbox[0].value,'Demo nhanh · 1 phút 45 giây')
            launch.assert_not_called();run.assert_not_called()

    def test_client_is_persistent_safe_and_can_pause(self):
        source=(p.ROOT/'dashboard/presentation_ui/presentation.js').read_text(encoding='utf-8')
        self.assertIn("event.source===window.parent",source)
        self.assertIn("textContent",source)
        self.assertNotIn('setTimeout(',source)
        self.assertIn('prefers-reduced-motion',source)
        self.assertIn('requestAnimationFrame',source)
        self.assertNotIn('eval(',source)
