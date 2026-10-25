import tempfile
import json
import sqlite3
import unittest
from pathlib import Path
from streamlit.testing.v1 import AppTest
from dashboard.demo import STEPS, command, evidence
from dashboard import demo, demo_runner
from unittest.mock import patch


def render_fixture():
    from dashboard.demo import main
    main()


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
        # Synthetic two-article evidence, never the developer's production files.
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo,'WORK',Path(tmp)), patch.object(demo_runner,'current',return_value=None), patch.object(demo_runner,'start') as start:
            work=Path(tmp)
            for step,_,_ in STEPS:
                row=dict(status='ok',completed_at='2026-10-14T00:00:00+00:00',fixture=True)
                if step=='clean': row.update(raw_count=3,clean_count=2,duplicate_removed=1)
                if step=='cluster': row.update(sizes=[dict(topic_cluster=0,count=1),dict(topic_cluster=1,count=1)],silhouette=0.1)
                if step=='save': row['serving']=dict(coverage_status='incomplete')
                if step=='kafka': row.update(first=['fixture',3,2],replay=['fixture',6,2],restart=['fixture',6,2])
                (work/(step+'.json')).write_text(json.dumps(row),encoding='utf-8')
            (work/'serving').mkdir()
            connection=sqlite3.connect(work/'serving'/('lab_'+demo.RUN+'.sqlite'))
            try:
                connection.execute('CREATE TABLE articles(article_id,title,source,category,sentiment_label)')
                connection.execute("INSERT INTO articles VALUES ('fixture','FIXTURE sample','fixture','fixture',NULL)")
                connection.commit()
            finally:
                connection.close()
            app=AppTest.from_function(render_fixture).run(timeout=20)
            app.sidebar.selectbox[0].select('Xem chi tiết 9 bước').run(timeout=20)
            self.assertFalse(app.exception)
            self.assertEqual(app.metric[1].value,'2')
            for index in range(9):
                app.sidebar.radio[0].set_value(index).run(timeout=20)
                self.assertFalse(app.exception)
            start.assert_not_called()
