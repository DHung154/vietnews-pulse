import ast
import copy
import json
import unittest
from pathlib import Path
from scripts.full_snapshot_pipeline import validate_config


class FullSnapshotSafetyTest(unittest.TestCase):
    def test_config_scope_and_schema(self):
        config=json.loads((Path(__file__).resolve().parents[1]/'config/full_snapshot.json').read_text())
        self.assertEqual(validate_config(config)['expected_count'],13388)
        for key,value in [('mode','raw'),('input_hdfs','file:///tmp/a'),('model_version','../raw'),('expected_count',0),('as_of','2026-10-08T00:00:00')]:
            bad=copy.deepcopy(config);bad[key]=value
            with self.assertRaises(ValueError):
                validate_config(bad)

    def test_hourly_dag_is_separate_and_nine_tasks(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'airflow/dags/vietnews_full_snapshot.py').read_text())
        stages=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='STEPS' for t in n.targets))
        self.assertEqual(len(stages),9)
        call=next(n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='DAG')
        values={v.arg:ast.literal_eval(v.value) for v in call.keywords if v.arg in ('schedule','catchup','max_active_runs')}
        self.assertEqual(values,dict(schedule='0 * * * *',catchup=False,max_active_runs=1))
