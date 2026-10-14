import unittest
from dashboard.pipeline_visual import render_pipeline


class PipelineVisualTest(unittest.TestCase):
    def test_waiting_running_error_and_missing_output(self):
        rows=[{'task_id':'prepare','state':'success'},{'task_id':'upload','state':'running'},
              {'task_id':'read','state':'failed'},{'task_id':'clean','state':'upstream_failed'}]
        page=render_pipeline(rows,{})
        self.assertEqual(page.count('<article '),9)
        self.assertEqual(page.count('class="processing"'),1)
        self.assertIn('Task xong · chưa đọc được output',page)
        self.assertIn('station s3 failed',page)
        self.assertIn('station s4 blocked',page)
        self.assertNotIn('778',page)

    def test_metrics_only_from_successful_stage_and_escape(self):
        outputs={'clean':{'clean_count':778,'duplicate_removed':31}}
        page=render_pipeline([{'task_id':'clean','state':'running'}],outputs)
        self.assertNotIn('778',page)
        page=render_pipeline([{'task_id':'clean','state':'success'}],outputs,False)
        self.assertIn('778 bài · loại 31 dòng trùng',page)
        self.assertIn('motion-off',page)
        self.assertIn('prefers-reduced-motion',page)
        page=render_pipeline([{'task_id':'prepare','state':'success'}],{'prepare':{'live_datanodes':'<script>x</script>'}})
        self.assertNotIn('<script>',page)
        self.assertIn('&lt;script&gt;',page)
