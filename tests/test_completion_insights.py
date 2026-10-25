import unittest
from scripts.analyze_completion_snapshot import bars


class InsightChartTest(unittest.TestCase):
    def test_chart_escapes_text_and_handles_empty(self):
        svg=bars([{'name':'<script>&','count':3}],'Title <test>')
        self.assertNotIn('<script>',svg)
        self.assertIn('&lt;script&gt;&amp;',svg)
        self.assertIn('>3</text>',svg)
        self.assertIn('<svg',bars([],'Empty'))
