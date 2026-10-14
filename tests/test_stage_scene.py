import unittest
from dashboard.stage_scene import DETAILS, focus_stage, render_scene


class StageSceneTest(unittest.TestCase):
    def test_follow_live_stage_and_no_fake_motion_after_finish(self):
        rows=[{'task_id':'prepare','state':'success'},{'task_id':'upload','state':'running'}]
        self.assertEqual(focus_stage(rows),'upload')
        markup=render_scene(rows,{})
        self.assertIn('stage-scene moving',markup)
        self.assertIn('data-scene="upload"',markup)
        failed=render_scene([{'task_id':'clean','state':'failed'}],{})
        self.assertIn('data-scene="clean"',failed)
        self.assertNotIn('<section class="stage-scene moving"',failed)
        complete=render_scene([{'task_id':key,'state':'success'} for key in DETAILS],{})
        self.assertIn('data-scene="verify"',complete)
        self.assertNotIn('<section class="stage-scene moving"',complete)

    def test_nine_distinct_explanatory_scenes_and_pause(self):
        for key in DETAILS:
            with self.subTest(key=key):
                html=render_scene([],{},preview=key)
                self.assertIn('MINH HỌA CÔNG ĐOẠN',html)
                self.assertIn('KHÔNG CHẠY LẠI PIPELINE',html)
                self.assertIn(f'data-scene="{key}"',html)
                self.assertIn('Chưa có kết quả bước này',html)
                self.assertNotIn('778 bài',html)
                paused=render_scene([],{},False,preview=key)
                self.assertNotIn('<section class="stage-scene moving"',paused)
        with self.assertRaises(ValueError):
            render_scene([],{},preview='<script>')

    def test_studio_layout_and_accessible_statuses(self):
        rows=[{'task_id':'clean','state':'failed'}]
        html=render_scene(rows,{},False)
        self.assertIn('01 / ĐẦU VÀO',html)
        self.assertIn('02 / CANONICAL ENGINE',html)
        self.assertIn('03 / ĐẦU RA',html)
        self.assertIn('Có lỗi',html)
        self.assertEqual(html.count('class="rail-stop '),9)
        self.assertIn('scene-mobile',html)
        self.assertIn('prefers-reduced-motion',html)
