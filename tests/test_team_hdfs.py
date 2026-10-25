import unittest
from scripts.team_read_hdfs import validate_server, DATASETS


class TeamHdfsTest(unittest.TestCase):
    def test_full_and_lab_are_separate_frozen_scopes(self):
        self.assertEqual(DATASETS['full'][1],13388)
        self.assertEqual(DATASETS['lab'][1],778)
        self.assertNotEqual(DATASETS['full'][0],DATASETS['lab'][0])
    def test_only_tailscale_ipv4(self):
        self.assertEqual(validate_server('100.100.1.2'),'100.100.1.2')
        for value in ('127.0.0.1','0.0.0.0','172.16.126.194','8.8.8.8','100.128.0.1','::1','x;cmd'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_server(value)
