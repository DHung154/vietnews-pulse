import unittest
from scripts.audit_repo_secrets import matches


class SecretAuditTest(unittest.TestCase):
    def test_findings_never_include_credential_value(self):
        value='ghp_'+'x'*36
        findings=matches('token='+value,'fixture')
        self.assertEqual(len(findings),1)
        self.assertNotIn(value,str(findings))
        self.assertEqual(len(findings[0]['value_sha256']),64)
        self.assertEqual(matches('password="changeme"','fixture'),[])
