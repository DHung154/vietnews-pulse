import hashlib
import unittest
from scripts.import_pilot_markdown import parse


class PilotMarkdownTest(unittest.TestCase):
    def setUp(self):
        self.sha=hashlib.sha256('Tin thành công'.encode()).hexdigest()
        self.metadata={'a':dict(input_text='Tin thành công',input_sha256=self.sha)}
        self.text=f'## Mẫu 001\n- **annotation_id:** `a`\n- **input_sha256:** `{self.sha}`\n```text\nTin thành công\n```\n- **Nhãn:** positive\n- **Bằng chứng:** thành công\n- **Lý do:** Kết quả rõ.\n'

    def test_valid_content_is_not_modified(self):
        rows,errors=parse(self.text,self.metadata)
        self.assertEqual(errors,[])
        self.assertEqual(rows[0]['input_text'],'Tin thành công')

    def test_changed_text_bad_span_and_duplicates_rejected(self):
        for text in (self.text.replace('Tin thành công','Text changed'),self.text.replace('**Bằng chứng:** thành công','**Bằng chứng:** absent'),self.text+self.text):
            with self.subTest(text=text):
                self.assertTrue(parse(text,self.metadata)[1])

    def test_blank_evidence_does_not_consume_next_line(self):
        text=self.text.replace('**Nhãn:** positive','**Nhãn:** needs_context').replace('**Bằng chứng:** thành công','**Bằng chứng:** ')
        rows,errors=parse(text,self.metadata)
        self.assertEqual(errors,[])
        self.assertEqual(rows[0]['evidence_span'],'')
