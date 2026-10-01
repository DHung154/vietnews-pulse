# Handoff review event groups

`scripts/prepare_event_groups.py` tạo review sheet từ metadata pilot, không sửa packet và không tự suy luận sự kiện. Chỉ hai trường hợp exact identity được gợi ý: cùng `article_id` hoặc cùng `content_group_id`. Các quan hệ diễn đạt lại/sự kiện liên quan vẫn phải người kiểm tra.

## Cách làm

1. Mở `evidence/d06/event_group_review_20260917_r2/event_group_review.csv`.
2. Đọc `input_text` và kiểm tra `input_sha256`; không sửa hai cột này. Với từng dòng, điền `event_group_id` ổn định cho các bài cùng sự kiện; bài không cùng sự kiện có group riêng ổn định. Không để trống.
3. Điền `review_status=reviewed`, `reviewer` và `review_reason` không rỗng. Không điền sentiment vào file này.
4. Kiểm tra một event group không nằm ở nhiều split.
5. Tạo metadata version mới, không ghi đè packet:

```text
python scripts\apply_event_groups.py --metadata annotations\news_sentiment_gold_v1\pilot_20260917\metadata.jsonl --review evidence\d06\event_group_review_20260917_r2\event_group_review.csv --output annotations\news_sentiment_gold_v1\pilot_20260917_event_grouped\metadata.jsonl
```

Output chỉ được tạo khi checksum manifest, input text/hash từng dòng, ID, group, reviewer/reason và split đều hợp lệ; không ghi đè output. Sau đó mới giao hai packet cho hai annotator; nhãn vẫn lưu append-only theo hướng dẫn `docs/NEWS_SENTIMENT_LABELING_GUIDE.md`.
