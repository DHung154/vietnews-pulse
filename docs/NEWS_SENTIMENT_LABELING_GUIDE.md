# Hướng dẫn gán nhãn news sentiment — bản pilot v0.1

Tài liệu phục vụ `DATA_COLLECTION_PLAN.md`. Đây là guideline dự kiến cần hiệu chỉnh qua 100 pilot; chưa có nhãn người thật được tạo bởi tài liệu này.

## 1. Chính xác đang gán điều gì?

Gán sắc thái nội dung chính được trình bày trong **title + description**, xét hậu quả/đánh giá được mô tả. Không gán cảm xúc cá nhân của người đọc, thái độ công chúng hay độ tin cậy của tin. Không suy rằng một báo “tiêu cực” vì đăng nhiều tin tai nạn.

Nhãn này bao gồm chiều tốt/xấu của sự việc được mô tả, không chỉ các tính từ cảm xúc. Phải chốt định nghĩa này với cả hai người trong pilot. Nếu nhóm muốn đo văn phong/stance của người viết thay vì nội dung sự việc, đó là bài toán khác và cần guideline/gold version riêng.

## 2. Quy tắc nhãn

| Nhãn | Chọn khi |
|---|---|
| positive | Nội dung chính thể hiện kết quả/lợi ích/cải thiện/thành công rõ, không cần tự suy đoán bên ngoài |
| negative | Nội dung chính thể hiện thiệt hại/rủi ro/thất bại/suy giảm/tổn thất rõ |
| neutral | Cung cấp thông tin/lịch/thay đổi/mô tả mà text không nghiêng rõ về lợi hay hại |
| needs_context | Text thiếu/hỏng/mơ hồ, không đủ căn cứ gán một trong ba nhãn |
| mixed | Hai chiều đáng kể cùng tồn tại, không có trọng tâm đủ rõ để chọn một chiều |
| invalid | Sai ngôn ngữ mục tiêu, menu/HTML rác, không phải bài tin hoặc lỗi extraction |

Ba nhãn đầu mới vào evaluation 3 lớp. Ba trạng thái sau phải được adjudicate hoặc giữ ngoài test chính với lý do/tỷ lệ công bố; **không đổi tất cả thành neutral**.

### Ví dụ do dự án tự viết, chỉ minh họa guideline

| Text minh họa | Nhãn dự kiến | Căn cứ |
|---|---|---|
| “Ca phẫu thuật thành công, bệnh nhân đã hồi phục.” | positive | Kết quả tốt được nêu trực tiếp |
| “Lũ làm hư hại hàng trăm ngôi nhà, nhiều gia đình phải sơ tán.” | negative | Thiệt hại rõ, dù câu viết khách quan |
| “Hội nghị sẽ diễn ra vào thứ Hai tại Đà Nẵng.” | neutral | Thông báo thời gian/địa điểm |
| “Giá xăng tăng 500 đồng một lít.” | neutral | Không tự chọn đối tượng được/mất lợi khi text không nói |
| “Giá xăng tăng khiến chi phí vận chuyển của doanh nghiệp tăng mạnh.” | negative | Hệ quả bất lợi đã được mô tả |
| “Không có thương vong sau vụ cháy gây thiệt hại lớn về tài sản.” | mixed | Hai chiều; không thấy chữ “không” rồi máy móc đảo nhãn |
| “Ông A nói dự án ‘rất thành công’; kết luận kiểm toán chưa được công bố.” | needs_context hoặc mixed | Cần guideline pilot chốt cách xử lý trọng tâm/trích dẫn; không ép nhãn |
| “Điều gì đã xảy ra với dự án này?” | needs_context | Thiếu thông tin xác định hướng |

Các ví dụ này **không** được đưa vào gold thật hoặc count dataset thật.

## 3. Trình tự cho mỗi mẫu

1. Đọc đúng input text được đóng băng, không mở URL để bổ sung kiến thức mà model không có.
2. Kiểm tra usable/đúng tiếng Việt; nếu không, chọn invalid + reason.
3. Xác định sự việc/trọng tâm chính trong title và description, không dựa vào category/source.
4. Chọn nhãn hoặc trạng thái khó; đánh dấu cụm từ làm căn cứ, ghi reason ngắn.
5. Nếu biết sự kiện từ ngoài đời, chỉ dựa thông tin trong mẫu; nếu mẫu không đủ thì needs_context.
6. Không nhìn nhãn annotator khác, prediction/confidence hoặc split test/dev khi gán.

Phủ định, mỉa mai, tiêu đề câu hỏi, trích dẫn và nhiều đối tượng phải được xét theo cả câu. Không quy luật “thể thao = positive”, “pháp luật = negative”, “tin báo khách quan = neutral”.

## 4. Schema packet và nhãn

Packet reviewer giữ metadata:

```text
annotation_id, article_id, content_group_id, event_group_id, dataset_id,
input_type, input_text, input_sha256, source, category, published_at,
sample_seed, sampling_stratum, selection_probability, split, guideline_version
```

View cho annotator chỉ cần annotation_id, input_text, lựa chọn label/status, evidence_span, reason, confidence self-rating. Metadata/split/probability nằm file riêng tránh bias.

Nhãn lưu append-only:

```text
annotation_id, input_sha256, annotator_id, label, reason, evidence_span,
annotated_at, guideline_version
```

Adjudication lưu riêng label_a/label_b/final_label/adjudicator/reason/time. Nếu input text đổi, mẫu phải được xem lại theo hash mới; không giữ nhãn cũ rồi đổi text.

## 5. Pilot, agreement và freeze

- 100 pilot không nằm trong main test; hai người độc lập.
- Tính agreement/Cohen kappa **trước adjudication**, kèm bảng nhầm lẫn và tỷ lệ difficult/invalid.
- Mục tiêu dự án ban đầu agreement ≥80%, kappa ≥0,65; kappa không đứng một mình, phải xem class skew/support.
- Không đạt: thảo luận các kiểu mâu thuẫn, sửa guideline, pilot bổ sung; không chạy main 1.200 trước khi hiểu nhãn giống nhau.
- Main: ưu tiên double-label toàn bộ. Audit lại 10% mẫu đồng ý, adjudicate tất cả bất đồng.
- Final test được giữ bởi owner evaluation; code/model agent chỉ nhận train/dev. Không đánh giá nhiều candidate trên test rồi chọn tốt nhất.
- Thiếu người gán: công bố blocked labeling, có thể triển khai tooling/DQ trước. Không tự điền nhãn bằng AI để hoàn thành mốc.

## 6. Khi dùng kết quả

Tên đúng: “dự đoán sắc thái nội dung title+description theo guideline news_sentiment_gold_v1”. Báo sample support, confusion, macro-F1 và giới hạn. Không gọi đó là dư luận xã hội hoặc đánh giá lập trường tòa soạn.
