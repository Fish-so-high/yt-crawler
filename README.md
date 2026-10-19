# yt-crawler — Crawler, lịch cào, quản lý hạn mức và Replay

Phần việc của vai trò **DE: Crawler và Replay**. Đầu ra: comment YouTube đúng schema đã chốt, đẩy vào Kafka topic `raw_comments` (khóa `video_id`) và lưu archive JSONL trong `data/raw/` làm nguồn cho Replay.

## 1. Chuẩn bị (làm 1 lần)

1. **API key**: vào https://console.cloud.google.com → tạo project → *APIs & Services → Library* → bật **YouTube Data API v3** → *Credentials → Create credentials → API key*. Nên *Restrict key* chỉ cho YouTube Data API v3. Không cần OAuth vì chỉ đọc comment công khai.
2. **Môi trường** (Python 3.11+, Docker):
   ```bash
   python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   cp .env.example .env        # điền YOUTUBE_API_KEY, AUTHOR_SALT
   docker compose -f docker-compose.dev.yml up -d        # Kafka 1 node để dev (bỏ qua nếu chưa cần Kafka)
   docker exec kafka-dev /opt/kafka/bin/kafka-topics.sh --create --topic raw_comments \
       --partitions 3 --bootstrap-server localhost:9092
   ```
   Để trống `KAFKA_BOOTSTRAP` trong `.env` thì crawler chỉ ghi JSONL, vẫn dùng được.
3. **AUTHOR_SALT**: cả nhóm dùng chung một giá trị, đặt 1 lần, không commit. Đổi muối giữa chừng sẽ làm `author_hash` không khớp nhau.

## 2. Quy trình chạy

```bash
# (a) Chốt kênh: tìm ứng viên (mỗi lần search tốn 100 đơn vị, mặc định tối đa 3 lần)
python -m src.main find-channels "review <tên máy>" "so sánh <máy A> <máy B>"
#     → chọn 10-20 kênh, điền vào config/config.yaml (channels), điền models + aliases + launch_date

# (b) Tìm video (đi qua playlist uploads của kênh, KHÔNG dùng search.list)
python -m src.main discover
python -m src.main status       # xem số video mỗi mẫu máy → chốt mục còn mở "≥ 20 video review"

# (c) Cào thử 1 lần, rồi chạy tự động
python -m src.main crawl
python -m src.main snapshot-videos      # chụp view/like/comment của video
python -m src.main run          # scheduler: discover 6h, snapshot video 7h (giờ VN), crawl mỗi 30 phút

# (d) Replay cho demo / đo hệ thống
python -m src.main replay --speed 10 --dry-run          # in thử, không đẩy Kafka
python -m src.main replay --speed 1   --model main      # đo độ trễ p50/p95 ở 1x
python -m src.main replay --speed 10                    # đo consumer lag ở 10x
python -m src.main replay --speed 10 --strip-stats      # demo: bỏ like/reply của "tương lai"
```
Chạy bền (không tắt khi đóng terminal): `nohup python -m src.main run > crawler.log 2>&1 &` hoặc dùng `tmux`.

## 3. Schema v2 (nguồn sự thật: `src/schema.py`)

**`comments`** (Kafka `raw_comments`, Mongo `comments`; khóa upsert là `comment_id`)

| Trường | Kiểu | Ghi chú |
|---|---|---|
| comment_id | string | khóa chính; consumer ghi Mongo bằng **upsert** theo trường này để xử lý lại không sinh bản sao |
| video_id | string | khóa Kafka |
| parent_id | string/null | luôn trỏ về comment gốc (YouTube chỉ có 2 tầng; trả lời ai nằm ở `@tên` trong text) |
| parent_text | string/null | **mới.** Nội dung comment gốc, làm ngữ cảnh cho reply, đỡ phải join trong Spark |
| model | string | mặc định theo video: `main` / `rival` / `both`. Luật nhận diện tên máy trong comment để ghi đè là việc của Data Science |
| source | string | **mới.** Hiện luôn là `youtube` |
| text, text_norm | string | `text_norm` do Data Science ghi |
| author_hash | string | SHA-256(salt + channel ID tác giả) |
| published_at | datetime UTC | thời gian sự kiện |
| like_count | int | số like **tại `fetched_at`** |
| reply_count | int/null | **mới.** `totalReplyCount`, chỉ có ở comment gốc |
| fetched_at | datetime UTC | **mới.** Lúc API trả số liệu |
| ingested_at | datetime UTC | lúc vào Kafka (Replay ghi đè), dùng đo độ trễ |
| schema_version | int | **mới.** Hiện là 2 |

**`videos`** (Mongo `videos`, file `data/raw/videos-YYYYMMDD.jsonl`, mỗi lần chụp là một dòng): video_id, channel_id, channel_title, title, published_at, model, video_type (heuristic từ tiêu đề: review/unbox/compare/long_term), source, view_count, like_count, comment_count, fetched_at, schema_version. Dùng để chuẩn hóa (ví dụ comment trên 1.000 lượt xem), không gắn vào từng comment. Dislike đã bị ẩn khỏi API.

Lưu ý khi dùng: `like_count` và `reply_count` là ảnh chụp lúc cào lần đầu, crawler không cập nhật lại. Khi phát lại dữ liệu cũ, hai số này là số của "tương lai" so với thời điểm phát; demo dùng `replay --strip-stats` hoặc chỉ dùng chúng cho bảng xếp hạng cuối kỳ.

## 4. Hạn mức (quota)

| Phương thức | Chi phí | Dùng để |
|---|---|---|
| `commentThreads.list` | 1 / trang 100 comment | cào comment chính |
| `comments.list` | 1 / trang | lấy đủ reply của một comment |
| `playlistItems.list`, `videos.list`, `channels.list` | 1 | tìm video, lấy commentCount, giải mã kênh |
| `search.list` | **100** | chỉ để tìm kênh ứng viên |

Mặc định 10.000 đơn vị/ngày/project, reset lúc **nửa đêm giờ Thái Bình Dương** (≈ 14h hoặc 15h giờ VN tùy mùa). Ước tính: 20.000 comment ≈ 200-400 đơn vị; discover 20 kênh < 100 đơn vị. Hạn mức dư rất nhiều **nếu tránh `search.list`**.

Cơ chế trong code (`src/quota.py`):
- Đếm đơn vị **trước** mỗi request (request lỗi cũng tốn ≥ 1), lưu SQLite nên restart không mất.
- 3 lớp chặn: ngưỡng ngày (85% của 10.000), ngân sách mỗi lần chạy (600), trần số lần `search.list`/ngày (80).
- Gặp lỗi `quotaExceeded` từ API thì đánh dấu hết hạn mức cả ngày PT.
- Crawl dừng giữa chừng được: lưu `page_token` sau mỗi trang, lần sau làm tiếp; `seen` chống trùng.

Không tạo nhiều project để lách hạn mức (vi phạm điều khoản). Nếu thiếu, điền form xin tăng quota của Google.

## 5. Các điểm cần báo lại nhóm

- **Schema**: video so sánh (nhắc cả 2 máy) được gán `model = "both"`, ngoài `main`/`rival` như schema chốt. Cần thống nhất với Team Data Science: bỏ, hay để NLP tự suy ra máy nào theo từng mệnh đề.
- `text` lấy từ `textDisplay` (plain text) vì API key không đọc được `textOriginal`. Có thể còn `@tên` ở đầu reply.
- `ingested_at` do Sink điền lúc ghi, Replay ghi đè khi phát lại; `fetched_at` giữ nguyên.
- Video có commentCount = 0 vẫn được cào (video mới đăng); chỉ video tắt bình luận mới bị loại.
- Giới hạn: theo dõi live chỉ bắt comment cấp 1 mới; reply mới dưới comment cũ sẽ không được bắt. Với đề tài này chấp nhận được.
- Chính sách YouTube API có yêu cầu về lưu trữ/làm mới dữ liệu; nên đọc YouTube API Services Developer Policies trước khi công bố bộ dữ liệu.
