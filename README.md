# 🚀 Demo Apache Kafka với Aiven Cloud — Gửi Tin Nhắn & Giả Lập Gửi Đồng Thời

Bộ source code demo chuyên dụng cho buổi thuyết trình **Seminar SOA - Apache Kafka**, hỗ trợ:
1. **Gửi 1 tin nhắn đơn lẻ** lên cụm Kafka trên **Aiven Cloud**.
2. **Giả lập gửi CÙNG LÚC nhiều tin nhắn (Concurrent Multi-Producer Burst)** bằng cơ chế bất đồng bộ song song (`Promise.all`) và gom lô (`Batch`).
3. **Quan sát trực quan cách Kafka chia tải vào nhiều Partitions (`P0, P1, P2`)** và phân phối cho nhiều **Consumers trong cùng 1 Consumer Group** xử lý song song theo thời gian thực.

---

## 📁 Cấu trúc thư mục

- [`server.js`](file:///e:/baitap/SOA/seminar/server.js) + [`public/index.html`](file:///e:/baitap/SOA/seminar/public/index.html): **Web Dashboard trực quan** (`http://localhost:3000`) để trình chiếu trực tiếp trong buổi Seminar.
- [`cli-producer.js`](file:///e:///baitap/SOA/seminar/cli-producer.js): Kịch bản **Producer chạy trên Terminal** (có menu gửi đơn lẻ hoặc bắn đồng thời 20 / 100 / N tin nhắn cùng lúc).
- [`cli-consumer.js`](file:///e:/baitap/SOA/seminar/cli-consumer.js): Kịch bản **Consumer Worker chạy trên nhiều cửa sổ Terminal** để chứng minh tính phân tán của Consumer Group.
- [`config/kafka.js`](file:///e:/baitap/SOA/seminar/config/kafka.js): Module kết nối **Aiven Kafka** (hỗ trợ cả **SSL Certificates** lẫn **SASL_SSL**).
- [`.env`](file:///e:/baitap/SOA/seminar/.env): File cấu hình thông tin kết nối Aiven.

---

## 🛠️ Bước 1: Lấy thông tin kết nối từ Aiven (`aiven.io`)

1. Đăng nhập vào [Aiven Console](https://console.aiven.io/) và tạo dịch vụ **Apache Kafka** (gói **Free**).
2. Vào mục **Service settings** -> phần **Advanced configuration** -> Bật tùy chọn **`kafka.auto_create_topics_enable` = `true`** (hoặc vào tab **Topics** tạo sẵn topic tên là `seminar-messages-demo` với **`Partitions = 3`**).
3. Ở tab **Overview** -> mục **Connection information**:
   - Copy **Service URI** (ví dụ: `kafka-1234abcd-myproject.aivencloud.com:12345`).
   - Tải 3 file chứng chỉ SSL về máy và bỏ vào thư mục [`./certs/`](file:///e:/baitap/SOA/seminar/certs/README.txt):
     - **CA Certificate** -> lưu tên là `ca.pem`
     - **Access Certificate** -> lưu tên là `service.cert`
     - **Access Key** -> lưu tên là `service.key`
4. Mở file [`.env`](file:///e:/baitap/SOA/seminar/.env) và điền địa chỉ `KAFKA_BROKER=kafka-xxxx.aivencloud.com:12345`.

> 💡 **Mẹo cực tiện lợi:** Khi dùng **Web Dashboard**, bạn không cần chép file thủ công mà có thể bấm nút **"⚙️ Cấu hình Aiven Kafka"** ngay trên trình duyệt để chọn trực tiếp 3 file `ca.pem`, `service.cert`, `service.key` từ máy tính!

---

## 🖥️ Bước 2: Chạy Demo

### Cách 1: Demo bằng Giao diện Web Trực Quan (Khuyên dùng khi thuyết trình)

```bash
npm start
```
Sau đó mở trình duyệt tại: **`http://localhost:3000`**

- Tại giao diện Web, bạn có thể:
  - Kết nối trực tiếp tới **Aiven Cloud** (hoặc bật **Chế độ Mô phỏng** nếu mạng phòng học chặn cổng Kafka).
  - Bấm gửi 1 tin nhắn hoặc kéo thanh trượt **Bắn đồng thời 15 / 50 / 100 / 200 tin nhắn cùng lúc** từ nhiều Producers.
  - Quan sát thanh phần trăm phân bổ vào **Partition 0, 1, 2** và **Consumer 1, 2, 3** nhảy số theo thời gian thực.

---

### Cách 2: Demo bằng nhiều cửa sổ Terminal (CLI)

Mở **3 cửa sổ Terminal** cho 3 Consumer cùng thuộc nhóm `seminar-consumer-group`:
```bash
# Terminal 1:
node cli-consumer.js Worker-1

# Terminal 2:
node cli-consumer.js Worker-2

# Terminal 3:
node cli-consumer.js Worker-3
```

Mở **Terminal thứ 4** để chạy Producer:
```bash
node cli-producer.js
```
Chọn phím `[2]` hoặc `[3]` để bắn đồng thời 20 hoặc 100 tin nhắn cùng một lúc và quan sát cả 3 cửa sổ Consumer chia nhau xử lý!
