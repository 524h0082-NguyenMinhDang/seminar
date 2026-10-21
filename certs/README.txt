HƯỚNG DẪN ĐẶT CHỨNG CHỈ AIVEN KAFKA VÀO THƯ MỤC NÀY (./certs)
================================================================

1. Truy cập https://console.aiven.io -> Chọn dịch vụ Apache Kafka của bạn.
2. Ở tab "Overview" -> Mục "Connection information":
   - Tải "CA Certificate" -> lưu vào thư mục này với tên: ca.pem
   - Tải "Access Certificate" -> lưu vào thư mục này với tên: service.cert
   - Tải "Access Key" -> lưu vào thư mục này với tên: service.key
3. Mở file .env ở thư mục gốc và thay KAFKA_BROKER bằng Service URI của bạn (VD: kafka-1234abcd-myproj.aivencloud.com:12345).
