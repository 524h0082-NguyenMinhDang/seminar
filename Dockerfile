FROM node:20-alpine

# Tạo thư mục làm việc trong Container
WORKDIR /app

# Copy file khai báo thư viện và cài đặt trước (tối ưu Docker Cache)
COPY package*.json ./
RUN npm install --omit=dev

# Copy toàn bộ mã nguồn vào Container
COPY config ./config
COPY public ./public
COPY certs ./certs
COPY server.js cli-producer.js cli-consumer.js ./
COPY .env.example ./.env.example

# Mở cổng 3000 cho Web Dashboard
EXPOSE 3000

# Lệnh khởi chạy mặc định khi bật Container
CMD ["node", "server.js"]
