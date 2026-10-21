const express = require('express');
const cors = require('cors');
const path = require('path');
const { getDefaultConfig, validateConfig, createKafkaClient } = require('./config/kafka');

const app = express();
app.use(cors());
app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Trạng thái kết nối Kafka hiện tại
let runtimeConfig = getDefaultConfig();
let kafkaClient = null;
let producer = null;
let activeConsumers = []; // Danh sách các consumer instance đang chạy trong Consumer Group
let isConnected = false;
let connectionMode = 'disconnected'; // 'aiven' | 'simulation' | 'disconnected'

// Danh sách các SSE clients (trình duyệt đang mở Dashboard)
const sseClients = new Set();

function broadcastEvent(eventType, data) {
  const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    res.write(payload);
  }
}

// Danh sách nội dung mẫu khi bắn hàng loạt tin nhắn cùng lúc
const BURST_TEMPLATES = [
  'Đặt hàng Flash Sale #iPhone16',
  'Thanh toán QR VNPAY thành công',
  'Tải lên phân đoạn Video 4K (Chunk)',
  'Yêu cầu xuất báo cáo tài chính',
  'Gửi SMS OTP xác thực giao dịch',
  'Cập nhật tọa độ GPS tài xế',
  'Phát hiện đăng nhập thiết bị mới',
  'Đồng bộ tồn kho từ kho tổng',
];

/**
 * Ngắt kết nối toàn bộ Producer & Consumers hiện tại
 */
async function disconnectAll() {
  if (producer) {
    try {
      await producer.disconnect();
    } catch {}
    producer = null;
  }
  for (const c of activeConsumers) {
    try {
      await c.instance.disconnect();
    } catch {}
  }
  activeConsumers = [];
  kafkaClient = null;
  isConnected = false;
  connectionMode = 'disconnected';
}

/**
 * Khởi tạo cụm Consumer Group (ví dụ 3 Consumers chạy song song để nhận tin nhắn từ 3 Partitions)
 */
async function startConsumerGroup(numWorkers = 3) {
  for (const c of activeConsumers) {
    try {
      await c.instance.disconnect();
    } catch {}
  }
  activeConsumers = [];

  if (connectionMode === 'simulation') {
    for (let i = 1; i <= numWorkers; i++) {
      activeConsumers.push({
        id: `Consumer-${i}`,
        assignedPartitions: [i - 1],
        processedCount: 0,
      });
    }
    broadcastEvent('status', getStatusSummary());
    return;
  }

  // Chế độ kết nối thật tới Aiven Kafka — Khởi chạy song song các Consumer để Join Group cùng lúc
  const startPromises = [];
  for (let i = 1; i <= numWorkers; i++) {
    const workerId = `Consumer-${i}`;
    const consumer = kafkaClient.consumer({
      groupId: runtimeConfig.groupId,
      sessionTimeout: 30000,
    });

    const workerMeta = {
      id: workerId,
      instance: consumer,
      assignedPartitions: [i - 1],
      processedCount: 0,
    };
    activeConsumers.push(workerMeta);

    const p = (async () => {
      await consumer.connect();
      await consumer.subscribe({ topic: runtimeConfig.topic, fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          workerMeta.processedCount++;
          if (!workerMeta.assignedPartitions.includes(partition)) {
            workerMeta.assignedPartitions.push(partition);
          }

          let parsed = {};
          const raw = message.value ? message.value.toString() : '{}';
          try {
            parsed = JSON.parse(raw);
          } catch {
            parsed = { content: raw, sender: 'Producer', id: message.key?.toString() };
          }

          const receivedAt = Date.now();
          const sentAt = parsed.sentAtMs || receivedAt;
          const e2eLatencyMs = Math.max(1, receivedAt - sentAt);

          broadcastEvent('message_consumed', {
            workerId,
            topic,
            partition,
            offset: message.offset,
            key: message.key ? message.key.toString() : null,
            payload: parsed,
            e2eLatencyMs,
            receivedAt: new Date(receivedAt).toISOString(),
            workerProcessedCount: workerMeta.processedCount,
          });
        },
      });
    })();
    startPromises.push(p);
  }

  await Promise.all(startPromises);
  broadcastEvent('status', getStatusSummary());
}

function getStatusSummary() {
  const validation = validateConfig(runtimeConfig);
  return {
    isConnected,
    connectionMode,
    broker: runtimeConfig.broker,
    authMethod: runtimeConfig.authMethod,
    topic: runtimeConfig.topic,
    partitions: runtimeConfig.partitions,
    groupId: runtimeConfig.groupId,
    configValid: validation.valid,
    configIssues: validation.issues,
    consumers: activeConsumers.map((c) => ({
      id: c.id,
      assignedPartitions: c.assignedPartitions,
      processedCount: c.processedCount,
    })),
  };
}

// ==========================================================
// API ENDPOINTS
// ==========================================================

// 1. Stream sự kiện Real-time về giao diện Web (Server-Sent Events)
app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  sseClients.add(res);
  res.write(`event: status\ndata: ${JSON.stringify(getStatusSummary())}\n\n`);

  req.on('close', () => {
    sseClients.delete(res);
  });
});

// 2. Lấy trạng thái hiện tại
app.get('/api/status', (req, res) => {
  res.json(getStatusSummary());
});

// 3. Kết nối tới Aiven Kafka (hoặc bật chế độ Giả lập nếu chưa có mạng/cert)
app.post('/api/connect', async (req, res) => {
  try {
    await disconnectAll();
    runtimeConfig = getDefaultConfig();

    const {
      mode = 'aiven',
      broker,
      authMethod,
      saslUsername,
      saslPassword,
      saslMechanism,
      caPemContent,
      certContent,
      keyContent,
      topic,
      partitions,
      groupId,
      numConsumers = 2,
    } = req.body || {};

    if (broker && !broker.includes('kafka-xxxx')) {
      runtimeConfig.broker = broker.trim();
    }
    if (authMethod) runtimeConfig.authMethod = authMethod;
    if (saslUsername) runtimeConfig.saslUsername = saslUsername.trim();
    if (saslPassword) runtimeConfig.saslPassword = saslPassword;
    if (saslMechanism) runtimeConfig.saslMechanism = saslMechanism;
    if (caPemContent) runtimeConfig.caPemContent = caPemContent;
    if (certContent) runtimeConfig.certContent = certContent;
    if (keyContent) runtimeConfig.keyContent = keyContent;
    if (topic) runtimeConfig.topic = topic.trim();
    if (partitions) runtimeConfig.partitions = parseInt(partitions, 10) || 2;
    if (groupId) runtimeConfig.groupId = groupId.trim();

    if (mode === 'simulation') {
      isConnected = true;
      connectionMode = 'simulation';
      await startConsumerGroup(parseInt(numConsumers, 10) || 2);
      broadcastEvent('system_log', {
        level: 'info',
        message: `Đã bật chế độ Mô phỏng Kafka (${runtimeConfig.partitions} Partitions, ${numConsumers} Consumers).`,
      });
      return res.json({ ok: true, status: getStatusSummary() });
    }

    // Kết nối thật tới Aiven Cloud
    kafkaClient = createKafkaClient(runtimeConfig);
    producer = kafkaClient.producer();
    await producer.connect();

    // Đảm bảo Topic tồn tại và tự động tương thích với giới hạn của Aiven Free Tier (tối đa 2 Partitions)
    try {
      const admin = kafkaClient.admin();
      await admin.connect();
      const existing = await admin.listTopics();
      if (!existing.includes(runtimeConfig.topic)) {
        for (const tryParts of [runtimeConfig.partitions, 2, 1]) {
          try {
            await admin.createTopics({
              topics: [{ topic: runtimeConfig.topic, numPartitions: tryParts }],
            });
            if (tryParts === 1 && runtimeConfig.partitions >= 2) {
              await admin
                .createPartitions({
                  topicPartitions: [{ topic: runtimeConfig.topic, count: 2 }],
                })
                .catch(() => {});
            }
            break;
          } catch {}
        }
      }
      const meta = await admin.fetchTopicMetadata({ topics: [runtimeConfig.topic] });
      const actualParts = meta.topics?.[0]?.partitions?.length;
      if (actualParts) {
        runtimeConfig.partitions = actualParts;
      }
      await admin.disconnect();
    } catch (adminErr) {
      broadcastEvent('system_log', {
        level: 'warn',
        message: `Lưu ý kiểm tra topic: ${adminErr.message}`,
      });
    }

    isConnected = true;
    connectionMode = 'aiven';

    await startConsumerGroup(runtimeConfig.partitions);

    broadcastEvent('system_log', {
      level: 'success',
      message: `Đã kết nối thành công tới Aiven Kafka (${runtimeConfig.broker}) với ${runtimeConfig.partitions} Partitions!`,
    });

    res.json({ ok: true, status: getStatusSummary() });
  } catch (err) {
    await disconnectAll();
    runtimeConfig = getDefaultConfig();
    res.status(400).json({
      ok: false,
      error: err.message,
      issues: err.validationIssues || [],
    });
  }
});

// 4. Gửi 1 tin nhắn đơn lẻ
app.post('/api/send-single', async (req, res) => {
  if (!isConnected) {
    return res.status(400).json({ ok: false, error: 'Chưa kết nối tới Aiven Kafka (hoặc chế độ Mô phỏng).' });
  }

  try {
    const { content = 'Tin nhắn thử nghiệm từ Producer', sender = 'Producer-1', key = '' } = req.body || {};
    const now = Date.now();
    const msgId = `MSG-${now.toString().slice(-5)}-${Math.floor(Math.random() * 90 + 10)}`;
    const messageKey = key.trim() || msgId;

    const payload = {
      id: msgId,
      seq: 1,
      total: 1,
      sender,
      type: 'SINGLE',
      content,
      sentAtMs: now,
      timestamp: new Date(now).toISOString(),
    };

    if (connectionMode === 'simulation') {
      const partition = Math.floor(Math.random() * runtimeConfig.partitions);
      const offset = String(Math.floor(Math.random() * 10000));
      broadcastEvent('message_produced', {
        id: msgId,
        sender,
        partition,
        offset,
        content,
        latencyMs: 12,
        type: 'SINGLE',
      });

      setTimeout(() => {
        const worker =
          activeConsumers[partition % Math.max(1, activeConsumers.length)] || {
            id: 'Consumer-1',
            processedCount: 1,
          };
        worker.processedCount = (worker.processedCount || 0) + 1;
        broadcastEvent('message_consumed', {
          workerId: worker.id,
          topic: runtimeConfig.topic,
          partition,
          offset,
          key: messageKey,
          payload,
          e2eLatencyMs: Math.floor(Math.random() * 25 + 15),
          receivedAt: new Date().toISOString(),
          workerProcessedCount: worker.processedCount,
        });
      }, 60);

      return res.json({ ok: true, partition, offset, id: msgId });
    }

    // Gửi thật lên Aiven Kafka
    const startMs = Date.now();
    const result = await producer.send({
      topic: runtimeConfig.topic,
      messages: [
        {
          key: messageKey,
          value: JSON.stringify(payload),
        },
      ],
    });
    const latencyMs = Date.now() - startMs;
    const meta = result[0];

    broadcastEvent('message_produced', {
      id: msgId,
      sender,
      partition: meta.partition,
      offset: meta.baseOffset,
      content,
      latencyMs,
      type: 'SINGLE',
    });

    res.json({
      ok: true,
      id: msgId,
      partition: meta.partition,
      offset: meta.baseOffset,
      latencyMs,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// 5. Giả lập gửi CÙNG LÚC nhiều tin nhắn (Concurrent Multi-Producer Burst)
app.post('/api/send-concurrent', async (req, res) => {
  if (!isConnected) {
    return res.status(400).json({ ok: false, error: 'Chưa kết nối tới Aiven Kafka (hoặc chế độ Mô phỏng).' });
  }

  try {
    const {
      count = 30,
      virtualProducers = 5,
      customPrefix = '',
      sendMode = 'parallel_requests', // 'parallel_requests' (Promise.all) hoặc 'single_batch'
    } = req.body || {};

    const totalMessages = Math.min(Math.max(2, parseInt(count, 10) || 30), 500);
    const numProducers = Math.min(Math.max(1, parseInt(virtualProducers, 10) || 5), 20);
    const batchId = `BATCH-${Date.now().toString().slice(-4)}`;
    const startTime = Date.now();
    const partitionDistribution = {};

    broadcastEvent('burst_started', {
      batchId,
      totalMessages,
      numProducers,
      sendMode,
      startedAt: new Date(startTime).toISOString(),
    });

    if (connectionMode === 'simulation') {
      const tasks = Array.from({ length: totalMessages }, (_, idx) => {
        const i = idx + 1;
        const sender = `Producer-${((i - 1) % numProducers) + 1}`;
        const template = customPrefix
          ? `${customPrefix} #${i}`
          : `${BURST_TEMPLATES[i % BURST_TEMPLATES.length]} (#${i}/${totalMessages})`;
        const partition = (i - 1) % runtimeConfig.partitions;
        partitionDistribution[partition] = (partitionDistribution[partition] || 0) + 1;
        const msgId = `${batchId}-${String(i).padStart(3, '0')}`;
        const offset = String(1000 + i);
        const now = Date.now();
        const payload = {
          id: msgId,
          batchId,
          seq: i,
          total: totalMessages,
          sender,
          type: 'CONCURRENT_BURST',
          content: template,
          sentAtMs: now,
          timestamp: new Date(now).toISOString(),
        };

        return new Promise((resolve) => {
          const delayProd = Math.floor(Math.random() * 80 + 10);
          setTimeout(() => {
            broadcastEvent('message_produced', {
              id: msgId,
              sender,
              partition,
              offset,
              content: template,
              latencyMs: delayProd,
              type: 'CONCURRENT_BURST',
            });

            setTimeout(() => {
              const worker =
                activeConsumers[partition % Math.max(1, activeConsumers.length)] || {
                  id: 'Consumer-1',
                  processedCount: 1,
                };
              worker.processedCount = (worker.processedCount || 0) + 1;
              broadcastEvent('message_consumed', {
                workerId: worker.id,
                topic: runtimeConfig.topic,
                partition,
                offset,
                key: msgId,
                payload,
                e2eLatencyMs: delayProd + Math.floor(Math.random() * 40 + 15),
                receivedAt: new Date().toISOString(),
                workerProcessedCount: worker.processedCount,
              });
              resolve();
            }, Math.floor(Math.random() * 120 + 30));
          }, delayProd);
        });
      });

      await Promise.all(tasks);
      const elapsedMs = Date.now() - startTime;
      const summary = {
        ok: true,
        batchId,
        totalMessages,
        numProducers,
        elapsedMs,
        throughputPerSec: Number(((totalMessages / Math.max(1, elapsedMs)) * 1000).toFixed(1)),
        partitionDistribution,
      };
      broadcastEvent('burst_completed', summary);
      return res.json(summary);
    }

    // ==========================================================
    // GỬI ĐỒNG THỜI LÊN AIVEN KAFKA THẬT
    // ==========================================================
    if (sendMode === 'single_batch') {
      // Gom tất cả tin nhắn gửi trong 1 lần gọi batch duy nhất
      const messages = Array.from({ length: totalMessages }, (_, idx) => {
        const i = idx + 1;
        const sender = `Producer-${((i - 1) % numProducers) + 1}`;
        const msgId = `${batchId}-${String(i).padStart(3, '0')}`;
        const content = customPrefix
          ? `${customPrefix} #${i}`
          : `${BURST_TEMPLATES[i % BURST_TEMPLATES.length]} (#${i}/${totalMessages})`;
        const now = Date.now();
        return {
          key: msgId,
          value: JSON.stringify({
            id: msgId,
            batchId,
            seq: i,
            total: totalMessages,
            sender,
            type: 'CONCURRENT_BURST',
            content,
            sentAtMs: now,
            timestamp: new Date(now).toISOString(),
          }),
        };
      });

      const results = await producer.send({
        topic: runtimeConfig.topic,
        messages,
      });

      const elapsedMs = Date.now() - startTime;
      results.forEach((r) => {
        partitionDistribution[r.partition] = (partitionDistribution[r.partition] || 0) + 1;
      });

      broadcastEvent('message_produced', {
        id: batchId,
        sender: `${numProducers} Producers (Batch)`,
        partition: 'Multi',
        offset: results[0]?.baseOffset || '0',
        content: `Đã bắn 1 Batch gồm ${totalMessages} tin nhắn cùng lúc`,
        latencyMs: elapsedMs,
        type: 'CONCURRENT_BURST',
      });
    } else {
      // Khởi tạo N luồng gửi bất đồng bộ CÙNG MỘT LÚC (Promise.all)
      const concurrentPromises = Array.from({ length: totalMessages }, (_, idx) => {
        const i = idx + 1;
        const sender = `Producer-${((i - 1) % numProducers) + 1}`;
        const msgId = `${batchId}-${String(i).padStart(3, '0')}`;
        const content = customPrefix
          ? `${customPrefix} #${i}`
          : `${BURST_TEMPLATES[i % BURST_TEMPLATES.length]} (#${i}/${totalMessages})`;
        const msgStart = Date.now();
        const payload = {
          id: msgId,
          batchId,
          seq: i,
          total: totalMessages,
          sender,
          type: 'CONCURRENT_BURST',
          content,
          sentAtMs: msgStart,
          timestamp: new Date(msgStart).toISOString(),
        };

        return producer
          .send({
            topic: runtimeConfig.topic,
            messages: [
              {
                key: msgId,
                value: JSON.stringify(payload),
              },
            ],
          })
          .then((resMeta) => {
            const meta = resMeta[0];
            const latencyMs = Date.now() - msgStart;
            partitionDistribution[meta.partition] = (partitionDistribution[meta.partition] || 0) + 1;

            broadcastEvent('message_produced', {
              id: msgId,
              sender,
              partition: meta.partition,
              offset: meta.baseOffset,
              content,
              latencyMs,
              type: 'CONCURRENT_BURST',
            });
            return meta;
          });
      });

      await Promise.all(concurrentPromises);
    }

    const elapsedMs = Date.now() - startTime;
    const summary = {
      ok: true,
      batchId,
      totalMessages,
      numProducers,
      elapsedMs,
      throughputPerSec: Number(((totalMessages / Math.max(1, elapsedMs)) * 1000).toFixed(1)),
      partitionDistribution,
    };

    broadcastEvent('burst_completed', summary);
    res.json(summary);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log('============================================================');
  console.log(`🚀 Aiven Kafka Seminar Dashboard đang chạy tại: http://localhost:${PORT}`);
  console.log('============================================================');

  // Tự động thử kết nối nếu file .env và certs đã được người dùng điền sẵn
  const check = validateConfig(runtimeConfig);
  if (check.valid) {
    try {
      console.log(`🔌 Phát hiện cấu hình Aiven hợp lệ (${runtimeConfig.broker}), đang tự động kết nối...`);
      kafkaClient = createKafkaClient(runtimeConfig);
      producer = kafkaClient.producer();
      await producer.connect();

      const admin = kafkaClient.admin();
      await admin.connect();
      const existing = await admin.listTopics();
      if (!existing.includes(runtimeConfig.topic)) {
        await admin.createTopics({
          topics: [{ topic: runtimeConfig.topic, numPartitions: 1 }],
        });
        await admin
          .createPartitions({
            topicPartitions: [{ topic: runtimeConfig.topic, count: 2 }],
          })
          .catch(() => {});
      }
      const meta = await admin.fetchTopicMetadata({ topics: [runtimeConfig.topic] });
      const actualParts = meta.topics?.[0]?.partitions?.length;
      if (actualParts) runtimeConfig.partitions = actualParts;
      await admin.disconnect();

      isConnected = true;
      connectionMode = 'aiven';
      await startConsumerGroup(runtimeConfig.partitions);
      console.log(`✅ Tự động kết nối Aiven Kafka thành công! (Topic: ${runtimeConfig.topic}, Partitions: ${runtimeConfig.partitions})`);
    } catch (e) {
      console.warn('⚠️ Chưa thể kết nối tự động tới Aiven:', e.message);
    }
  } else {
    console.log('ℹ️  Bạn có thể cấu hình Aiven trực tiếp trên giao diện Web http://localhost:' + PORT);
  }
});
