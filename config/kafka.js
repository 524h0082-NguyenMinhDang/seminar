const fs = require('fs');
const path = require('path');
const { Kafka, logLevel } = require('kafkajs');
require('dotenv').config();

/**
 * Đọc cấu hình từ biến môi trường (.env) hoặc ghi đè từ giao diện Web
 */
function getDefaultConfig() {
  return {
    broker: process.env.KAFKA_BROKER || 'kafka-xxxx-project.aivencloud.com:12345',
    authMethod: (process.env.KAFKA_AUTH_METHOD || 'ssl').toLowerCase(),
    caPath: process.env.KAFKA_CA_PATH || './certs/ca.pem',
    certPath: process.env.KAFKA_CERT_PATH || './certs/service.cert',
    keyPath: process.env.KAFKA_KEY_PATH || './certs/service.key',
    saslUsername: process.env.KAFKA_SASL_USERNAME || 'avnadmin',
    saslPassword: process.env.KAFKA_SASL_PASSWORD || '',
    saslMechanism: process.env.KAFKA_SASL_MECHANISM || 'scram-sha-256',
    topic: process.env.KAFKA_TOPIC || 'seminar-messages-demo',
    partitions: parseInt(process.env.KAFKA_PARTITIONS || '2', 10),
    groupId: process.env.KAFKA_GROUP_ID || 'seminar-consumer-group',
  };
}

/**
 * Kiểm tra xem cấu hình Aiven đã được thiết lập đầy đủ chưa
 */
function validateConfig(cfg = getDefaultConfig()) {
  const issues = [];
  if (!cfg.broker || cfg.broker.includes('kafka-xxxx')) {
    issues.push('Chưa cấu hình KAFKA_BROKER (Service URI từ Aiven).');
  }

  const resolveFile = (p) => path.resolve(process.cwd(), p);

  if (cfg.authMethod === 'ssl') {
    if (!cfg.caPemContent && !fs.existsSync(resolveFile(cfg.caPath))) {
      issues.push(`Thiếu file CA Certificate tại: ${cfg.caPath}`);
    }
    if (!cfg.certContent && !fs.existsSync(resolveFile(cfg.certPath))) {
      issues.push(`Thiếu file Access Certificate tại: ${cfg.certPath}`);
    }
    if (!cfg.keyContent && !fs.existsSync(resolveFile(cfg.keyPath))) {
      issues.push(`Thiếu file Access Key tại: ${cfg.keyPath}`);
    }
  } else if (cfg.authMethod === 'sasl') {
    if (!cfg.caPemContent && !fs.existsSync(resolveFile(cfg.caPath))) {
      issues.push(`Thiếu file CA Certificate tại: ${cfg.caPath}`);
    }
    if (!cfg.saslUsername || !cfg.saslPassword || cfg.saslPassword === 'your_aiven_password_here') {
      issues.push('Chưa nhập Username / Password cho SASL.');
    }
  }

  return {
    valid: issues.length === 0,
    issues,
  };
}

/**
 * Khởi tạo đối tượng Kafka client kết nối tới Aiven Cloud
 */
function createKafkaClient(customConfig = {}) {
  const cfg = { ...getDefaultConfig(), ...customConfig };
  const validation = validateConfig(cfg);

  if (!validation.valid) {
    const err = new Error('Cấu hình Aiven Kafka chưa hoàn tất:\n- ' + validation.issues.join('\n- '));
    err.validationIssues = validation.issues;
    throw err;
  }

  const resolveFile = (p) => path.resolve(process.cwd(), p);
  const ca = cfg.caPemContent || fs.readFileSync(resolveFile(cfg.caPath), 'utf-8');

  let sslConfig = {
    rejectUnauthorized: true,
    ca: [ca],
  };

  let saslConfig = undefined;

  if (cfg.authMethod === 'ssl') {
    const cert = cfg.certContent || fs.readFileSync(resolveFile(cfg.certPath), 'utf-8');
    const key = cfg.keyContent || fs.readFileSync(resolveFile(cfg.keyPath), 'utf-8');
    sslConfig.cert = cert;
    sslConfig.key = key;
  } else if (cfg.authMethod === 'sasl') {
    saslConfig = {
      mechanism: cfg.saslMechanism || 'scram-sha-256',
      username: cfg.saslUsername,
      password: cfg.saslPassword,
    };
  }

  const kafkaOptions = {
    clientId: cfg.clientId || 'aiven-seminar-demo',
    brokers: [cfg.broker.trim()],
    ssl: sslConfig,
    connectionTimeout: 10000,
    authenticationTimeout: 10000,
    logLevel: logLevel.WARN,
  };

  if (saslConfig) {
    kafkaOptions.sasl = saslConfig;
  }

  return new Kafka(kafkaOptions);
}

module.exports = {
  getDefaultConfig,
  validateConfig,
  createKafkaClient,
};
