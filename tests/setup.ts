import { jest } from '@jest/globals';

// Mock Redis
const redisMock = {
  pipeline: jest.fn(() => ({
    lpop: jest.fn().mockReturnThis(),
    hgetall: jest.fn().mockReturnThis(),
    exec: jest.fn().mockResolvedValue([] as any),
  })),
  get: jest.fn(),
  set: jest.fn(),
  hset: jest.fn(),
  hgetall: jest.fn(),
  duplicate: jest.fn().mockReturnThis(),
  options: {
    host: 'localhost',
    port: 6379,
  },
};

jest.mock('../src/shared/redis', () => ({
  redis: redisMock,
}));

// Mock Twilio
const twilioMock = {
  messages: {
    create: jest.fn().mockResolvedValue({ sid: 'test-message-sid' } as any),
  },
};

jest.mock('twilio', () => jest.fn(() => twilioMock));

// Mock BullMQ
const jobMock = {
  id: 'test-job-id',
  data: {},
  attemptsMade: 0,
  opts: { attempts: 3 },
};

const queueMock = {
  add: jest.fn().mockResolvedValue(jobMock as any),
  count: jest.fn().mockResolvedValue(0 as any),
  getWaiting: jest.fn().mockResolvedValue([] as any),
};

const workerMock = {
  on: jest.fn(),
  waitUntilReady: jest.fn().mockResolvedValue(undefined as any),
};

const queueEventsMock = {
  on: jest.fn(),
};

jest.mock('bullmq', () => ({
  Queue: jest.fn(() => queueMock),
  Worker: jest.fn(() => workerMock),
  QueueEvents: jest.fn(() => queueEventsMock),
}));

// Mock fetch for webhook tests
global.fetch = jest.fn().mockResolvedValue({
  ok: true,
  status: 200,
  statusText: 'OK',
} as any) as jest.Mock;

// Setup environment variables for tests
process.env.WEBHOOK_BATCH_SIZE = '10';
process.env.SMS_BATCH_SIZE = '5';
process.env.WEBHOOK_CONCURRENCY = '20';
process.env.SMS_CONCURRENCY = '10';
process.env.TWILIO_ACCOUNT_SID = 'test-account-sid';
process.env.TWILIO_AUTH_TOKEN = 'test-auth-token';
process.env.TWILIO_FROM_NUMBER = '+1234567890';

export { redisMock, twilioMock, queueMock, workerMock, queueEventsMock };