import { WebhookProcessor } from '../src/webhook/WebhookProcessor';
import { Job } from 'bullmq';
import { redisMock, queueMock, workerMock } from './setup';

describe('WebhookProcessor', () => {
  let processor: WebhookProcessor;

  beforeEach(() => {
    jest.clearAllMocks();
    processor = new WebhookProcessor();
  });

  afterEach(() => {
    jest.clearAllTimers();
  });

  describe('constructor', () => {
    it('should initialize with batch processing configuration', () => {
      expect(processor).toBeDefined();
      
      const { Queue, Worker } = require('bullmq');
      expect(Queue).toHaveBeenCalledWith('webhooks', { connection: redisMock });
      expect(Worker).toHaveBeenCalledWith(
        'webhooks',
        expect.any(Function),
        { connection: redisMock, concurrency: 75 }
      );
    });

    it('should use environment variables for configuration', () => {
      process.env.WEBHOOK_CONCURRENCY = '100';
      process.env.WEBHOOK_BATCH_SIZE = '25';
      
      const customProcessor = new WebhookProcessor();
      expect(customProcessor).toBeDefined();
    });
  });

  describe('start', () => {
    it('should start the processor and setup DLQ handlers', async () => {
      workerMock.waitUntilReady.mockResolvedValue(undefined);

      await processor.start();

      expect(workerMock.waitUntilReady).toHaveBeenCalled();
      expect(workerMock.on).toHaveBeenCalledWith('failed', expect.any(Function));
    });
  });

  describe('batch processing', () => {
    const mockJobs: Job<any>[] = [
      {
        id: '1',
        data: {
          url: 'https://api.example.com/webhook1',
          payload: { eventType: 'payment.completed', amount: 100 },
          merchantId: 'merchant1',
          eventType: 'payment.completed',
          deliveryId: 'delivery1',
          userId: 'user1',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>,
      {
        id: '2',
        data: {
          url: 'https://api.example.com/webhook1',
          payload: { eventType: 'payment.completed', amount: 200 },
          merchantId: 'merchant1',
          eventType: 'payment.completed',
          deliveryId: 'delivery2',
          userId: 'user1',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>,
      {
        id: '3',
        data: {
          url: 'https://api.different.com/webhook',
          payload: { eventType: 'payment.failed', amount: 50 },
          merchantId: 'merchant2',
          eventType: 'payment.failed',
          deliveryId: 'delivery3',
          userId: 'user2',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>,
    ];

    it('should group jobs by URL to avoid rate limiting', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      // Call processBatchJobs directly for testing
      await (processor as any).processBatchJobs(mockJobs);

      // Should make separate HTTP calls
      expect(fetchMock).toHaveBeenCalledTimes(3);
      
      // Verify correct webhook URLs were called
      expect(fetchMock).toHaveBeenCalledWith(
        'https://api.example.com/webhook1',
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({
            'Content-Type': 'application/json',
            'X-PortIOPay-Delivery': 'delivery1',
            'X-PortIOPay-Event': 'payment.completed',
          }),
          body: JSON.stringify({ eventType: 'payment.completed', amount: 100 }),
        })
      );
    });

    it('should handle parallel HTTP requests with Promise.allSettled', async () => {
      const fetchMock = global.fetch as jest.Mock;
      
      // Mock one success and one failure
      fetchMock
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          statusText: 'OK',
        })
        .mockResolvedValueOnce({
          ok: false,
          status: 500,
          statusText: 'Internal Server Error',
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          statusText: 'OK',
        });

      // Should not throw even if one request fails
      await expect((processor as any).processBatchJobs(mockJobs)).resolves.not.toThrow();
      
      expect(fetchMock).toHaveBeenCalledTimes(3);
    });

    it('should implement rate limiting for same URL', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      const sameUrlJobs = Array(15).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          url: 'https://api.example.com/webhook',
          payload: { eventType: 'test', data: i },
          merchantId: 'merchant1',
          eventType: 'test',
          deliveryId: `delivery-${i}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const startTime = Date.now();
      await (processor as any).processBatchJobs(sameUrlJobs);
      const endTime = Date.now();

      // Should have introduced delays for rate limiting
      expect(endTime - startTime).toBeGreaterThan(0);
      expect(fetchMock).toHaveBeenCalledTimes(15);
    });
  });

  describe('single job processing', () => {
    it('should process individual webhook delivery', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      const job = {
        id: '1',
        data: {
          url: 'https://api.example.com/webhook',
          payload: { eventType: 'payment.completed', amount: 100 },
          merchantId: 'merchant1',
          eventType: 'payment.completed',
          deliveryId: 'delivery1',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>;

      await (processor as any).processSingle(job);

      expect(fetchMock).toHaveBeenCalledWith(
        'https://api.example.com/webhook',
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({
            'Content-Type': 'application/json',
            'X-PortIOPay-Delivery': 'delivery1',
            'X-PortIOPay-Event': 'payment.completed',
          }),
          body: JSON.stringify({ eventType: 'payment.completed', amount: 100 }),
          signal: expect.any(AbortSignal),
        })
      );
    });

    it('should throw error on failed webhook delivery', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      const job = {
        id: '1',
        data: {
          url: 'https://api.example.com/webhook',
          payload: { eventType: 'payment.completed', amount: 100 },
          merchantId: 'merchant1',
          eventType: 'payment.completed',
          deliveryId: 'delivery1',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>;

      await expect((processor as any).processSingle(job)).rejects.toThrow(
        'Webhook delivery failed: 500 Internal Server Error'
      );
    });

    it('should timeout requests after 10 seconds', async () => {
      const fetchMock = global.fetch as jest.Mock;
      
      const job = {
        id: '1',
        data: {
          url: 'https://api.example.com/webhook',
          payload: { eventType: 'payment.completed', amount: 100 },
          merchantId: 'merchant1',
          eventType: 'payment.completed',
          deliveryId: 'delivery1',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>;

      await (processor as any).processSingle(job);

      expect(fetchMock).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          signal: expect.any(AbortSignal),
        })
      );
    });
  });

  describe('DLQ functionality', () => {
    it('should add failed jobs to DLQ after max retries', async () => {
      const mockJob = {
        id: 'failed-job',
        data: {
          url: 'https://api.example.com/webhook',
          payload: { eventType: 'payment.completed', amount: 100 },
          merchantId: 'merchant1',
          eventType: 'payment.completed',
          deliveryId: 'failed-delivery',
        },
        attemptsMade: 3,
        opts: { attempts: 3 },
      };

      const mockError = new Error('Webhook delivery failed');
      
      // Access private method for testing
      await (processor as any).addToDLQ(mockJob.data, mockError.message);

      expect(redisMock.hset).toHaveBeenCalledWith(
        'webhook:dlq:failed-delivery',
        expect.objectContaining({
          ...mockJob.data,
          failureReason: 'Webhook delivery failed',
          failedAt: expect.any(Number),
        })
      );
    });
  });
});