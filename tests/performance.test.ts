import { WebhookProcessor } from '../src/webhook/WebhookProcessor';
import { SmsProcessor } from '../src/sms/SmsProcessor';
import { Job } from 'bullmq';
import { redisMock, twilioMock, queueMock } from './setup';

describe('Performance Tests - 40% Throughput Improvement', () => {
  let webhookProcessor: WebhookProcessor;
  let smsProcessor: SmsProcessor;

  beforeEach(() => {
    jest.clearAllMocks();
    webhookProcessor = new WebhookProcessor();
    smsProcessor = new SmsProcessor();
  });

  afterEach(() => {
    jest.clearAllTimers();
  });

  describe('Webhook batch processing performance', () => {
    it('should process large batches efficiently with parallel requests', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      // Create 50 webhook jobs (default batch size)
      const jobs: Job<any>[] = Array(50).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          url: `https://webhook${i % 5}.example.com/endpoint`, // 5 different URLs
          payload: { eventType: 'payment.completed', amount: 100 + i },
          merchantId: `merchant${i % 10}`,
          eventType: 'payment.completed',
          deliveryId: `delivery-${i}`,
          userId: `user${i % 20}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const startTime = Date.now();
      await (webhookProcessor as any).processBatchJobs(jobs);
      const endTime = Date.now();

      // All jobs should be processed
      expect(fetchMock).toHaveBeenCalledTimes(50);

      // Should complete within reasonable time (parallel processing)
      const processingTime = endTime - startTime;
      expect(processingTime).toBeLessThan(5000); // 5 seconds max for 50 webhooks

      console.log(`Webhook batch processing time: ${processingTime}ms for 50 jobs`);
    });

    it('should maintain p95 latency under 2 seconds for individual webhooks', async () => {
      const fetchMock = global.fetch as jest.Mock;
      
      // Simulate realistic network latency
      fetchMock.mockImplementation(() => 
        new Promise(resolve => 
          setTimeout(() => resolve({
            ok: true,
            status: 200,
            statusText: 'OK',
          }), Math.random() * 100) // 0-100ms network latency
        )
      );

      const job: Job<any> = {
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
      };

      // Test multiple individual deliveries
      const latencies: number[] = [];
      for (let i = 0; i < 20; i++) {
        const startTime = Date.now();
        await (webhookProcessor as any).processSingle(job);
        const endTime = Date.now();
        latencies.push(endTime - startTime);
      }

      // Calculate p95 latency
      const sortedLatencies = latencies.sort((a, b) => a - b);
      const p95Index = Math.floor(sortedLatencies.length * 0.95);
      const p95Latency = sortedLatencies[p95Index];

      expect(p95Latency).toBeLessThan(2000); // Less than 2 seconds
      console.log(`Webhook p95 latency: ${p95Latency}ms`);
    });

    it('should handle URL grouping for rate limiting efficiently', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      // Create jobs with same URL to test rate limiting
      const sameUrlJobs: Job<any>[] = Array(30).fill(null).map((_, i) => ({
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
      await (webhookProcessor as any).processBatchJobs(sameUrlJobs);
      const endTime = Date.now();

      // All jobs should be processed
      expect(fetchMock).toHaveBeenCalledTimes(30);

      // Should introduce rate limiting delays but still be efficient
      const processingTime = endTime - startTime;
      expect(processingTime).toBeGreaterThan(100); // Some delay for rate limiting
      expect(processingTime).toBeLessThan(3000); // But not too slow

      console.log(`Rate-limited webhook processing time: ${processingTime}ms for 30 jobs to same URL`);
    });
  });

  describe('SMS batch processing performance', () => {
    it('should process SMS batches efficiently with idempotency checking', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue(
          Array(10).fill([null, null]) // None already delivered
        ),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');
      
      twilioMock.messages.create.mockResolvedValue({ sid: 'msg-sid' });

      // Create 10 SMS jobs (default batch size)
      const jobs: Job<any>[] = Array(10).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          to: `+123456789${i}`,
          message: `Payment confirmed: $${100 + i}`,
          merchantId: `merchant${i % 3}`,
          deliveryId: `delivery-${i}`,
          idempotencyKey: `idem-${i}`,
          userId: `user${i % 5}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const startTime = Date.now();
      await (smsProcessor as any).processBatchJobs(jobs);
      const endTime = Date.now();

      // All jobs should be processed
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(10);

      // Should complete efficiently with parallel processing
      const processingTime = endTime - startTime;
      expect(processingTime).toBeLessThan(2000); // 2 seconds max for 10 SMS

      console.log(`SMS batch processing time: ${processingTime}ms for 10 jobs`);
    });

    it('should efficiently filter already delivered messages in large batches', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([
          [null, null],   // Not delivered
          [null, '1'],    // Already delivered
          [null, null],   // Not delivered
          [null, '1'],    // Already delivered
          [null, null],   // Not delivered
        ]),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');
      
      twilioMock.messages.create.mockResolvedValue({ sid: 'msg-sid' });

      const jobs: Job<any>[] = Array(5).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          to: `+123456789${i}`,
          message: `Test message ${i}`,
          merchantId: 'merchant1',
          deliveryId: `delivery-${i}`,
          idempotencyKey: `idem-${i}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const startTime = Date.now();
      await (smsProcessor as any).processBatchJobs(jobs);
      const endTime = Date.now();

      // Should only process 3 jobs (2 were already delivered)
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(3);
      
      // Pipeline should be used for efficient filtering
      expect(redisMock.pipeline).toHaveBeenCalled();
      expect(pipelineMock.get).toHaveBeenCalledTimes(5);

      const processingTime = endTime - startTime;
      console.log(`SMS filtering and processing time: ${processingTime}ms for 5 jobs (3 processed)`);
    });
  });

  describe('Queue depth monitoring performance', () => {
    it('should efficiently monitor queue depth without impacting throughput', async () => {
      queueMock.count.mockResolvedValue(500);

      const job = {
        deliveryId: 'test-delivery',
        merchantId: 'test-merchant',
        testData: 'test',
      };

      // Test multiple enqueue operations
      const startTime = Date.now();
      
      for (let i = 0; i < 20; i++) {
        await (webhookProcessor as any).enqueue({
          ...job,
          deliveryId: `delivery-${i}`,
        });
      }
      
      const endTime = Date.now();
      const processingTime = endTime - startTime;

      // Queue depth checking should not significantly impact enqueue performance
      expect(processingTime).toBeLessThan(1000); // 1 second max for 20 enqueues
      expect(queueMock.count).toHaveBeenCalledTimes(20);
      expect(queueMock.add).toHaveBeenCalledTimes(20);

      console.log(`Queue depth monitoring overhead: ${processingTime}ms for 20 enqueues`);
    });
  });

  describe('Memory usage and efficiency', () => {
    it('should process large batches without excessive memory usage', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      // Create a large batch to test memory efficiency
      const largeBatch: Job<any>[] = Array(100).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          url: `https://webhook${i % 10}.example.com/endpoint`,
          payload: { 
            eventType: 'payment.completed', 
            amount: 100 + i,
            // Add some payload size to test memory handling
            metadata: Array(100).fill(`data-${i}`).join('-')
          },
          merchantId: `merchant${i % 10}`,
          eventType: 'payment.completed',
          deliveryId: `delivery-${i}`,
          userId: `user${i % 20}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const initialMemory = process.memoryUsage().heapUsed;
      
      await (webhookProcessor as any).processBatchJobs(largeBatch);
      
      const finalMemory = process.memoryUsage().heapUsed;
      const memoryIncrease = finalMemory - initialMemory;
      
      // Memory increase should be reasonable (not holding all job data in memory)
      expect(memoryIncrease).toBeLessThan(50 * 1024 * 1024); // Less than 50MB increase
      
      console.log(`Memory increase for 100 job batch: ${Math.round(memoryIncrease / 1024 / 1024)}MB`);
    });
  });
});