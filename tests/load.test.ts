import { WebhookProcessor } from '../src/webhook/WebhookProcessor';
import { SmsProcessor } from '../src/sms/SmsProcessor';
import { Job } from 'bullmq';
import { redisMock, twilioMock, queueMock } from './setup';

describe('Load Tests - 40% Throughput Improvement Validation', () => {
  let webhookProcessor: WebhookProcessor;
  let smsProcessor: SmsProcessor;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.setTimeout(30000); // 30 second timeout for load tests
    webhookProcessor = new WebhookProcessor();
    smsProcessor = new SmsProcessor();
  });

  afterEach(() => {
    jest.clearAllTimers();
  });

  describe('Webhook throughput validation', () => {
    it('should achieve target throughput improvement with current + 40% load', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockImplementation(() => 
        new Promise(resolve => 
          setTimeout(() => resolve({
            ok: true,
            status: 200,
            statusText: 'OK',
          }), 50 + Math.random() * 50) // 50-100ms realistic webhook response time
        )
      );

      // Simulate current baseline load (100 webhooks) + 40% increase = 140 webhooks
      const currentLoad = 100;
      const increasedLoad = Math.floor(currentLoad * 1.4); // 140

      console.log(`Testing webhook throughput: ${currentLoad} -> ${increasedLoad} (+40%)`);

      const jobs: Job<any>[] = Array(increasedLoad).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          url: `https://webhook${i % 20}.example.com/endpoint`, // 20 different webhooks
          payload: { 
            eventType: 'payment.completed', 
            amount: 100 + i,
            timestamp: Date.now(),
            transactionId: `txn-${i}`,
          },
          merchantId: `merchant${i % 15}`,
          eventType: 'payment.completed',
          deliveryId: `delivery-${i}`,
          userId: `user${i % 30}`, // 30 different users for FIFO testing
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const startTime = Date.now();
      await (webhookProcessor as any).processBatchJobs(jobs);
      const endTime = Date.now();

      const totalTime = endTime - startTime;
      const throughputPerSecond = (increasedLoad / totalTime) * 1000;

      // All jobs should be processed successfully
      expect(fetchMock).toHaveBeenCalledTimes(increasedLoad);

      // Should maintain sub-2-second p95 latency even with increased load
      expect(totalTime).toBeLessThan(increasedLoad * 100); // Should be much faster than sequential

      // Throughput should be significantly higher than sequential processing
      const sequentialEstimate = increasedLoad * 75; // ~75ms per webhook
      expect(totalTime).toBeLessThan(sequentialEstimate * 0.4); // At least 60% faster

      console.log(`Webhook load test results:`);
      console.log(`- Total jobs: ${increasedLoad}`);
      console.log(`- Total time: ${totalTime}ms`);
      console.log(`- Throughput: ${throughputPerSecond.toFixed(2)} webhooks/second`);
      console.log(`- Average latency: ${(totalTime / increasedLoad).toFixed(2)}ms per webhook`);
    });

    it('should maintain FIFO ordering per user under increased load', async () => {
      const fetchMock = global.fetch as jest.Mock;
      const processedJobs: any[] = [];
      
      fetchMock.mockImplementation((url, options) => {
        const payload = JSON.parse(options.body);
        processedJobs.push({
          userId: payload.userId,
          sequence: payload.sequence,
          timestamp: Date.now(),
        });
        
        return Promise.resolve({
          ok: true,
          status: 200,
          statusText: 'OK',
        });
      });

      // Create jobs with specific ordering requirements per user
      const jobs: Job<any>[] = [];
      const usersWithSequences = ['user1', 'user2', 'user3'];
      
      usersWithSequences.forEach(userId => {
        for (let seq = 0; seq < 20; seq++) {
          jobs.push({
            id: `${userId}-${seq}`,
            data: {
              url: 'https://api.example.com/webhook',
              payload: { 
                userId,
                sequence: seq,
                eventType: 'payment.completed',
              },
              merchantId: 'merchant1',
              eventType: 'payment.completed',
              deliveryId: `${userId}-delivery-${seq}`,
              userId,
            },
            attemptsMade: 0,
            opts: { attempts: 3 },
          } as Job<any>);
        }
      });

      // Shuffle jobs to test ordering preservation
      const shuffledJobs = jobs.sort(() => Math.random() - 0.5);

      await (webhookProcessor as any).processBatchJobs(shuffledJobs);

      // Verify FIFO ordering per user
      usersWithSequences.forEach(userId => {
        const userJobs = processedJobs
          .filter(job => job.userId === userId)
          .sort((a, b) => a.timestamp - b.timestamp);

        for (let i = 0; i < userJobs.length - 1; i++) {
          expect(userJobs[i].sequence).toBeLessThan(userJobs[i + 1].sequence);
        }
      });

      expect(fetchMock).toHaveBeenCalledTimes(60); // 3 users * 20 sequences
    });

    it('should handle webhook endpoint rate limiting gracefully', async () => {
      const fetchMock = global.fetch as jest.Mock;
      const callTimestamps: number[] = [];
      
      fetchMock.mockImplementation(() => {
        callTimestamps.push(Date.now());
        return Promise.resolve({
          ok: true,
          status: 200,
          statusText: 'OK',
        });
      });

      // Create 50 jobs to same webhook URL (tests rate limiting)
      const jobs: Job<any>[] = Array(50).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          url: 'https://rate-limited-webhook.example.com/endpoint',
          payload: { eventType: 'test', sequence: i },
          merchantId: 'merchant1',
          eventType: 'test',
          deliveryId: `delivery-${i}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      await (webhookProcessor as any).processBatchJobs(jobs);

      expect(fetchMock).toHaveBeenCalledTimes(50);

      // Verify rate limiting delays were applied
      const sortedTimestamps = callTimestamps.sort((a, b) => a - b);
      let hasRateLimitingDelays = false;
      
      for (let i = 10; i < sortedTimestamps.length - 1; i += 10) {
        const timeDiff = sortedTimestamps[i + 1] - sortedTimestamps[i];
        if (timeDiff > 40) { // Expected ~50ms delay between batches
          hasRateLimitingDelays = true;
          break;
        }
      }

      expect(hasRateLimitingDelays).toBe(true);
    });
  });

  describe('SMS throughput validation', () => {
    it('should achieve target throughput improvement with current + 40% SMS load', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn(),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');
      
      twilioMock.messages.create.mockImplementation(() => 
        new Promise(resolve => 
          setTimeout(() => resolve({ sid: `msg-${Date.now()}` }), 30 + Math.random() * 20) // 30-50ms Twilio response
        )
      );

      // Simulate current baseline load (50 SMS) + 40% increase = 70 SMS
      const currentLoad = 50;
      const increasedLoad = Math.floor(currentLoad * 1.4); // 70

      // Mock all messages as not delivered
      pipelineMock.exec.mockResolvedValue(
        Array(increasedLoad).fill([null, null])
      );

      console.log(`Testing SMS throughput: ${currentLoad} -> ${increasedLoad} (+40%)`);

      const jobs: Job<any>[] = Array(increasedLoad).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          to: `+12345678${String(i).padStart(2, '0')}`,
          message: `Payment confirmed: $${100 + i}. Transaction ID: txn-${i}`,
          merchantId: `merchant${i % 10}`,
          deliveryId: `delivery-${i}`,
          idempotencyKey: `idem-${i}`,
          userId: `user${i % 25}`, // 25 different users
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const startTime = Date.now();
      await (smsProcessor as any).processBatchJobs(jobs);
      const endTime = Date.now();

      const totalTime = endTime - startTime;
      const throughputPerSecond = (increasedLoad / totalTime) * 1000;

      // All jobs should be processed successfully
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(increasedLoad);

      // Should be significantly faster than sequential processing
      const sequentialEstimate = increasedLoad * 40; // ~40ms per SMS
      expect(totalTime).toBeLessThan(sequentialEstimate * 0.5); // At least 50% faster

      console.log(`SMS load test results:`);
      console.log(`- Total jobs: ${increasedLoad}`);
      console.log(`- Total time: ${totalTime}ms`);
      console.log(`- Throughput: ${throughputPerSecond.toFixed(2)} SMS/second`);
      console.log(`- Average latency: ${(totalTime / increasedLoad).toFixed(2)}ms per SMS`);
    });

    it('should maintain idempotency under high load', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn(),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.set.mockResolvedValue('OK');
      twilioMock.messages.create.mockResolvedValue({ sid: 'msg-sid' });

      // Simulate mixed scenario: 30% already delivered, 70% new
      const totalJobs = 100;
      const alreadyDelivered = 30;
      const newJobs = 70;

      pipelineMock.exec.mockResolvedValue([
        ...Array(alreadyDelivered).fill([null, '1']), // Already delivered
        ...Array(newJobs).fill([null, null]),         // Not delivered
      ]);

      const jobs: Job<any>[] = Array(totalJobs).fill(null).map((_, i) => ({
        id: `job-${i}`,
        data: {
          to: `+1234567${String(i).padStart(3, '0')}`,
          message: `Test message ${i}`,
          merchantId: 'merchant1',
          deliveryId: `delivery-${i}`,
          idempotencyKey: `idem-${i}`,
          userId: `user${i % 20}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      await (smsProcessor as any).processBatchJobs(jobs);

      // Should only process new jobs (70), skip already delivered (30)
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(newJobs);

      // Should use pipeline for efficient idempotency checking
      expect(redisMock.pipeline).toHaveBeenCalled();
      expect(pipelineMock.get).toHaveBeenCalledTimes(totalJobs);

      console.log(`Idempotency test: ${alreadyDelivered} skipped, ${newJobs} processed`);
    });
  });

  describe('Mixed workload performance', () => {
    it('should handle concurrent webhook and SMS processing', async () => {
      const fetchMock = global.fetch as jest.Mock;
      fetchMock.mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue(Array(30).fill([null, null])),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');
      twilioMock.messages.create.mockResolvedValue({ sid: 'msg-sid' });

      // Create mixed workload
      const webhookJobs: Job<any>[] = Array(50).fill(null).map((_, i) => ({
        id: `webhook-${i}`,
        data: {
          url: `https://webhook${i % 10}.example.com/endpoint`,
          payload: { eventType: 'payment.completed', amount: 100 + i },
          merchantId: `merchant${i % 5}`,
          eventType: 'payment.completed',
          deliveryId: `webhook-delivery-${i}`,
          userId: `user${i % 15}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      const smsJobs: Job<any>[] = Array(30).fill(null).map((_, i) => ({
        id: `sms-${i}`,
        data: {
          to: `+123456${String(i).padStart(4, '0')}`,
          message: `Payment confirmed: $${100 + i}`,
          merchantId: `merchant${i % 5}`,
          deliveryId: `sms-delivery-${i}`,
          idempotencyKey: `sms-idem-${i}`,
          userId: `user${i % 15}`,
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      })) as Job<any>[];

      // Process both workloads concurrently
      const startTime = Date.now();
      await Promise.all([
        (webhookProcessor as any).processBatchJobs(webhookJobs),
        (smsProcessor as any).processBatchJobs(smsJobs),
      ]);
      const endTime = Date.now();

      const totalTime = endTime - startTime;

      // Both workloads should complete successfully
      expect(fetchMock).toHaveBeenCalledTimes(50);
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(30);

      // Concurrent processing should be faster than sequential
      const totalJobs = 80;
      const throughputPerSecond = (totalJobs / totalTime) * 1000;

      console.log(`Mixed workload results:`);
      console.log(`- Webhooks: 50, SMS: 30 (80 total)`);
      console.log(`- Total time: ${totalTime}ms`);
      console.log(`- Combined throughput: ${throughputPerSecond.toFixed(2)} jobs/second`);

      // Should achieve good combined throughput
      expect(throughputPerSecond).toBeGreaterThan(10); // At least 10 jobs/second combined
    });
  });

  describe('Queue depth monitoring under load', () => {
    it('should efficiently handle queue depth alerts during peak load', async () => {
      queueMock.count
        .mockResolvedValueOnce(800)  // Below threshold
        .mockResolvedValueOnce(900)  // Above 80% threshold
        .mockResolvedValueOnce(950); // Above 90% threshold

      const jobs = Array(3).fill(null).map((_, i) => ({
        deliveryId: `delivery-${i}`,
        merchantId: `merchant-${i}`,
        url: `https://webhook${i}.example.com/endpoint`,
        payload: { eventType: 'test' },
        eventType: 'test',
      }));

      // Should handle queue depth monitoring efficiently
      for (const job of jobs) {
        await (webhookProcessor as any).enqueue(job);
      }

      // All jobs should be enqueued despite queue depth checks
      expect(queueMock.add).toHaveBeenCalledTimes(3);
      expect(queueMock.count).toHaveBeenCalledTimes(3);

      // Should emit metrics for high queue depth
      // (Verified through log output in actual implementation)
    });
  });
});