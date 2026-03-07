import { SmsProcessor } from '../src/sms/SmsProcessor';
import { Job } from 'bullmq';
import { redisMock, twilioMock, queueMock, workerMock } from './setup';

describe('SmsProcessor', () => {
  let processor: SmsProcessor;

  beforeEach(() => {
    jest.clearAllMocks();
    processor = new SmsProcessor();
  });

  afterEach(() => {
    jest.clearAllTimers();
  });

  describe('constructor', () => {
    it('should initialize with batch processing configuration', () => {
      expect(processor).toBeDefined();
      
      const { Queue, Worker } = require('bullmq');
      expect(Queue).toHaveBeenCalledWith('sms', { connection: redisMock });
      expect(Worker).toHaveBeenCalledWith(
        'sms',
        expect.any(Function),
        { connection: redisMock, concurrency: 15 }
      );
    });

    it('should initialize Twilio client', () => {
      const twilioModule = require('twilio');
      expect(twilioModule).toHaveBeenCalledWith(
        'test-account-sid',
        'test-auth-token'
      );
    });

    it('should use environment variables for configuration', () => {
      process.env.SMS_CONCURRENCY = '20';
      process.env.SMS_BATCH_SIZE = '15';
      
      const customProcessor = new SmsProcessor();
      expect(customProcessor).toBeDefined();
    });
  });

  describe('start', () => {
    it('should start the processor', async () => {
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
          to: '+1234567890',
          message: 'Payment confirmed: $100',
          merchantId: 'merchant1',
          deliveryId: 'delivery1',
          idempotencyKey: 'idem1',
          userId: 'user1',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>,
      {
        id: '2',
        data: {
          to: '+1987654321',
          message: 'Payment confirmed: $200',
          merchantId: 'merchant1',
          deliveryId: 'delivery2',
          idempotencyKey: 'idem2',
          userId: 'user1',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>,
      {
        id: '3',
        data: {
          to: '+1555666777',
          message: 'Payment failed',
          merchantId: 'merchant2',
          deliveryId: 'delivery3',
          idempotencyKey: 'idem3',
          userId: 'user2',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      } as Job<any>,
    ];

    it('should filter out already delivered messages in batch', async () => {
      // Mock pipeline for checking delivered messages
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([
          [null, null],      // idem1 not delivered
          [null, '1'],       // idem2 already delivered
          [null, null],      // idem3 not delivered
        ]),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null); // Not delivered on single checks
      redisMock.set.mockResolvedValue('OK');

      await (processor as any).processBatchJobs(mockJobs);

      // Should only process 2 jobs (skip the already delivered one)
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(2);
      
      expect(twilioMock.messages.create).toHaveBeenCalledWith({
        to: '+1234567890',
        from: '+1234567890',
        body: 'Payment confirmed: $100',
      });

      expect(twilioMock.messages.create).toHaveBeenCalledWith({
        to: '+1555666777',
        from: '+1234567890',
        body: 'Payment failed',
      });
    });

    it('should use Redis pipeline for efficient delivery checking', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([
          [null, null],
          [null, null],
          [null, null],
        ]),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');

      await (processor as any).processBatchJobs(mockJobs);

      expect(redisMock.pipeline).toHaveBeenCalled();
      expect(pipelineMock.get).toHaveBeenCalledTimes(3);
      expect(pipelineMock.exec).toHaveBeenCalled();
    });

    it('should handle all delivered messages gracefully', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([
          [null, '1'],  // All already delivered
          [null, '1'],
          [null, '1'],
        ]),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);

      await (processor as any).processBatchJobs(mockJobs);

      expect(twilioMock.messages.create).not.toHaveBeenCalled();
    });

    it('should handle partial batch failures gracefully', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([
          [null, null],
          [null, null],
          [null, null],
        ]),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');

      // Mock Twilio to fail for second message
      twilioMock.messages.create
        .mockResolvedValueOnce({ sid: 'msg-1' })
        .mockRejectedValueOnce(new Error('Twilio error'))
        .mockResolvedValueOnce({ sid: 'msg-3' });

      // Should not throw even if some messages fail
      await expect((processor as any).processBatchJobs(mockJobs)).resolves.not.toThrow();
      
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(3);
    });

    it('should log batch processing results', async () => {
      const pipelineMock = {
        get: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([
          [null, null],
          [null, null],
          [null, null],
        ]),
      };
      
      redisMock.pipeline.mockReturnValue(pipelineMock);
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');

      // Mock one success, one failure
      twilioMock.messages.create
        .mockResolvedValueOnce({ sid: 'msg-1' })
        .mockRejectedValueOnce(new Error('Twilio error'))
        .mockResolvedValueOnce({ sid: 'msg-3' });

      await (processor as any).processBatchJobs(mockJobs);

      // Verify processing was attempted for all non-delivered jobs
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(3);
    });
  });

  describe('single job processing', () => {
    const mockJob: Job<any> = {
      id: '1',
      data: {
        to: '+1234567890',
        message: 'Payment confirmed: $100',
        merchantId: 'merchant1',
        deliveryId: 'delivery1',
        idempotencyKey: 'idem1',
      },
      attemptsMade: 0,
      opts: { attempts: 3 },
    };

    it('should process individual SMS when not already delivered', async () => {
      redisMock.get.mockResolvedValue(null); // Not delivered
      redisMock.set.mockResolvedValue('OK');
      twilioMock.messages.create.mockResolvedValue({ sid: 'msg-1' });

      await (processor as any).processSingle(mockJob);

      expect(redisMock.get).toHaveBeenCalledWith('sms:delivered:idem1');
      expect(twilioMock.messages.create).toHaveBeenCalledWith({
        to: '+1234567890',
        from: '+1234567890',
        body: 'Payment confirmed: $100',
      });
      expect(redisMock.set).toHaveBeenCalledWith('sms:delivered:idem1', '1', 'EX', 86400);
    });

    it('should skip SMS if already delivered', async () => {
      redisMock.get.mockResolvedValue('1'); // Already delivered

      await (processor as any).processSingle(mockJob);

      expect(redisMock.get).toHaveBeenCalledWith('sms:delivered:idem1');
      expect(twilioMock.messages.create).not.toHaveBeenCalled();
      expect(redisMock.set).not.toHaveBeenCalled();
    });

    it('should mark message as delivered with TTL', async () => {
      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');
      twilioMock.messages.create.mockResolvedValue({ sid: 'msg-1' });

      await (processor as any).processSingle(mockJob);

      expect(redisMock.set).toHaveBeenCalledWith(
        'sms:delivered:idem1',
        '1',
        'EX',
        86400 // 24 hours TTL
      );
    });

    it('should throw error on Twilio failure', async () => {
      redisMock.get.mockResolvedValue(null);
      twilioMock.messages.create.mockRejectedValue(new Error('Twilio error'));

      await expect((processor as any).processSingle(mockJob)).rejects.toThrow('Twilio error');
      
      // Should not mark as delivered on failure
      expect(redisMock.set).not.toHaveBeenCalled();
    });
  });

  describe('idempotency', () => {
    it('should maintain idempotency across batch and single processing', async () => {
      const job: Job<any> = {
        id: '1',
        data: {
          to: '+1234567890',
          message: 'Test message',
          merchantId: 'merchant1',
          deliveryId: 'delivery1',
          idempotencyKey: 'test-key',
        },
        attemptsMade: 0,
        opts: { attempts: 3 },
      };

      redisMock.get.mockResolvedValue(null);
      redisMock.set.mockResolvedValue('OK');
      twilioMock.messages.create.mockResolvedValue({ sid: 'msg-1' });

      // Process first time
      await (processor as any).processSingle(job);
      
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(1);
      expect(redisMock.set).toHaveBeenCalledWith('sms:delivered:test-key', '1', 'EX', 86400);

      // Process second time (simulate retry)
      redisMock.get.mockResolvedValue('1'); // Now marked as delivered
      
      await (processor as any).processSingle(job);
      
      // Should not send again
      expect(twilioMock.messages.create).toHaveBeenCalledTimes(1);
    });
  });
});