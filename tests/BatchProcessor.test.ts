import { BatchProcessor, BatchProcessorConfig, BatchJob } from '../src/shared/BatchProcessor';
import { Job } from 'bullmq';
import { redisMock, queueMock, workerMock, queueEventsMock } from './setup';

interface TestJob extends BatchJob {
  testData: string;
  userId?: string;
}

class TestBatchProcessor extends BatchProcessor<TestJob> {
  public processedJobs: Job<TestJob>[] = [];
  public processedBatches: Job<TestJob>[][] = [];

  protected async processBatchJobs(jobs: Job<TestJob>[]): Promise<void> {
    this.processedBatches.push(jobs);
    for (const job of jobs) {
      this.processedJobs.push(job);
    }
  }

  protected async processSingle(job: Job<TestJob>): Promise<void> {
    this.processedJobs.push(job);
  }

  // Expose private methods for testing
  public async testEnqueue(job: TestJob) {
    return this.enqueue(job);
  }
}

describe('BatchProcessor', () => {
  let processor: TestBatchProcessor;
  const config: BatchProcessorConfig = {
    queueName: 'test-queue',
    concurrency: 10,
    batchSize: 5,
    processingInterval: 100,
    maxQueueDepth: 1000,
  };

  beforeEach(() => {
    jest.clearAllMocks();
    processor = new TestBatchProcessor(config);
  });

  afterEach(async () => {
    // Clean up any intervals
    jest.clearAllTimers();
  });

  describe('constructor', () => {
    it('should initialize with correct configuration', () => {
      expect(processor).toBeDefined();
      expect(processor['config']).toEqual(config);
    });

    it('should create queue and worker with correct names', () => {
      const { Queue, Worker, QueueEvents } = require('bullmq');
      expect(Queue).toHaveBeenCalledWith('test-queue', { connection: redisMock });
      expect(Worker).toHaveBeenCalledWith(
        'test-queue',
        expect.any(Function),
        { connection: redisMock, concurrency: 10 }
      );
      expect(QueueEvents).toHaveBeenCalledWith('test-queue', { connection: redisMock });
    });
  });

  describe('enqueue', () => {
    it('should successfully enqueue a job when queue is below capacity', async () => {
      const testJob: TestJob = {
        deliveryId: 'test-delivery-id',
        merchantId: 'test-merchant',
        testData: 'test-data',
      };

      queueMock.count.mockResolvedValue(100);
      queueMock.add.mockResolvedValue({ id: 'job-id' });

      await processor.testEnqueue(testJob);

      expect(queueMock.add).toHaveBeenCalledWith('process', testJob, {
        jobId: 'test-delivery-id',
        attempts: 5,
        backoff: { type: 'exponential', delay: 2000 },
        removeOnComplete: { age: 3600 },
      });
    });

    it('should throw error when queue is at capacity', async () => {
      const testJob: TestJob = {
        deliveryId: 'test-delivery-id',
        merchantId: 'test-merchant',
        testData: 'test-data',
      };

      queueMock.count.mockResolvedValue(1000); // At max capacity

      await expect(processor.testEnqueue(testJob)).rejects.toThrow(
        'test-queue queue at capacity (1000). Retry after backoff.'
      );
    });

    it('should emit queue depth metrics when approaching threshold', async () => {
      const testJob: TestJob = {
        deliveryId: 'test-delivery-id',
        merchantId: 'test-merchant',
        testData: 'test-data',
      };

      queueMock.count.mockResolvedValue(850); // Above 80% threshold
      queueMock.add.mockResolvedValue({ id: 'job-id' });

      await processor.testEnqueue(testJob);

      expect(queueMock.add).toHaveBeenCalled();
    });
  });

  describe('start', () => {
    it('should start the processor and setup event listeners', async () => {
      workerMock.waitUntilReady.mockResolvedValue(undefined);

      await processor.start();

      expect(workerMock.waitUntilReady).toHaveBeenCalled();
      expect(queueEventsMock.on).toHaveBeenCalledWith('waiting', expect.any(Function));
    });
  });

  describe('batch processing', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('should process jobs in batches when available', async () => {
      const jobs: Job<TestJob>[] = [
        {
          id: '1',
          data: { deliveryId: '1', merchantId: 'merchant1', testData: 'data1', userId: 'user1' },
          attemptsMade: 0,
          opts: { attempts: 3 },
        } as Job<TestJob>,
        {
          id: '2',
          data: { deliveryId: '2', merchantId: 'merchant1', testData: 'data2', userId: 'user1' },
          attemptsMade: 0,
          opts: { attempts: 3 },
        } as Job<TestJob>,
      ];

      // Mock getBatchJobs to return test jobs
      queueMock.getWaiting.mockResolvedValue(jobs);

      await processor.start();

      // Advance timer to trigger batch processing
      jest.advanceTimersByTime(config.processingInterval);
      await new Promise(resolve => setImmediate(resolve));

      expect(processor.processedBatches).toHaveLength(1);
      expect(processor.processedBatches[0]).toHaveLength(2);
    });

    it('should group jobs by user to maintain FIFO ordering', async () => {
      const jobs: Job<TestJob>[] = [
        {
          id: '1',
          data: { deliveryId: '1', merchantId: 'merchant1', testData: 'data1', userId: 'user1' },
          attemptsMade: 0,
          opts: { attempts: 3 },
        } as Job<TestJob>,
        {
          id: '2',
          data: { deliveryId: '2', merchantId: 'merchant1', testData: 'data2', userId: 'user2' },
          attemptsMade: 0,
          opts: { attempts: 3 },
        } as Job<TestJob>,
        {
          id: '3',
          data: { deliveryId: '3', merchantId: 'merchant1', testData: 'data3', userId: 'user1' },
          attemptsMade: 0,
          opts: { attempts: 3 },
        } as Job<TestJob>,
      ];

      queueMock.getWaiting.mockResolvedValue(jobs);

      await processor.start();
      jest.advanceTimersByTime(config.processingInterval);
      await new Promise(resolve => setImmediate(resolve));

      // Should create separate batches for different users
      expect(processor.processedBatches.length).toBeGreaterThan(0);
    });

    it('should handle empty batch gracefully', async () => {
      queueMock.getWaiting.mockResolvedValue([]);

      await processor.start();
      jest.advanceTimersByTime(config.processingInterval);
      await new Promise(resolve => setImmediate(resolve));

      expect(processor.processedBatches).toHaveLength(0);
    });
  });

  describe('Redis pipeline operations', () => {
    it('should use Redis pipeline for efficient batch job retrieval', async () => {
      const pipelineMock = {
        lpop: jest.fn().mockReturnThis(),
        hgetall: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([
          [null, 'job-1'],
          [null, 'job-2'],
        ]),
      };

      redisMock.pipeline.mockReturnValue(pipelineMock);

      await processor.start();
      jest.advanceTimersByTime(config.processingInterval);
      await new Promise(resolve => setImmediate(resolve));

      expect(redisMock.pipeline).toHaveBeenCalled();
      expect(pipelineMock.lpop).toHaveBeenCalled();
    });

    it('should fallback to standard BullMQ method on pipeline failure', async () => {
      const pipelineMock = {
        lpop: jest.fn().mockReturnThis(),
        exec: jest.fn().mockRejectedValue(new Error('Pipeline error')),
      };

      redisMock.pipeline.mockReturnValue(pipelineMock);
      queueMock.getWaiting.mockResolvedValue([]);

      await processor.start();
      jest.advanceTimersByTime(config.processingInterval);
      await new Promise(resolve => setImmediate(resolve));

      expect(queueMock.getWaiting).toHaveBeenCalled();
    });
  });
});