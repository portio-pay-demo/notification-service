import { Router } from 'express';
import { z } from 'zod';
import { randomUUID } from 'crypto';
import { logger } from './logger';

export const notificationRouter = Router();

const notificationRequestSchema = z.object({
  notification_type: z.string(),
  recipient_id: z.string(),
  channel: z.enum(['email', 'sms', 'push']),
  message: z.string().optional(),
  request_id: z.string().optional(),
});

type NotificationRequest = z.infer<typeof notificationRequestSchema>;

interface NotificationResponse {
  request_id: string;
  status: 'queued' | 'processing' | 'delivered' | 'failed';
  delivery_id: string;
}

notificationRouter.post('/notifications/send', async (req, res) => {
  const startTime = Date.now();
  let request_id: string = '';
  
  try {
    // Validate request body
    const validationResult = notificationRequestSchema.safeParse(req.body);
    
    if (!validationResult.success) {
      const latency_ms = Date.now() - startTime;
      logger.error({
        status: 'validation_failed',
        latency_ms,
        errors: validationResult.error.errors
      }, 'Request validation failed');
      
      return res.status(400).json({
        error: 'Invalid request body',
        details: validationResult.error.errors
      });
    }

    const { notification_type, recipient_id, channel, message, request_id: provided_request_id } = validationResult.data;
    
    // Auto-generate request_id if not provided
    request_id = provided_request_id || randomUUID();
    
    // Log incoming request
    logger.info({
      notification_type,
      recipient_id,
      channel,
      request_id
    }, 'Notification request received');

    // Generate delivery_id for tracking
    const delivery_id = randomUUID();
    
    // Queue notification based on channel
    // TODO: Integrate with processors when BullMQ/Redis configuration is working
    // For now, simulate queueing to demonstrate logging functionality
    try {
      if (channel === 'sms') {
        logger.info({ 
          request_id,
          channel: 'sms',
          recipient_id,
          merchantId: 'system'
        }, 'Would enqueue SMS notification');
        // await smsProcessor.enqueue({ ... });
      } else if (channel === 'email' || channel === 'push') {
        logger.info({
          request_id,
          channel,
          recipient_id,
          eventType: `notification.${channel}`
        }, `Would enqueue ${channel} notification`);
        // await webhookProcessor.enqueue({ ... });
      }
    } catch (processorError) {
      const latency_ms = Date.now() - startTime;
      logger.warn({
        request_id,
        status: 'queue_failed',
        latency_ms,
        error: processorError instanceof Error ? processorError.message : 'Queue failed'
      }, 'Failed to queue notification, will retry');
      
      return res.status(503).json({
        error: 'Service temporarily unavailable',
        request_id,
        message: 'Notification queue is at capacity, please retry'
      });
    }
    
    const response: NotificationResponse = {
      request_id,
      status: 'queued',
      delivery_id
    };
    
    const latency_ms = Date.now() - startTime;
    
    // Log successful response
    logger.info({
      request_id,
      status: 'queued',
      delivery_status: 'queued',
      latency_ms
    }, 'Notification queued successfully');
    
    res.json(response);
    
  } catch (error) {
    const latency_ms = Date.now() - startTime;
    
    logger.error({
      request_id: request_id || 'unknown',
      status: 'error',
      latency_ms,
      error: error instanceof Error ? error.message : 'Unknown error'
    }, 'Notification request failed');
    
    res.status(500).json({
      error: 'Internal server error',
      request_id: request_id || 'unknown'
    });
  }
});

notificationRouter.get('/notifications/:id/status', (req, res) => {
  res.json({ id: req.params.id, status: 'delivered' });
});
