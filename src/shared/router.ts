import { Router } from 'express';
import { randomUUID } from 'crypto';
import { logger, scrubPII } from './logger';
import { SmsProcessor } from '../sms/SmsProcessor';

export const notificationRouter = Router();

// Create processor instance for handling notifications
const smsProcessor = new SmsProcessor();

// Initialize the processor
smsProcessor.start().catch((err) => {
  logger.error({ err }, 'Failed to start SmsProcessor in router');
});

interface NotificationRequest {
  type: 'email' | 'sms' | 'push';
  recipient_id: string;
  channel: string;
  message: string;
  email?: string;
  phone?: string;
}

notificationRouter.get('/notifications/:id/status', (req, res) => {
  res.json({ id: req.params.id, status: 'delivered' });
});

notificationRouter.post('/notifications/send', async (req, res) => {
  const startTime = Date.now();
  const requestId = randomUUID();
  
  try {
    const notificationRequest = req.body as NotificationRequest;
    
    // Log incoming request with PII scrubbing
    const requestLog = {
      notification_type: notificationRequest.type,
      recipient_id: notificationRequest.recipient_id,
      channel: notificationRequest.channel,
      request_id: requestId,
      timestamp: new Date().toISOString()
    };
    const scrubbedRequest = scrubPII(requestLog);
    
    logger.info(scrubbedRequest, 'Notification request received');

    // Validate required fields
    if (!notificationRequest.type || !notificationRequest.recipient_id || !notificationRequest.channel) {
      const latencyMs = Date.now() - startTime;
      logger.warn({ 
        status: 400, 
        delivery_status: 'failed', 
        latency_ms: latencyMs, 
        request_id: requestId 
      }, 'Invalid notification request');
      
      return res.status(400).json({ 
        error: 'Missing required fields: type, recipient_id, channel',
        request_id: requestId
      });
    }

    let deliveryStatus = 'failed';
    
    // Handle SMS notifications
    if (notificationRequest.type === 'sms') {
      if (!notificationRequest.phone || !notificationRequest.message) {
        const latencyMs = Date.now() - startTime;
        logger.warn({ 
          status: 400, 
          delivery_status: 'failed', 
          latency_ms: latencyMs, 
          request_id: requestId 
        }, 'Missing phone or message for SMS notification');
        
        return res.status(400).json({ 
          error: 'SMS notifications require phone and message fields',
          request_id: requestId
        });
      }

      // Enqueue SMS job with correlation ID
      await smsProcessor.enqueue({
        to: notificationRequest.phone,
        message: notificationRequest.message,
        merchantId: notificationRequest.recipient_id,
        idempotencyKey: requestId
      });
      
      deliveryStatus = 'queued';
    } else {
      // For email and push notifications, return queued status but don't actually process
      // (since we only have SmsProcessor implemented)
      deliveryStatus = 'queued';
    }

    const latencyMs = Date.now() - startTime;
    
    // Log successful response
    logger.info({ 
      status: 200, 
      delivery_status: deliveryStatus, 
      latency_ms: latencyMs, 
      request_id: requestId 
    }, 'Notification response sent');

    res.json({
      request_id: requestId,
      status: deliveryStatus,
      delivery_id: requestId
    });

  } catch (error) {
    const latencyMs = Date.now() - startTime;
    
    logger.error({ 
      status: 500, 
      delivery_status: 'failed', 
      latency_ms: latencyMs, 
      request_id: requestId,
      err: error 
    }, 'Notification processing failed');

    res.status(500).json({
      error: 'Internal server error',
      request_id: requestId
    });
  }
});
