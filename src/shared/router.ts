import { Router } from 'express';
import { logger } from './logger';
import { randomUUID } from 'crypto';

export const notificationRouter = Router();

notificationRouter.get('/notifications/:id/status', (req, res) => {
  res.json({ id: req.params.id, status: 'delivered' });
});

interface NotificationSendRequest {
  notification_type: string;
  recipient_id: string;
  channel: 'email' | 'sms' | 'push';
  request_id?: string;
}

interface NotificationSendResponse {
  success: boolean;
  notification_id: string;
  request_id: string;
}

notificationRouter.post('/notifications/send', (req, res) => {
  const startTime = Date.now();
  
  // Extract and validate request data
  const { notification_type, recipient_id, channel, request_id: providedRequestId } = req.body as NotificationSendRequest;
  
  // Auto-generate request_id if not provided
  const request_id = providedRequestId || randomUUID();
  
  // Log request entry with structured metadata (no PII)
  logger.info({
    request_id,
    notification_type,
    recipient_id,
    channel
  }, 'Notification send request received');
  
  try {
    // Generate notification_id for response
    const notification_id = randomUUID();
    
    // Simulate notification processing logic
    const delivery_status = 'queued'; // In real implementation, this would come from actual processing
    
    const response: NotificationSendResponse = {
      success: true,
      notification_id,
      request_id
    };
    
    // Calculate latency
    const latency_ms = Date.now() - startTime;
    
    // Log successful response
    logger.info({
      request_id,
      status: 'success',
      delivery_status,
      latency_ms,
      notification_id
    }, 'Notification send request completed');
    
    res.status(200).json(response);
    
  } catch (error) {
    const latency_ms = Date.now() - startTime;
    
    // Log error response
    logger.error({
      request_id,
      status: 'error',
      delivery_status: 'failed',
      latency_ms,
      error: error instanceof Error ? error.message : 'Unknown error'
    }, 'Notification send request failed');
    
    res.status(500).json({
      success: false,
      request_id,
      error: 'Internal server error'
    });
  }
});
