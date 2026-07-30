import pino from 'pino';

export const logger = pino({
  name: 'notification-service',
  level: process.env.LOG_LEVEL || 'info',
});

export function scrubPII(data: any): any {
  if (!data || typeof data !== 'object') {
    return data;
  }

  const scrubbed = { ...data };
  
  // Remove email addresses - keep recipient_id only
  if (scrubbed.email) {
    delete scrubbed.email;
  }
  
  // Remove phone numbers - keep recipient_id only  
  if (scrubbed.phone) {
    delete scrubbed.phone;
  }
  
  // Remove message content to prevent PII leakage
  if (scrubbed.message) {
    delete scrubbed.message;
  }

  return scrubbed;
}
