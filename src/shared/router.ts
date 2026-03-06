import { Router } from 'express';

export const notificationRouter = Router();

notificationRouter.get('/notifications/:id/status', (req, res) => {
  res.json({ id: req.params.id, status: 'delivered' });
});
