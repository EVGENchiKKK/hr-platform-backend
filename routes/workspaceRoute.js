const express = require('express');
const { authenticate } = require('../middleware/auth');
const workspaceController = require('../controllers/workspaceController');

const router = express.Router();

router.get('/data', authenticate, workspaceController.getBootstrap);
router.get('/bootstrap', authenticate, workspaceController.getBootstrap);
router.put('/appeals/:id', authenticate, workspaceController.updateAppeal);
router.post('/appeals/:id/messages', authenticate, workspaceController.sendAppealMessage);

module.exports = router;
