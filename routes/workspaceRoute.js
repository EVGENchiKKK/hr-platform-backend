const express = require('express');
const { authenticate } = require('../middleware/auth');
const workspaceController = require('../controllers/workspaceController');

const router = express.Router();

router.get('/data', authenticate, workspaceController.getBootstrap);
router.get('/bootstrap', authenticate, workspaceController.getBootstrap);
router.post('/forum/topics', authenticate, workspaceController.createForumTopic);
router.post('/forum/topics/:id/posts', authenticate, workspaceController.createForumPost);
router.post('/appeals', authenticate, workspaceController.createAppeal);
router.put('/appeals/:id', authenticate, workspaceController.updateAppeal);
router.post('/appeals/:id/messages', authenticate, workspaceController.sendAppealMessage);
router.post('/surveys/:id/submit', authenticate, workspaceController.submitSurvey);
router.post('/courses/:id/start', authenticate, workspaceController.startCourse);
router.post('/courses/:id/progress', authenticate, workspaceController.advanceCourseProgress);

module.exports = router;
