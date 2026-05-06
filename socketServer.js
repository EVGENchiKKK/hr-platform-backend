const { Server } = require('socket.io');
const { verifyToken } = require('./utils/jwt');
const { pool } = require('./config/database');

let io;

const normalizeRoleName = (roleName) => `${roleName || ''}`.trim().toLowerCase();
const isAppealManager = (roleName) => ['hr', 'admin'].includes(normalizeRoleName(roleName));
const userRoom = (userId) => `user:${userId}`;
const appealRoom = (appealId) => `appeal:${appealId}`;
const forumTopicRoom = (topicId) => `forum-topic:${topicId}`;
const forumGlobalRoom = 'forum:global';

const getSocketUser = async (token) => {
  if (!token) {
    return null;
  }

  const decoded = verifyToken(token);
  if (!decoded?.userId) {
    return null;
  }

  const [rows] = await pool.query(
    `SELECT u.User_ID, u.U_is_active, r.R_name
     FROM user u
     JOIN role r ON r.Role_ID = u.Role_ID
     WHERE u.User_ID = ?
     LIMIT 1`,
    [decoded.userId]
  );

  const user = rows[0];
  if (!user || !user.U_is_active) {
    return null;
  }

  return {
    userId: Number(user.User_ID),
    roleName: normalizeRoleName(user.R_name),
  };
};

const setupSocketServer = (httpServer) => {
  io = new Server(httpServer, {
    cors: {
      origin: [
        'http://localhost:3000',
        'http://localhost:5173',
        'http://127.0.0.1:5173',
        process.env.CORS_ORIGIN,
      ].filter(Boolean),
      credentials: true,
    },
  });

  io.use(async (socket, next) => {
    try {
      const token = socket.handshake.auth?.token || null;
      const user = await getSocketUser(token);

      if (!user) {
        return next(new Error('Unauthorized'));
      }

      socket.user = user;
      return next();
    } catch (error) {
      return next(new Error('Unauthorized'));
    }
  });

  io.on('connection', (socket) => {
    socket.join(userRoom(socket.user.userId));
    socket.join(forumGlobalRoom);

    socket.on('appeal:join', async ({ appealId }) => {
      const normalizedAppealId = Number(appealId || 0);
      if (!normalizedAppealId) {
        return;
      }

      const [rows] = await pool.query(
        `SELECT Appeal_ID, User_ID
         FROM appeal
         WHERE Appeal_ID = ?
         LIMIT 1`,
        [normalizedAppealId]
      );

      const appeal = rows[0];
      if (!appeal) {
        return;
      }

      const canAccess = isAppealManager(socket.user.roleName) || Number(appeal.User_ID) === Number(socket.user.userId);
      if (!canAccess) {
        return;
      }

      socket.join(appealRoom(normalizedAppealId));
    });

    socket.on('appeal:leave', ({ appealId }) => {
      const normalizedAppealId = Number(appealId || 0);
      if (!normalizedAppealId) {
        return;
      }

      socket.leave(appealRoom(normalizedAppealId));
    });

    socket.on('forum:join', ({ topicId }) => {
      const normalizedTopicId = Number(topicId || 0);
      if (!normalizedTopicId) {
        return;
      }

      socket.join(forumTopicRoom(normalizedTopicId));
    });

    socket.on('forum:leave', ({ topicId }) => {
      const normalizedTopicId = Number(topicId || 0);
      if (!normalizedTopicId) {
        return;
      }

      socket.leave(forumTopicRoom(normalizedTopicId));
    });
  });

  return io;
};

const emitAppealCreated = ({ appealId, authorId, recipientId }) => {
  if (!io) {
    return;
  }

  const payload = { appealId: Number(appealId) };
  io.to(userRoom(authorId)).emit('appeal:created', payload);
  if (recipientId) {
    io.to(userRoom(recipientId)).emit('appeal:created', payload);
  }
};

const emitAppealUpdated = ({ appealId, status, authorId, recipientId }) => {
  if (!io) {
    return;
  }

  const payload = { appealId: Number(appealId), status };
  io.to(appealRoom(appealId)).emit('appeal:updated', payload);
  io.to(userRoom(authorId)).emit('appeal:updated', payload);
  if (recipientId) {
    io.to(userRoom(recipientId)).emit('appeal:updated', payload);
  }
};

const emitAppealMessage = ({ appealId, status, message, authorId, recipientId }) => {
  if (!io) {
    return;
  }

  const payload = { appealId: Number(appealId), status, message };
  io.to(appealRoom(appealId)).emit('appeal:message', payload);
  io.to(userRoom(authorId)).emit('appeal:message', payload);
  if (recipientId) {
    io.to(userRoom(recipientId)).emit('appeal:message', payload);
  }
};

const emitForumTopicCreated = ({ topic }) => {
  if (!io) {
    return;
  }

  io.to(forumGlobalRoom).emit('forum:topic_created', { topic });
};

const emitForumMessage = ({ topicId, message, replies, updatedAt }) => {
  if (!io) {
    return;
  }

  const payload = {
    topicId: Number(topicId),
    message,
    replies,
    updatedAt,
  };

  io.to(forumGlobalRoom).emit('forum:message', payload);
  io.to(forumTopicRoom(topicId)).emit('forum:message', payload);
};

module.exports = {
  setupSocketServer,
  emitAppealCreated,
  emitAppealUpdated,
  emitAppealMessage,
  emitForumTopicCreated,
  emitForumMessage,
};
