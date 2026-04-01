const { pool } = require('../config/database');
const { hashPassword, validatePasswordStrength } = require('../utils/password');

const DEPARTMENT_COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#14b8a6', '#f97316', '#8b5cf6', '#0ea5e9'];
const MANAGER_ROLES = ['hr', 'admin'];

let ensureAppealMessagesTablePromise;
let ensureNotificationsTablePromise;

const getInitials = (firstName = '', lastName = '') =>
  `${firstName.charAt(0)}${lastName.charAt(0)}`.toUpperCase() || 'HR';

const safeJsonParse = (value, fallback) => {
  if (!value) {
    return fallback;
  }

  if (typeof value === 'object') {
    return value;
  }

  try {
    return JSON.parse(value);
  } catch (error) {
    return fallback;
  }
};

const normalizeRoleName = (roleName) => `${roleName || ''}`.trim().toLowerCase();

const isAppealManager = (roleName) => MANAGER_ROLES.includes(normalizeRoleName(roleName));

const formatPersonName = (name, surname, middleName) =>
  `${surname || ''} ${name || ''}${middleName ? ` ${middleName}` : ''}`.trim();

const mapTaskStatus = (status) => {
  switch (status) {
    case 'todo':
      return 'pending';
    case 'done':
      return 'completed';
    case 'review':
      return 'in_progress';
    case 'cancelled':
      return 'cancelled';
    default:
      return status;
  }
};

const mapAppealStatus = (status) => {
  switch (status) {
    case 'new':
      return 'open';
    case 'in_progress':
      return 'in_review';
    case 'closed':
      return 'closed';
    default:
      return status;
  }
};

const mapWorkspaceStatusToDb = (status) => {
  const statusMap = {
    open: 'new',
    in_review: 'in_progress',
    resolved: 'resolved',
    closed: 'closed'
  };

  return statusMap[status] || status;
};

const buildAppealSubject = (appeal) => {
  const category = appeal.A_category ? `: ${appeal.A_category}` : '';
  const preview = appeal.A_content.length > 48 ? `${appeal.A_content.slice(0, 48)}...` : appeal.A_content;
  return `${appeal.A_type}${category}` || preview;
};

const getLastMonths = (count = 6) => {
  const months = [];
  const now = new Date();
  const currentMonth = new Date(now.getFullYear(), now.getMonth(), 1);

  for (let index = count - 1; index >= 0; index -= 1) {
    const date = new Date(currentMonth.getFullYear(), currentMonth.getMonth() - index, 1);
    const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
    const label = date.toLocaleDateString('ru-RU', { month: 'short' }).replace('.', '');
    months.push({ key, label });
  }

  return months;
};

const countCourseModules = (contentStructure) => {
  if (!Array.isArray(contentStructure)) {
    return 0;
  }

  return contentStructure.length;
};

const countCourseLessons = (contentStructure) => {
  if (!Array.isArray(contentStructure)) {
    return 0;
  }

  return contentStructure.reduce((total, module) => {
    const lessons = Array.isArray(module?.lessons) ? module.lessons.length : 0;
    return total + lessons;
  }, 0);
};

const normalizeSurveyAnswers = (questions, answers) => {
  const answerMap = new Map();

  if (Array.isArray(answers)) {
    answers.forEach((answer) => {
      answerMap.set(String(answer.question_id), answer.answer);
    });
  } else if (answers && typeof answers === 'object') {
    Object.entries(answers).forEach(([key, value]) => {
      answerMap.set(String(key), value);
    });
  }

  return questions.map((question) => ({
    question_id: question.id,
    answer: answerMap.get(String(question.id)) ?? null
  }));
};

const isEmptyAnswer = (answer) => {
  if (answer === null || answer === undefined) {
    return true;
  }

  if (typeof answer === 'string') {
    return answer.trim() === '';
  }

  return false;
};

const calculateSurveyScore = (surveyType, questions, normalizedAnswers) => {
  if (!questions.length) {
    return 0;
  }

  if (surveyType === 'test') {
    const totalPoints = questions.reduce((sum, question) => sum + Number(question.points || 1), 0) || 1;
    const earnedPoints = questions.reduce((sum, question) => {
      const answer = normalizedAnswers.find((item) => Number(item.question_id) === Number(question.id));
      if (!answer || isEmptyAnswer(answer.answer)) {
        return sum;
      }

      return String(answer.answer) === String(question.correct) ? sum + Number(question.points || 1) : sum;
    }, 0);

    return Math.round((earnedPoints / totalPoints) * 100);
  }

  const answeredCount = normalizedAnswers.filter((answer) => !isEmptyAnswer(answer.answer)).length;
  return Math.round((answeredCount / questions.length) * 100);
};

const mapEnrollment = (row, totalLessons) => {
  if (!row?.my_enrollment_id) {
    return null;
  }

  const totalModules = Number(row.my_total_lessons || totalLessons || 0);
  const completedModules = Number(row.my_completed_lessons || 0);

  return {
    id: row.my_enrollment_id,
    currentLessonIndex: Number(row.my_current_lesson_index || 0),
    currentModuleIndex: Number(row.my_current_lesson_index || 0),
    progressPercent: Number(row.my_progress_percent || 0),
    completedLessons: completedModules,
    completedModules,
    totalLessons,
    totalModules,
    finalScore: row.my_final_score === null ? null : Number(row.my_final_score),
    isCompleted: Boolean(row.my_is_completed),
    isCertified: Boolean(row.my_is_certified),
    enrolledAt: row.my_enrolled_at,
    lastAccessed: row.my_last_accessed,
    completedAt: row.my_completed_at
  };
};

const mapSurveyResult = (row) => {
  if (!row?.my_result_id) {
    return null;
  }

  return {
    id: row.my_result_id,
    answers: safeJsonParse(row.my_answers, []),
    score: Number(row.my_score || 0),
    isCompleted: Boolean(row.my_is_completed),
    startedAt: row.my_started_at,
    submittedAt: row.my_submitted_at
  };
};

const ensureAppealMessagesTable = async () => {
  if (!ensureAppealMessagesTablePromise) {
    ensureAppealMessagesTablePromise = pool.query(
      `CREATE TABLE IF NOT EXISTS appeal_message (
        Appeal_Message_ID INT NOT NULL AUTO_INCREMENT,
        Appeal_ID INT NOT NULL,
        Author_ID INT NOT NULL,
        AM_content TEXT NOT NULL,
        AM_created DATETIME DEFAULT CURRENT_TIMESTAMP,
        AM_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (Appeal_Message_ID),
        KEY idx_appeal_message_appeal (Appeal_ID),
        KEY idx_appeal_message_author (Author_ID),
        KEY idx_appeal_message_created (AM_created),
        CONSTRAINT fk_appeal_message_appeal FOREIGN KEY (Appeal_ID) REFERENCES appeal (Appeal_ID) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_appeal_message_author FOREIGN KEY (Author_ID) REFERENCES user (User_ID) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT appeal_message_chk_1 CHECK (CHAR_LENGTH(AM_content) >= 1)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`
    ).catch((error) => {
      ensureAppealMessagesTablePromise = null;
      throw error;
    });
  }

  await ensureAppealMessagesTablePromise;
};

const ensureNotificationsTable = async () => {
  if (!ensureNotificationsTablePromise) {
    ensureNotificationsTablePromise = pool.query(
      `CREATE TABLE IF NOT EXISTS notification (
        Notification_ID INT NOT NULL AUTO_INCREMENT,
        User_ID INT NOT NULL,
        N_type VARCHAR(50) NOT NULL,
        N_title VARCHAR(200) NOT NULL,
        N_message TEXT NOT NULL,
        N_link VARCHAR(255) DEFAULT NULL,
        N_is_read TINYINT(1) DEFAULT 0,
        N_created DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (Notification_ID),
        KEY idx_notification_user (User_ID),
        KEY idx_notification_read (N_is_read),
        KEY idx_notification_created (N_created),
        CONSTRAINT fk_notification_user FOREIGN KEY (User_ID) REFERENCES user (User_ID) ON DELETE CASCADE ON UPDATE CASCADE
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`
    ).catch((error) => {
      ensureNotificationsTablePromise = null;
      throw error;
    });
  }

  await ensureNotificationsTablePromise;
};

const createNotification = async (executor, { userId, type, title, message, link = null }) => {
  if (!userId || !title || !message) {
    return;
  }

  await executor.query(
    `INSERT INTO notification (User_ID, N_type, N_title, N_message, N_link)
     VALUES (?, ?, ?, ?, ?)`,
    [userId, type || 'system', title, message, link]
  );
};

const formatNotificationTime = (value) => {
  if (!value) {
    return 'Только что';
  }

  const createdAt = new Date(value);
  const diffMinutes = Math.max(0, Math.round((Date.now() - createdAt.getTime()) / 60000));

  if (diffMinutes < 1) {
    return 'Только что';
  }
  if (diffMinutes < 60) {
    return `${diffMinutes} мин назад`;
  }

  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours} ч назад`;
  }

  const diffDays = Math.round(diffHours / 24);
  return `${diffDays} дн назад`;
};

const buildPersistedMessage = (row) => ({
  id: row.Appeal_Message_ID,
  appealId: row.Appeal_ID,
  authorId: row.Author_ID,
  authorName: row.author_name,
  authorRole: normalizeRoleName(row.role_name) || 'employee',
  text: row.AM_content,
  createdAt: row.AM_created,
  type: 'message'
});

const buildAppealThread = (appealRow, persistedMessages) => {
  const messages = [
    {
      id: `appeal-${appealRow.Appeal_ID}-root`,
      appealId: appealRow.Appeal_ID,
      authorId: appealRow.author_id,
      authorName: appealRow.author_name,
      authorRole: normalizeRoleName(appealRow.author_role_name) || 'employee',
      text: appealRow.A_content,
      createdAt: appealRow.A_created,
      type: 'appeal'
    }
  ];

  if (persistedMessages.length === 0 && `${appealRow.A_response || ''}`.trim()) {
    messages.push({
      id: `appeal-${appealRow.Appeal_ID}-legacy-response`,
      appealId: appealRow.Appeal_ID,
      authorId: appealRow.A_responder_id,
      authorName: appealRow.responder_name || 'HR',
      authorRole: normalizeRoleName(appealRow.responder_role_name) || 'hr',
      text: appealRow.A_response,
      createdAt: appealRow.A_closed_at || appealRow.A_updated || appealRow.A_created,
      type: 'message'
    });
  }

  return messages.concat(persistedMessages);
};

const fetchAppealAccessContext = async (executor, appealId, userId) => {
  const [userRows] = await executor.query(
    `SELECT u.User_ID, r.R_name
     FROM user u
     JOIN role r ON r.Role_ID = u.Role_ID
     WHERE u.User_ID = ?
     LIMIT 1`,
    [userId]
  );

  const [appealRows] = await executor.query(
    `SELECT Appeal_ID, User_ID, A_status
     FROM appeal
     WHERE Appeal_ID = ?
     LIMIT 1`,
    [appealId]
  );

  return {
    currentUser: userRows[0] || null,
    appeal: appealRows[0] || null
  };
};

const fetchInsertedMessage = async (executor, messageId) => {
  const [rows] = await executor.query(
    `SELECT
      am.Appeal_Message_ID, am.Appeal_ID, am.Author_ID, am.AM_content, am.AM_created,
      CONCAT(u.U_name, ' ', u.U_surname) AS author_name,
      r.R_name AS role_name
    FROM appeal_message am
    JOIN user u ON u.User_ID = am.Author_ID
    LEFT JOIN role r ON r.Role_ID = u.Role_ID
    WHERE am.Appeal_Message_ID = ?
    LIMIT 1`,
    [messageId]
  );

  return rows[0] ? buildPersistedMessage(rows[0]) : null;
};

exports.getBootstrap = async (req, res) => {
  try {
    await ensureAppealMessagesTable();
    await ensureNotificationsTable();
    const [currentUserRows] = await pool.query(
      `SELECT r.R_name, u.Department_ID, d.D_name AS department_name
       FROM user u
       JOIN role r ON r.Role_ID = u.Role_ID
       LEFT JOIN department d ON d.Department_ID = u.Department_ID
       WHERE u.User_ID = ?
       LIMIT 1`,
      [req.user.userId]
    );
    const currentRoleName = normalizeRoleName(currentUserRows[0]?.R_name);
    const currentDepartmentId = Number(currentUserRows[0]?.Department_ID || 0) || null;
    const currentDepartmentName = currentUserRows[0]?.department_name || null;
    const canViewPeopleInsights = isAppealManager(currentRoleName);

    const [employeeRows, departmentRows, taskRows, appealRows, forumRows, courseRows, surveyRows, appealRecipientRows] = await Promise.all([
      pool.query(
        `SELECT
          u.User_ID, u.U_name, u.U_surname, u.U_lastname, u.U_email, u.U_phone, u.U_hire_date,
          u.U_points_balance, u.U_is_active, u.U_login,
          r.R_name AS role_name,
          d.Department_ID, d.D_name AS department_name,
          COALESCE(tk.total_tasks, 0) AS total_tasks,
          COALESCE(tk.completed_tasks, 0) AS completed_tasks,
          COALESCE(tk.completion_rate, 0) AS completion_rate,
          COALESCE(course_stats.avg_progress, 0) AS course_progress,
          COALESCE(course_stats.completed_courses, 0) AS completed_courses,
          COALESCE(course_stats.completed_modules, 0) AS completed_modules,
          COALESCE(course_stats.total_modules, 0) AS total_modules,
          COALESCE(appeal_stats.appeal_count, 0) AS appeal_count,
          COALESCE(forum_stats.topic_count, 0) AS forum_topic_count,
          COALESCE(survey_stats.survey_count, 0) AS survey_count,
          COALESCE(survey_stats.avg_survey_score, 0) AS avg_survey_score
        FROM user u
        LEFT JOIN role r ON r.Role_ID = u.Role_ID
        LEFT JOIN department d ON d.Department_ID = u.Department_ID
        LEFT JOIN v_task_kpi tk ON tk.User_ID = u.User_ID
        LEFT JOIN (
          SELECT
            User_ID,
            AVG(E_progress_percent) AS avg_progress,
            SUM(CASE WHEN E_is_completed = 1 THEN 1 ELSE 0 END) AS completed_courses,
            SUM(COALESCE(E_completed_lessons, 0)) AS completed_modules,
            SUM(COALESCE(E_total_lessons, 0)) AS total_modules
          FROM enrollment
          GROUP BY User_ID
        ) course_stats ON course_stats.User_ID = u.User_ID
        LEFT JOIN (
          SELECT User_ID, COUNT(*) AS appeal_count
          FROM appeal
          GROUP BY User_ID
        ) appeal_stats ON appeal_stats.User_ID = u.User_ID
        LEFT JOIN (
          SELECT Author_ID, COUNT(*) AS topic_count
          FROM forum_them
          GROUP BY Author_ID
        ) forum_stats ON forum_stats.Author_ID = u.User_ID
        LEFT JOIN (
          SELECT
            User_ID,
            COUNT(*) AS survey_count,
            AVG(SR_score) AS avg_survey_score
          FROM opros_result
          WHERE OR_is_completed = 1
          GROUP BY User_ID
        ) survey_stats ON survey_stats.User_ID = u.User_ID
        WHERE u.U_is_active = TRUE
        ORDER BY u.U_surname, u.U_name`
      ),
      pool.query(
        `SELECT
          d.Department_ID, d.D_name, d.D_code, d.D_description,
          CONCAT(hu.U_name, ' ', hu.U_surname) AS head_name,
          COALESCE(vds.employee_count, 0) AS employee_count,
          COALESCE(vds.completed_tasks, 0) AS completed_tasks,
          COALESCE(vds.active_tasks, 0) AS active_tasks,
          COALESCE(vds.appeal_count, 0) AS appeal_count,
          COALESCE(vds.avg_achievement_points, 0) AS avg_achievement_points
        FROM department d
        LEFT JOIN user hu ON hu.User_ID = d.D_head_id
        LEFT JOIN v_department_stats vds ON vds.Department_ID = d.Department_ID
        WHERE d.D_is_active = TRUE
        ORDER BY d.D_name`
      ),
      pool.query(
        `SELECT
          t.Task_ID, t.T_title, t.T_description, t.T_priority, t.T_status, t.T_deadline, t.T_created, t.T_completed_at,
          t.T_assignee_user_id, t.T_assignee_dept_id,
          d.D_name AS department_name,
          CONCAT(au.U_name, ' ', au.U_surname) AS assignee_name,
          t.T_kpi_metrics
        FROM task t
        LEFT JOIN user au ON au.User_ID = t.T_assignee_user_id
        LEFT JOIN department d ON d.Department_ID = t.T_assignee_dept_id
        ORDER BY t.T_deadline`
      ),
      pool.query(
        `SELECT
          a.Appeal_ID, a.User_ID AS author_id, a.A_type, a.A_category, a.A_priority, a.A_content,
          a.A_status, a.A_response, a.A_created, a.A_updated, a.A_closed_at, a.A_responder_id,
          a.A_is_anonymous, a.A_is_confidential,
          CONCAT(u.U_name, ' ', u.U_surname) AS author_name,
          author_role.R_name AS author_role_name,
          d.D_name AS department_name,
          CONCAT(responder.U_name, ' ', responder.U_surname) AS responder_name,
          responder_role.R_name AS responder_role_name
        FROM appeal a
        JOIN user u ON u.User_ID = a.User_ID
        LEFT JOIN role author_role ON author_role.Role_ID = u.Role_ID
        LEFT JOIN department d ON d.Department_ID = u.Department_ID
        LEFT JOIN user responder ON responder.User_ID = a.A_responder_id
        LEFT JOIN role responder_role ON responder_role.Role_ID = responder.Role_ID
        ORDER BY a.A_created DESC`
      ),
      pool.query(
        `SELECT
          vfa.Forum_them_ID, ft.Author_ID, vfa.FT_name, vfa.author_name, vfa.department, vfa.FT_views_count, vfa.FT_posts_count,
          vfa.FT_created, vfa.FT_is_locked, ft.FT_category
        FROM v_forum_activity vfa
        JOIN forum_them ft ON ft.Forum_them_ID = vfa.Forum_them_ID
        ORDER BY vfa.FT_created DESC`
      ),
      pool.query(
        `SELECT
          c.Course_ID, c.C_name, c.C_description, c.C_category, c.C_estimated_hours, c.C_content_structure, c.C_content_url,
          c.C_department_id, department.D_name AS course_department_name,
          c.C_is_published, c.C_created, creator.U_name AS creator_name, creator.U_surname AS creator_surname,
          COUNT(DISTINCT e.Enrollment_ID) AS enrolled_count,
          SUM(CASE WHEN e.E_is_completed = 1 THEN 1 ELSE 0 END) AS completed_count
        FROM course c
        JOIN user creator ON creator.User_ID = c.C_created_by
        LEFT JOIN department department ON department.Department_ID = c.C_department_id
        LEFT JOIN enrollment e ON e.Course_ID = c.Course_ID
        GROUP BY c.Course_ID
        ORDER BY c.C_created DESC`
      ),
      pool.query(
        `SELECT
          o.Opros_ID, o.O_title, o.O_description, o.O_type, o.O_questions, o.O_is_active, o.O_start_date, o.O_end_date,
          o.O_department_id, department.D_name AS survey_department_name,
          creator.U_name AS creator_name, creator.U_surname AS creator_surname,
          COUNT(DISTINCT r.Opros_Result_ID) AS response_count
        FROM opros o
        JOIN user creator ON creator.User_ID = o.O_created_by
        LEFT JOIN department department ON department.Department_ID = o.O_department_id
        LEFT JOIN opros_result r ON r.Opros_ID = o.Opros_ID
        GROUP BY o.Opros_ID
        ORDER BY o.O_created DESC`
      ),
      pool.query(
        `SELECT
          u.User_ID,
          CONCAT(u.U_surname, ' ', u.U_name, IF(u.U_lastname IS NOT NULL AND u.U_lastname <> '', CONCAT(' ', u.U_lastname), '')) AS full_name,
          r.R_name AS role_name,
          d.D_name AS department_name
        FROM user u
        JOIN role r ON r.Role_ID = u.Role_ID
        LEFT JOIN department d ON d.Department_ID = u.Department_ID
        WHERE u.U_is_active = TRUE
          AND LOWER(r.R_name) IN ('hr', 'admin')
        ORDER BY full_name ASC`
      )
    ]);

    const appealIds = appealRows[0].map((row) => row.Appeal_ID);
    const courseIds = courseRows[0].map((row) => row.Course_ID);
    const surveyIds = surveyRows[0].map((row) => row.Opros_ID);
    const forumTopicIds = forumRows[0].map((row) => row.Forum_them_ID);
    const [appealMessageRows] = appealIds.length > 0
      ? await pool.query(
        `SELECT
          am.Appeal_Message_ID, am.Appeal_ID, am.Author_ID, am.AM_content, am.AM_created,
          CONCAT(u.U_name, ' ', u.U_surname) AS author_name,
          r.R_name AS role_name
        FROM appeal_message am
        JOIN user u ON u.User_ID = am.Author_ID
        LEFT JOIN role r ON r.Role_ID = u.Role_ID
        WHERE am.Appeal_ID IN (?)
        ORDER BY am.AM_created ASC, am.Appeal_Message_ID ASC`,
        [appealIds]
      )
      : [[]];

    const [notificationRows] = await pool.query(
      `SELECT Notification_ID, N_type, N_title, N_message, N_link, N_is_read, N_created
       FROM notification
       WHERE User_ID = ?
       ORDER BY N_created DESC
       LIMIT 20`,
      [req.user.userId]
    );

    const [forumPostRows] = forumTopicIds.length > 0
      ? await pool.query(
        `SELECT
          fp.Forum_posts_ID, fp.Forum_them_ID, fp.Author_ID, fp.FP_content, fp.FP_created, fp.FP_updated, fp.FP_is_edited, fp.FP_is_solution,
          CONCAT(u.U_name, ' ', u.U_surname) AS author_name,
          r.R_name AS role_name,
          d.D_name AS department_name
        FROM forum_posts fp
        JOIN user u ON u.User_ID = fp.Author_ID
        LEFT JOIN role r ON r.Role_ID = u.Role_ID
        LEFT JOIN department d ON d.Department_ID = u.Department_ID
        WHERE fp.Forum_them_ID IN (?)
        ORDER BY fp.FP_created ASC, fp.Forum_posts_ID ASC`,
        [forumTopicIds]
      )
      : [[]];

    const [userEnrollmentRows] = courseIds.length > 0
      ? await pool.query(
        `SELECT
          Enrollment_ID AS my_enrollment_id,
          Course_ID,
          E_current_lesson_index AS my_current_lesson_index,
          E_progress_percent AS my_progress_percent,
          E_completed_lessons AS my_completed_lessons,
          E_total_lessons AS my_total_lessons,
          E_final_score AS my_final_score,
          E_is_completed AS my_is_completed,
          E_is_certified AS my_is_certified,
          E_enrolled_at AS my_enrolled_at,
          E_last_accessed AS my_last_accessed,
          E_completed_at AS my_completed_at
        FROM enrollment
        WHERE User_ID = ? AND Course_ID IN (?)`,
        [req.user.userId, courseIds]
      )
      : [[]];

    const [userSurveyResultRows] = surveyIds.length > 0
      ? await pool.query(
        `SELECT
          Opros_Result_ID AS my_result_id,
          Opros_ID,
          OR_answers AS my_answers,
          SR_score AS my_score,
          OR_is_completed AS my_is_completed,
          OR_started_at AS my_started_at,
          OR_submitted_at AS my_submitted_at
        FROM opros_result
        WHERE User_ID = ? AND Opros_ID IN (?)`,
        [req.user.userId, surveyIds]
      )
      : [[]];

    const [allEnrollmentRows] = canViewPeopleInsights && courseIds.length > 0
      ? await pool.query(
        `SELECT
          e.User_ID,
          e.Course_ID,
          e.Enrollment_ID,
          e.E_current_lesson_index,
          e.E_progress_percent,
          e.E_completed_lessons,
          e.E_total_lessons,
          e.E_final_score,
          e.E_is_completed,
          e.E_is_certified,
          e.E_enrolled_at,
          e.E_last_accessed,
          e.E_completed_at,
          CONCAT(u.U_surname, ' ', u.U_name, IF(u.U_lastname IS NOT NULL AND u.U_lastname <> '', CONCAT(' ', u.U_lastname), '')) AS employee_name,
          d.D_name AS department_name,
          r.R_name AS role_name
        FROM enrollment e
        JOIN user u ON u.User_ID = e.User_ID
        LEFT JOIN department d ON d.Department_ID = u.Department_ID
        LEFT JOIN role r ON r.Role_ID = u.Role_ID
        WHERE e.Course_ID IN (?)
        ORDER BY employee_name ASC`,
        [courseIds]
      )
      : [[]];

    const [allSurveyResultRows] = canViewPeopleInsights && surveyIds.length > 0
      ? await pool.query(
        `SELECT
          sr.User_ID,
          sr.Opros_ID,
          sr.Opros_Result_ID,
          sr.OR_answers,
          sr.SR_score,
          sr.OR_is_completed,
          sr.OR_started_at,
          sr.OR_submitted_at,
          CONCAT(u.U_surname, ' ', u.U_name, IF(u.U_lastname IS NOT NULL AND u.U_lastname <> '', CONCAT(' ', u.U_lastname), '')) AS employee_name,
          d.D_name AS department_name,
          r.R_name AS role_name
        FROM opros_result sr
        JOIN user u ON u.User_ID = sr.User_ID
        LEFT JOIN department d ON d.Department_ID = u.Department_ID
        LEFT JOIN role r ON r.Role_ID = u.Role_ID
        WHERE sr.Opros_ID IN (?)
        ORDER BY employee_name ASC`,
        [surveyIds]
      )
      : [[]];

    const messagesByAppeal = appealMessageRows.reduce((accumulator, row) => {
      const currentMessages = accumulator.get(row.Appeal_ID) || [];
      currentMessages.push(buildPersistedMessage(row));
      accumulator.set(row.Appeal_ID, currentMessages);
      return accumulator;
    }, new Map());

    const enrollmentsByCourse = userEnrollmentRows.reduce((accumulator, row) => {
      accumulator.set(row.Course_ID, row);
      return accumulator;
    }, new Map());

    const resultsBySurvey = userSurveyResultRows.reduce((accumulator, row) => {
      accumulator.set(row.Opros_ID, row);
      return accumulator;
    }, new Map());

    const enrollmentsByCourseAll = allEnrollmentRows.reduce((accumulator, row) => {
      const currentRows = accumulator.get(row.Course_ID) || [];
      currentRows.push({
        employeeId: row.User_ID,
        employeeName: row.employee_name,
        department: row.department_name,
        role: row.role_name,
        enrollmentId: row.Enrollment_ID,
        currentLessonIndex: Number(row.E_current_lesson_index || 0),
        currentModuleIndex: Number(row.E_current_lesson_index || 0),
        progressPercent: Number(row.E_progress_percent || 0),
        completedLessons: Number(row.E_completed_lessons || 0),
        completedModules: Number(row.E_completed_lessons || 0),
        totalLessons: Number(row.E_total_lessons || 0),
        totalModules: Number(row.E_total_lessons || 0),
        finalScore: row.E_final_score === null ? null : Number(row.E_final_score),
        isCompleted: Boolean(row.E_is_completed),
        isCertified: Boolean(row.E_is_certified),
        enrolledAt: row.E_enrolled_at,
        lastAccessed: row.E_last_accessed,
        completedAt: row.E_completed_at
      });
      accumulator.set(row.Course_ID, currentRows);
      return accumulator;
    }, new Map());

    const resultsBySurveyAll = allSurveyResultRows.reduce((accumulator, row) => {
      const currentRows = accumulator.get(row.Opros_ID) || [];
      currentRows.push({
        employeeId: row.User_ID,
        employeeName: row.employee_name,
        department: row.department_name,
        role: row.role_name,
        resultId: row.Opros_Result_ID,
        answers: safeJsonParse(row.OR_answers, []),
        score: Number(row.SR_score || 0),
        isCompleted: Boolean(row.OR_is_completed),
        startedAt: row.OR_started_at,
        submittedAt: row.OR_submitted_at
      });
      accumulator.set(row.Opros_ID, currentRows);
      return accumulator;
    }, new Map());

    const forumPostsByTopic = forumPostRows.reduce((accumulator, row) => {
      const currentRows = accumulator.get(row.Forum_them_ID) || [];
      currentRows.push({
        id: row.Forum_posts_ID,
        topicId: row.Forum_them_ID,
        authorId: row.Author_ID,
        authorName: row.author_name,
        authorRole: normalizeRoleName(row.role_name) || 'employee',
        department: row.department_name || 'Без отдела',
        content: row.FP_content,
        createdAt: row.FP_created,
        updatedAt: row.FP_updated,
        isEdited: Boolean(row.FP_is_edited),
        isSolution: Boolean(row.FP_is_solution)
      });
      accumulator.set(row.Forum_them_ID, currentRows);
      return accumulator;
    }, new Map());

    const employeeCourseMap = allEnrollmentRows.reduce((accumulator, row) => {
      const currentRows = accumulator.get(row.User_ID) || [];
      currentRows.push({
        courseId: row.Course_ID,
        enrollmentId: row.Enrollment_ID,
        progressPercent: Number(row.E_progress_percent || 0),
        completedLessons: Number(row.E_completed_lessons || 0),
        totalLessons: Number(row.E_total_lessons || 0),
        finalScore: row.E_final_score === null ? null : Number(row.E_final_score),
        isCompleted: Boolean(row.E_is_completed),
        enrolledAt: row.E_enrolled_at,
        completedAt: row.E_completed_at
      });
      accumulator.set(row.User_ID, currentRows);
      return accumulator;
    }, new Map());

    const employeeSurveyMap = allSurveyResultRows.reduce((accumulator, row) => {
      const currentRows = accumulator.get(row.User_ID) || [];
      currentRows.push({
        surveyId: row.Opros_ID,
        resultId: row.Opros_Result_ID,
        score: Number(row.SR_score || 0),
        isCompleted: Boolean(row.OR_is_completed),
        startedAt: row.OR_started_at,
        submittedAt: row.OR_submitted_at
      });
      accumulator.set(row.User_ID, currentRows);
      return accumulator;
    }, new Map());

    const employees = employeeRows[0].map((row) => {
      const fullName = formatPersonName(row.U_name, row.U_surname, row.U_lastname);
      const employeeCourses = employeeCourseMap.get(row.User_ID) || [];
      const employeeSurveys = employeeSurveyMap.get(row.User_ID) || [];
      const moduleProgress = Number(row.total_modules || 0) > 0
        ? Math.round((Number(row.completed_modules || 0) / Number(row.total_modules || 0)) * 100)
        : Math.round(Number(row.course_progress || 0));
      const surveyActivity = Math.min(Number(row.survey_count || 0) * 12, 24);
      const moduleActivity = Math.min(Number(row.completed_modules || 0) * 6, 24);
      const taskActivity = Math.round(Number(row.completion_rate || 0) * 0.4);
      const pointsActivity = Math.min(Math.round(Number(row.U_points_balance || 0) / 10), 12);
      const kpiBase = Math.min(
        100,
        taskActivity
          + Math.round(moduleProgress * 0.3)
          + surveyActivity
          + moduleActivity
          + pointsActivity
      );

      return {
        id: row.User_ID,
        login: row.U_login,
        name: fullName,
        firstName: row.U_name,
        lastName: row.U_surname,
        middleName: row.U_lastname,
        department: row.department_name,
        departmentId: row.Department_ID,
        position: row.role_name,
        role: row.role_name,
        email: row.U_email,
        phone: row.U_phone,
        hireDate: row.U_hire_date,
        status: row.U_is_active ? 'active' : 'inactive',
        avatar: getInitials(row.U_name, row.U_surname),
        kpi: Math.round(kpiBase || 0),
        totalTasks: Number(row.total_tasks || 0),
        completedTasks: Number(row.completed_tasks || 0),
        taskCompletion: Number(row.completion_rate || 0),
        courseProgress: Math.round(Number(row.course_progress || 0)),
        moduleProgress,
        completedCourses: Number(row.completed_courses || 0),
        completedModules: Number(row.completed_modules || 0),
        totalModules: Number(row.total_modules || 0),
        appealCount: Number(row.appeal_count || 0),
        forumTopics: Number(row.forum_topic_count || 0),
        surveyCount: Number(row.survey_count || 0),
        averageSurveyScore: Math.round(Number(row.avg_survey_score || 0)),
        pointsBalance: Number(row.U_points_balance || 0),
        activityScore: Math.round(moduleActivity + surveyActivity + taskActivity + pointsActivity),
        trainingRecords: employeeCourses,
        surveyRecords: employeeSurveys
      };
    });

    const departments = departmentRows[0].map((row, index) => {
      const kpi = Math.min(
        100,
        Math.round((Number(row.avg_achievement_points || 0) * 0.35) + (Number(row.completed_tasks || 0) * 8) + (Number(row.employee_count || 0) * 2))
      );

      return {
        id: row.Department_ID,
        name: row.D_name,
        code: row.D_code,
        description: row.D_description,
        head: row.head_name,
        employeeCount: Number(row.employee_count || 0),
        completedTasks: Number(row.completed_tasks || 0),
        activeTasks: Number(row.active_tasks || 0),
        appealCount: Number(row.appeal_count || 0),
        kpi,
        color: DEPARTMENT_COLORS[index % DEPARTMENT_COLORS.length]
      };
    });

    const tasks = taskRows[0].map((row) => {
      const metrics = safeJsonParse(row.T_kpi_metrics, []);
      return {
        id: row.Task_ID,
        assigneeId: row.T_assignee_user_id ? Number(row.T_assignee_user_id) : null,
        departmentId: row.T_assignee_dept_id ? Number(row.T_assignee_dept_id) : null,
        title: row.T_title,
        description: row.T_description,
        assignee: row.assignee_name || 'Не назначен',
        department: row.department_name || 'Без отдела',
        createdAt: row.T_created,
        deadline: row.T_deadline,
        completedAt: row.T_completed_at,
        status: mapTaskStatus(row.T_status),
        kpiWeight: Math.round(Number(metrics?.[0]?.weight || 0) * 100) || 0,
        priority: row.T_priority === 'urgent' ? 'high' : row.T_priority || 'medium'
      };
    });

    const appeals = appealRows[0].map((row) => {
      const persistedMessages = messagesByAppeal.get(row.Appeal_ID) || [];
      const messages = buildAppealThread(row, persistedMessages);

      return {
        id: row.Appeal_ID,
        authorId: row.author_id,
        recipientId: row.A_responder_id ? Number(row.A_responder_id) : null,
        recipientName: row.responder_name || 'Не назначен',
        subject: buildAppealSubject(row),
        from: row.author_name,
        department: row.department_name || 'Без отдела',
        date: row.A_created,
        status: mapAppealStatus(row.A_status),
        priority: row.A_priority || 'medium',
        type: row.A_type || 'question',
        category: row.A_category || row.A_type,
        description: row.A_content,
        response: row.A_response,
        isAnonymous: Boolean(row.A_is_anonymous),
        isConfidential: Boolean(row.A_is_confidential),
        messages,
        lastMessageAt: messages[messages.length - 1]?.createdAt || row.A_created
      };
    });

    const forumPosts = forumRows[0].map((row) => {
      const messages = forumPostsByTopic.get(row.Forum_them_ID) || [];
      return {
        id: row.Forum_them_ID,
        title: row.FT_name,
        authorId: row.Author_ID ? Number(row.Author_ID) : null,
        author: row.author_name,
        department: row.department,
        category: row.FT_category || 'Обсуждение',
        replies: messages.length,
        views: Number(row.FT_views_count || 0),
        date: row.FT_created,
        updatedAt: row.FT_created,
        isLocked: Boolean(row.FT_is_locked),
        pinned: Boolean(row.FT_is_locked) || row.FT_category === 'Объявления',
        tags: row.FT_category ? row.FT_category.toLowerCase().split(/\s+/).slice(0, 3) : [],
        messages
      };
    });

    const courses = courseRows[0].map((row) => {
      const content = safeJsonParse(row.C_content_structure, []);
      const totalModules = countCourseModules(content);
      const moduleCount = Array.isArray(content) ? content.length : 0;
      const totalLessons = countCourseLessons(content);
      const enrollmentRow = enrollmentsByCourse.get(row.Course_ID);
      const visibleToCurrentUser = canViewPeopleInsights
        || !row.C_department_id
        || Number(row.C_department_id) === Number(currentDepartmentId);
      return {
        id: row.Course_ID,
        title: row.C_name,
        description: row.C_description,
        createdAt: row.C_created,
        duration: row.C_estimated_hours ? `${row.C_estimated_hours} ч` : 'Не указано',
        modules: moduleCount,
        totalModules,
        totalLessons,
        enrolled: Number(row.enrolled_count || 0),
        completed: Number(row.completed_count || 0),
        status: row.C_is_published ? 'active' : 'draft',
        category: row.C_category || 'Обучение',
        instructor: `${row.creator_name} ${row.creator_surname}`.trim(),
        departmentId: row.C_department_id,
        department: row.course_department_name,
        visibleToCurrentUser,
        contentUrl: row.C_content_url,
        contentStructure: content,
        myEnrollment: mapEnrollment(enrollmentRow, totalLessons),
        participants: canViewPeopleInsights ? (enrollmentsByCourseAll.get(row.Course_ID) || []) : []
      };
    });

    const employeeCount = employees.length || 1;
    const surveys = surveyRows[0].map((row) => {
      const resultRow = resultsBySurvey.get(row.Opros_ID);
      return {
        id: row.Opros_ID,
        title: row.O_title,
        type: row.O_type === 'feedback' ? 'survey' : row.O_type,
        status: row.O_is_active ? 'active' : 'completed',
        responses: Number(row.response_count || 0),
        total: employeeCount,
        deadline: row.O_end_date,
        createdBy: `${row.creator_name} ${row.creator_surname}`.trim(),
        description: row.O_description,
        departmentId: row.O_department_id,
        department: row.survey_department_name,
        visibleToCurrentUser: canViewPeopleInsights
          || !row.O_department_id
          || Number(row.O_department_id) === Number(currentDepartmentId),
        questions: safeJsonParse(row.O_questions, []),
        myResult: mapSurveyResult(resultRow),
        submissions: canViewPeopleInsights ? (resultsBySurveyAll.get(row.Opros_ID) || []) : []
      };
    });

    const scopedEmployees = canViewPeopleInsights
      ? employees
      : employees.filter((employee) => Number(employee.id) === Number(req.user.userId));

    const scopedDepartments = canViewPeopleInsights
      ? departments
      : departments.filter((department) => Number(department.id) === Number(currentDepartmentId));

    const scopedTasks = canViewPeopleInsights
      ? tasks
      : tasks.filter((task) => Number(task.assigneeId) === Number(req.user.userId));

    const scopedAppeals = canViewPeopleInsights
      ? appeals
      : appeals.filter((appeal) => Number(appeal.authorId) === Number(req.user.userId));

    const scopedForumPosts = forumPosts;
    const appealRecipients = appealRecipientRows[0].map((row) => ({
      id: row.User_ID,
      name: row.full_name,
      role: row.role_name,
      department: row.department_name
    }));

    const scopedCourses = courses.filter((course) => course.visibleToCurrentUser);
    const scopedSurveys = surveys.filter((survey) => survey.visibleToCurrentUser);

    const lastMonths = getLastMonths(6);

    const monthlyMap = new Map(lastMonths.map((month) => [month.key, {
      month: month.label,
      hires: 0,
      tasks: 0,
      appeals: 0,
      forum: 0,
      courses: 0
    }]));

    scopedTasks.forEach((task) => {
      const date = new Date(task.createdAt);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).tasks += 1;
      }
    });

    scopedAppeals.forEach((appeal) => {
      const date = new Date(appeal.date);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).appeals += 1;
      }
    });

    scopedEmployees.forEach((employee) => {
      const date = new Date(employee.hireDate);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).hires += 1;
      }
    });

    scopedForumPosts.forEach((post) => {
      const date = new Date(post.date);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).forum += 1;
      }
    });

    scopedCourses.forEach((course) => {
      const date = new Date(course.createdAt || course.created || null);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).courses += 1;
      }
    });

    const monthlyStats = lastMonths.map((month) => monthlyMap.get(month.key));

    const departmentMonthlyMap = new Map(lastMonths.map((month) => [month.key, { month: month.label }]));

    scopedDepartments.forEach((department) => {
      lastMonths.forEach((month) => {
        departmentMonthlyMap.get(month.key)[department.code || department.name] = 0;
      });
    });

    scopedTasks.forEach((task) => {
      if (!task.department) {
        return;
      }

      const department = scopedDepartments.find((item) => item.name === task.department);
      if (!department) {
        return;
      }

      const date = new Date(task.createdAt);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (departmentMonthlyMap.has(key)) {
        departmentMonthlyMap.get(key)[department.code || department.name] += 1;
      }
    });

    const departmentMonthlyStats = lastMonths.map((month) => departmentMonthlyMap.get(month.key));
    const notifications = notificationRows.map((row) => ({
      id: row.Notification_ID,
      type: row.N_type,
      title: row.N_title,
      text: row.N_message,
      link: row.N_link,
      unread: !row.N_is_read,
      createdAt: row.N_created,
      time: formatNotificationTime(row.N_created)
    }));

    res.json({
      success: true,
      data: {
        currentUserRole: currentRoleName,
        currentUserDepartmentId: currentDepartmentId,
        currentUserDepartmentName: currentDepartmentName,
        employees: scopedEmployees,
        departments: scopedDepartments,
        tasks: scopedTasks,
        appeals: scopedAppeals,
        forumPosts: scopedForumPosts,
        appealRecipients,
        courses: scopedCourses,
        surveys: scopedSurveys,
        notifications,
        monthlyStats,
        departmentMonthlyStats
      }
    });
  } catch (error) {
    console.error('Workspace bootstrap error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при загрузке данных рабочего пространства'
    });
  }
};

exports.createAppeal = async (req, res) => {
  const type = `${req.body.type || ''}`.trim().toLowerCase();
  const category = `${req.body.category || ''}`.trim();
  const priority = `${req.body.priority || ''}`.trim().toLowerCase();
  const content = `${req.body.content || ''}`.trim();
  const recipientId = Number(req.body.recipientId || 0);
  const isAnonymous = Boolean(req.body.isAnonymous);
  const isConfidential = Boolean(req.body.isConfidential);

  const allowedTypes = ['complaint', 'suggestion', 'question'];
  const allowedPriorities = ['low', 'medium', 'high'];

  if (!allowedTypes.includes(type)) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный тип обращения'
    });
  }

  if (!allowedPriorities.includes(priority)) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный приоритет обращения'
    });
  }

  if (!category || category.length < 3) {
    return res.status(400).json({
      success: false,
      error: 'Укажите тему обращения не короче 3 символов'
    });
  }

  if (!content || content.length < 10) {
    return res.status(400).json({
      success: false,
      error: 'Текст обращения должен содержать не менее 10 символов'
    });
  }

  if (!recipientId) {
    return res.status(400).json({
      success: false,
      error: 'Выберите получателя обращения'
    });
  }

  try {
    await ensureNotificationsTable();
    const [recipientRows] = await pool.query(
      `SELECT u.User_ID, u.U_is_active, r.R_name
       FROM user u
       JOIN role r ON r.Role_ID = u.Role_ID
       WHERE u.User_ID = ?
       LIMIT 1`,
      [recipientId]
    );

    const recipient = recipientRows[0];

    if (!recipient || !recipient.U_is_active) {
      return res.status(404).json({
        success: false,
        error: 'Получатель обращения не найден'
      });
    }

    if (!isAppealManager(recipient.R_name)) {
      return res.status(400).json({
        success: false,
        error: 'Получателем обращения может быть только HR или администратор'
      });
    }

    const [insertResult] = await pool.query(
      `INSERT INTO appeal (
        User_ID,
        A_type,
        A_category,
        A_priority,
        A_content,
        A_status,
        A_responder_id,
        A_is_anonymous,
        A_is_confidential
      ) VALUES (?, ?, ?, ?, ?, 'new', ?, ?, ?)`,
      [req.user.userId, type, category, priority, content, recipientId, isAnonymous ? 1 : 0, isConfidential ? 1 : 0]
    );

    await createNotification(pool, {
      userId: recipientId,
      type: 'appeal_created',
      title: 'Новое обращение',
      message: `Поступило новое обращение "${category}"`,
      link: '/appeals'
    });

    res.status(201).json({
      success: true,
      message: 'Обращение создано',
      data: {
        id: insertResult.insertId
      }
    });
  } catch (error) {
    console.error('Create appeal error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при создании обращения'
    });
  }
};

exports.createForumTopic = async (req, res) => {
  const title = `${req.body.title || ''}`.trim();
  const category = `${req.body.category || ''}`.trim();
  const content = `${req.body.content || ''}`.trim();

  if (!title || title.length < 5) {
    return res.status(400).json({
      success: false,
      error: 'Укажите название темы не короче 5 символов'
    });
  }

  if (!content || content.length < 5) {
    return res.status(400).json({
      success: false,
      error: 'Введите сообщение темы не короче 5 символов'
    });
  }

  let connection;

  try {
    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [topicResult] = await connection.query(
      `INSERT INTO forum_them (Author_ID, FT_name, FT_category, FT_posts_count)
       VALUES (?, ?, ?, 1)`,
      [req.user.userId, title, category || 'Обсуждение']
    );

    await connection.query(
      `INSERT INTO forum_posts (Forum_them_ID, Author_ID, FP_content)
       VALUES (?, ?, ?)`,
      [topicResult.insertId, req.user.userId, content]
    );

    await connection.commit();

    res.status(201).json({
      success: true,
      message: 'Тема форума создана',
      data: {
        id: topicResult.insertId
      }
    });
  } catch (error) {
    if (connection) {
      await connection.rollback();
    }

    console.error('Create forum topic error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при создании темы форума'
    });
  } finally {
    if (connection) {
      connection.release();
    }
  }
};

exports.createTask = async (req, res) => {
  const title = `${req.body.title || ''}`.trim();
  const description = `${req.body.description || ''}`.trim();
  const priority = `${req.body.priority || ''}`.trim().toLowerCase();
  const deadline = `${req.body.deadline || ''}`.trim();
  const assigneeId = Number(req.body.assigneeId || 0);
  const kpiWeight = Math.max(0, Math.min(100, Number(req.body.kpiWeight || 0)));

  if (!title || title.length < 3) {
    return res.status(400).json({
      success: false,
      error: 'Укажите название задачи не короче 3 символов'
    });
  }

  if (!description || description.length < 5) {
    return res.status(400).json({
      success: false,
      error: 'Описание задачи должно содержать не менее 5 символов'
    });
  }

  if (!assigneeId) {
    return res.status(400).json({
      success: false,
      error: 'Выберите сотрудника для задачи'
    });
  }

  if (!deadline) {
    return res.status(400).json({
      success: false,
      error: 'Укажите срок выполнения задачи'
    });
  }

  if (!['low', 'medium', 'high', 'urgent'].includes(priority)) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный приоритет задачи'
    });
  }

  try {
    const [creatorRows, assigneeRows] = await Promise.all([
      pool.query(
        `SELECT u.User_ID, r.R_name
         FROM user u
         JOIN role r ON r.Role_ID = u.Role_ID
         WHERE u.User_ID = ?
         LIMIT 1`,
        [req.user.userId]
      ),
      pool.query(
        `SELECT User_ID, Department_ID, U_is_active
         FROM user
         WHERE User_ID = ?
         LIMIT 1`,
        [assigneeId]
      )
    ]);

    const creator = creatorRows[0][0];
    const assignee = assigneeRows[0][0];

    if (!creator || !isAppealManager(creator.R_name)) {
      return res.status(403).json({
        success: false,
        error: 'Создавать задачи могут только HR и администратор'
      });
    }

    if (!assignee || !assignee.U_is_active) {
      return res.status(404).json({
        success: false,
        error: 'Сотрудник для задачи не найден'
      });
    }

    const [insertResult] = await pool.query(
      `INSERT INTO task (
        T_title,
        T_description,
        T_priority,
        T_status,
        T_assignee_user_id,
        T_assignee_dept_id,
        T_creator_id,
        T_deadline,
        T_kpi_metrics
      ) VALUES (?, ?, ?, 'todo', ?, ?, ?, ?, ?)`,
      [
        title,
        description,
        priority,
        assigneeId,
        assignee.Department_ID || null,
        req.user.userId,
        deadline,
        JSON.stringify([{ metric: 'completion', weight: kpiWeight / 100, target: 100 }])
      ]
    );

    res.status(201).json({
      success: true,
      message: 'Задача создана',
      data: {
        id: insertResult.insertId
      }
    });
  } catch (error) {
    console.error('Create task error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при создании задачи'
    });
  }
};

exports.createDepartment = async (req, res) => {
  const name = `${req.body.name || ''}`.trim();
  const code = `${req.body.code || ''}`.trim().toUpperCase();
  const description = `${req.body.description || ''}`.trim();
  const headId = Number(req.body.headId || 0) || null;

  try {
    await ensureNotificationsTable();
    const [creatorRows] = await pool.query(
      `SELECT u.User_ID, r.R_name
       FROM user u
       JOIN role r ON r.Role_ID = u.Role_ID
       WHERE u.User_ID = ?
       LIMIT 1`,
      [req.user.userId]
    );

    const creator = creatorRows[0];

    if (!creator || !isAppealManager(creator.R_name)) {
      return res.status(403).json({
        success: false,
        error: 'Добавлять отделы могут только HR и администратор'
      });
    }

    if (!name || name.length < 2) {
      return res.status(400).json({
        success: false,
        error: 'Название отдела должно содержать минимум 2 символа'
      });
    }

    if (!code || code.length < 2) {
      return res.status(400).json({
        success: false,
        error: 'Код отдела должен содержать минимум 2 символа'
      });
    }

    const [duplicateRows] = await pool.query(
      `SELECT Department_ID
       FROM department
       WHERE D_name = ? OR D_code = ?
       LIMIT 1`,
      [name, code]
    );

    if (duplicateRows.length > 0) {
      return res.status(409).json({
        success: false,
        error: 'Отдел с таким названием или кодом уже существует'
      });
    }

    if (headId) {
      const [headRows] = await pool.query(
        `SELECT User_ID
         FROM user
         WHERE User_ID = ? AND U_is_active = TRUE
         LIMIT 1`,
        [headId]
      );

      if (headRows.length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Руководитель отдела не найден'
        });
      }
    }

    const [result] = await pool.query(
      `INSERT INTO department (D_name, D_code, D_description, D_head_id, D_is_active)
       VALUES (?, ?, ?, ?, TRUE)`,
      [name, code, description || null, headId]
    );

    res.status(201).json({
      success: true,
      message: 'Отдел создан',
      data: {
        id: result.insertId
      }
    });
  } catch (error) {
    console.error('Create department error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при создании отдела'
    });
  }
};

exports.markNotificationsRead = async (req, res) => {
  try {
    await ensureNotificationsTable();
    await pool.query(
      `UPDATE notification
       SET N_is_read = 1
       WHERE User_ID = ? AND N_is_read = 0`,
      [req.user.userId]
    );

    res.json({
      success: true,
      message: 'Уведомления отмечены как прочитанные'
    });
  } catch (error) {
    console.error('Mark notifications read error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при обновлении уведомлений'
    });
  }
};

exports.createEmployee = async (req, res) => {
  const {
    login,
    firstName,
    lastName,
    middleName,
    email,
    phone,
    hireDate,
    roleId,
    departmentId,
    password
  } = req.body;

  try {
    await ensureNotificationsTable();
    const [creatorRows] = await pool.query(
      `SELECT u.User_ID, r.R_name
       FROM user u
       JOIN role r ON r.Role_ID = u.Role_ID
       WHERE u.User_ID = ?
       LIMIT 1`,
      [req.user.userId]
    );

    const creator = creatorRows[0];

    if (!creator || !isAppealManager(creator.R_name)) {
      return res.status(403).json({
        success: false,
        error: 'Добавлять сотрудников могут только HR и администратор'
      });
    }

    const passwordValidation = validatePasswordStrength(password);
    if (!passwordValidation.isValid) {
      return res.status(400).json({
        success: false,
        error: 'Слабый пароль',
        details: passwordValidation.errors
      });
    }

    const normalizedEmail = `${email || ''}`.trim().toLowerCase();
    const normalizedLogin = `${login || ''}`.trim().toLowerCase();
    const normalizedFirstName = `${firstName || ''}`.trim();
    const normalizedLastName = `${lastName || ''}`.trim();
    const normalizedMiddleName = `${middleName || ''}`.trim() || null;
    const normalizedPhone = `${phone || ''}`.trim() || null;

    if (!normalizedLogin || !normalizedFirstName || !normalizedLastName || !normalizedEmail || !hireDate || !roleId || !departmentId) {
      return res.status(400).json({
        success: false,
        error: 'Заполните все обязательные поля сотрудника'
      });
    }

    const [existingUsers] = await pool.query(
      'SELECT User_ID, U_email, U_login FROM user WHERE U_email = ? OR U_login = ?',
      [normalizedEmail, normalizedLogin]
    );

    if (existingUsers.length > 0) {
      return res.status(409).json({
        success: false,
        error: 'Пользователь с таким email или логином уже существует'
      });
    }

    const [[roleRows], [departmentRows]] = await Promise.all([
      pool.query('SELECT Role_ID FROM role WHERE Role_ID = ? LIMIT 1', [roleId]),
      pool.query('SELECT Department_ID FROM department WHERE Department_ID = ? AND D_is_active = TRUE LIMIT 1', [departmentId])
    ]);

    if (roleRows.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Выбранная роль не найдена'
      });
    }

    if (departmentRows.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Выбранный отдел не найден'
      });
    }

    const passwordHash = await hashPassword(password);

    const [result] = await pool.query(
      `INSERT INTO user (
        U_login, U_password_hash, U_name, U_surname, U_lastname, U_email,
        U_phone, U_hire_date, Role_ID, Department_ID, U_is_active
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)`,
      [
        normalizedLogin,
        passwordHash,
        normalizedFirstName,
        normalizedLastName,
        normalizedMiddleName,
        normalizedEmail,
        normalizedPhone,
        hireDate,
        roleId,
        departmentId
      ]
    );

    res.status(201).json({
      success: true,
      message: 'Сотрудник создан',
      data: {
        id: result.insertId
      }
    });
  } catch (error) {
    console.error('Create employee error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при создании сотрудника'
    });
  }
};

exports.createTest = async (req, res) => {
  const title = `${req.body.title || ''}`.trim();
  const description = `${req.body.description || ''}`.trim();
  const departmentId = Number(req.body.departmentId || 0) || null;
  const endDate = `${req.body.endDate || ''}`.trim() || null;
  const questions = Array.isArray(req.body.questions) ? req.body.questions : [];

  try {
    const [creatorRows] = await pool.query(
      `SELECT u.User_ID, r.R_name
       FROM user u
       JOIN role r ON r.Role_ID = u.Role_ID
       WHERE u.User_ID = ?
       LIMIT 1`,
      [req.user.userId]
    );

    const creator = creatorRows[0];

    if (!creator || !isAppealManager(creator.R_name)) {
      return res.status(403).json({
        success: false,
        error: 'Создавать тесты могут только HR и администратор'
      });
    }

    if (!title || title.length < 3) {
      return res.status(400).json({
        success: false,
        error: 'Название теста должно содержать минимум 3 символа'
      });
    }

    if (questions.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Добавьте хотя бы один вопрос'
      });
    }

    const normalizedQuestions = questions.map((question, index) => ({
      id: index + 1,
      text: `${question.text || ''}`.trim(),
      type: 'choice',
      required: true,
      points: Math.max(1, Number(question.points || 1)),
      options: Array.isArray(question.options) ? question.options.map((option) => `${option || ''}`.trim()).filter(Boolean) : [],
      correct: String(question.correct ?? '')
    }));

    const invalidQuestion = normalizedQuestions.find((question) => !question.text || question.options.length < 2 || question.correct === '');
    if (invalidQuestion) {
      return res.status(400).json({
        success: false,
        error: 'У каждого вопроса должны быть текст, минимум два варианта ответа и правильный вариант'
      });
    }

    if (departmentId) {
      const [departmentRows] = await pool.query(
        'SELECT Department_ID FROM department WHERE Department_ID = ? AND D_is_active = TRUE LIMIT 1',
        [departmentId]
      );

      if (departmentRows.length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Выбранный отдел не найден'
        });
      }
    }

    const [result] = await pool.query(
      `INSERT INTO opros (
        O_title,
        O_description,
        O_created_by,
        O_department_id,
        O_type,
        O_questions,
        O_is_active,
        O_start_date,
        O_end_date
      ) VALUES (?, ?, ?, ?, 'test', ?, TRUE, NOW(), ?)`,
      [title, description || null, req.user.userId, departmentId, JSON.stringify(normalizedQuestions), endDate || null]
    );

    const [recipientRows] = await pool.query(
      `SELECT User_ID
       FROM user
       WHERE U_is_active = TRUE
         AND (? IS NULL OR Department_ID = ?)`,
      [departmentId, departmentId]
    );

    for (const recipient of recipientRows) {
      await createNotification(pool, {
        userId: recipient.User_ID,
        type: 'test_assigned',
        title: 'Новый тест',
        message: `Вам доступен новый тест "${title}"`,
        link: '/surveys'
      });
    }

    res.status(201).json({
      success: true,
      message: 'Тест создан',
      data: {
        id: result.insertId
      }
    });
  } catch (error) {
    console.error('Create test error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при создании теста'
    });
  }
};

exports.completeTask = async (req, res) => {
  const taskId = Number(req.params.id);

  if (!taskId) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный идентификатор задачи'
    });
  }

  try {
    await ensureNotificationsTable();
    const [taskRows] = await pool.query(
      `SELECT Task_ID, T_assignee_user_id, T_status
       FROM task
       WHERE Task_ID = ?
       LIMIT 1`,
      [taskId]
    );

    const task = taskRows[0];

    if (!task) {
      return res.status(404).json({
        success: false,
        error: 'Задача не найдена'
      });
    }

    if (Number(task.T_assignee_user_id) !== Number(req.user.userId)) {
      return res.status(403).json({
        success: false,
        error: 'Вы можете завершать только свои задачи'
      });
    }

    if (task.T_status === 'done') {
      return res.json({
        success: true,
        message: 'Задача уже была завершена'
      });
    }

    if (task.T_status === 'cancelled') {
      return res.status(400).json({
        success: false,
        error: 'Отменённую задачу нельзя завершить'
      });
    }

    await pool.query(
      `UPDATE task
       SET T_status = 'done',
           T_completed_at = NOW(),
           T_updated = NOW()
       WHERE Task_ID = ?`,
      [taskId]
    );

    const [creatorRows] = await pool.query(
      `SELECT T_creator_id, T_title
       FROM task
       WHERE Task_ID = ?
       LIMIT 1`,
      [taskId]
    );

    const creator = creatorRows[0];
    if (creator?.T_creator_id) {
      await createNotification(pool, {
        userId: creator.T_creator_id,
        type: 'task_completed',
        title: 'Задача выполнена',
        message: `Сотрудник завершил задачу "${creator.T_title}"`,
        link: '/tasks'
      });
    }

    res.json({
      success: true,
      message: 'Задача отмечена как завершённая'
    });
  } catch (error) {
    console.error('Complete task error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при завершении задачи'
    });
  }
};

exports.createForumPost = async (req, res) => {
  const topicId = Number(req.params.id);
  const content = `${req.body.content || ''}`.trim();

  if (!topicId) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный идентификатор темы'
    });
  }

  if (!content || content.length < 1) {
    return res.status(400).json({
      success: false,
      error: 'Сообщение не должно быть пустым'
    });
  }

  let connection;

  try {
    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [topicRows] = await connection.query(
      `SELECT Forum_them_ID, FT_is_locked
       FROM forum_them
       WHERE Forum_them_ID = ?
       LIMIT 1`,
      [topicId]
    );

    const topic = topicRows[0];

    if (!topic) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        error: 'Тема форума не найдена'
      });
    }

    if (topic.FT_is_locked) {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        error: 'Тема закрыта для новых сообщений'
      });
    }

    await connection.query(
      `INSERT INTO forum_posts (Forum_them_ID, Author_ID, FP_content)
       VALUES (?, ?, ?)`,
      [topicId, req.user.userId, content]
    );

    await connection.query(
      `UPDATE forum_them
       SET FT_posts_count = FT_posts_count + 1,
           FT_updated = NOW()
       WHERE Forum_them_ID = ?`,
      [topicId]
    );

    await connection.commit();

    res.status(201).json({
      success: true,
      message: 'Сообщение на форуме отправлено'
    });
  } catch (error) {
    if (connection) {
      await connection.rollback();
    }

    console.error('Create forum post error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при отправке сообщения на форум'
    });
  } finally {
    if (connection) {
      connection.release();
    }
  }
};

exports.updateAppeal = async (req, res) => {
  const appealId = Number(req.params.id);
  const { status, response } = req.body;

  if (!appealId) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный идентификатор обращения'
    });
  }

  const normalizedStatus = mapWorkspaceStatusToDb(status);
  const allowedStatuses = ['new', 'in_progress', 'resolved', 'closed'];

  if (!allowedStatuses.includes(normalizedStatus)) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный статус обращения'
    });
  }

  const trimmedResponse = `${response || ''}`.trim();
  let connection;

  try {
    await ensureAppealMessagesTable();
    await ensureNotificationsTable();
    connection = await pool.getConnection();
    await connection.beginTransaction();

    const { currentUser, appeal } = await fetchAppealAccessContext(connection, appealId, req.user.userId);

    if (!currentUser) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        error: 'Пользователь не найден'
      });
    }

    if (!isAppealManager(currentUser.R_name)) {
      await connection.rollback();
      return res.status(403).json({
        success: false,
        error: 'Недостаточно прав для изменения обращения'
      });
    }

    if (!appeal) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        error: 'Обращение не найдено'
      });
    }

    let insertedMessage = null;

    if (trimmedResponse) {
      const [insertResult] = await connection.query(
        `INSERT INTO appeal_message (Appeal_ID, Author_ID, AM_content)
         VALUES (?, ?, ?)`,
        [appealId, req.user.userId, trimmedResponse]
      );

      insertedMessage = await fetchInsertedMessage(connection, insertResult.insertId);
    }

    await connection.query(
      `UPDATE appeal
       SET A_status = ?,
           A_response = CASE WHEN ? <> '' THEN ? ELSE A_response END,
           A_responder_id = ?,
           A_closed_at = CASE
             WHEN ? IN ('resolved', 'closed') THEN COALESCE(A_closed_at, NOW())
             ELSE NULL
           END
       WHERE Appeal_ID = ?`,
      [normalizedStatus, trimmedResponse, trimmedResponse, req.user.userId, normalizedStatus, appealId]
    );

    await createNotification(connection, {
      userId: appeal.User_ID,
      type: 'appeal_updated',
      title: 'Обращение обновлено',
      message: trimmedResponse
        ? 'По вашему обращению поступил новый ответ'
        : `Статус обращения изменён на ${mapAppealStatus(normalizedStatus)}`,
      link: '/appeals'
    });

    await connection.commit();

    res.json({
      success: true,
      message: 'Обращение обновлено',
      data: {
        status: mapAppealStatus(normalizedStatus),
        message: insertedMessage
      }
    });
  } catch (error) {
    if (connection) {
      await connection.rollback();
    }

    console.error('Update appeal error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при обновлении обращения'
    });
  } finally {
    if (connection) {
      connection.release();
    }
  }
};

exports.sendAppealMessage = async (req, res) => {
  const appealId = Number(req.params.id);
  const content = `${req.body.content || ''}`.trim();

  if (!appealId) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный идентификатор обращения'
    });
  }

  if (!content) {
    return res.status(400).json({
      success: false,
      error: 'Сообщение не должно быть пустым'
    });
  }

  let connection;

  try {
    await ensureAppealMessagesTable();
    await ensureNotificationsTable();
    connection = await pool.getConnection();
    await connection.beginTransaction();

    const { currentUser, appeal } = await fetchAppealAccessContext(connection, appealId, req.user.userId);

    if (!currentUser) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        error: 'Пользователь не найден'
      });
    }

    if (!appeal) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        error: 'Обращение не найдено'
      });
    }

    const manager = isAppealManager(currentUser.R_name);
    const isOwner = Number(appeal.User_ID) === Number(req.user.userId);

    if (!manager && !isOwner) {
      await connection.rollback();
      return res.status(403).json({
        success: false,
        error: 'Недостаточно прав для переписки по этому обращению'
      });
    }

    if (appeal.A_status === 'closed') {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        error: 'Закрытое обращение нельзя продолжить в чате'
      });
    }

    const [insertResult] = await connection.query(
      `INSERT INTO appeal_message (Appeal_ID, Author_ID, AM_content)
       VALUES (?, ?, ?)`,
      [appealId, req.user.userId, content]
    );

    const nextStatus = manager
      ? (appeal.A_status === 'new' ? 'in_progress' : appeal.A_status)
      : (appeal.A_status === 'resolved' ? 'in_progress' : appeal.A_status);

    await connection.query(
      `UPDATE appeal
       SET A_status = ?,
           A_response = CASE WHEN ? THEN ? ELSE A_response END,
           A_responder_id = CASE WHEN ? THEN ? ELSE A_responder_id END,
           A_closed_at = CASE
             WHEN ? IN ('resolved', 'closed') THEN COALESCE(A_closed_at, NOW())
             ELSE NULL
           END
       WHERE Appeal_ID = ?`,
      [nextStatus, manager ? 1 : 0, content, manager ? 1 : 0, req.user.userId, nextStatus, appealId]
    );

    await createNotification(connection, {
      userId: manager ? appeal.User_ID : (appeal.A_responder_id || null),
      type: 'appeal_message',
      title: manager ? 'Ответ по обращению' : 'Новое сообщение по обращению',
      message: content.length > 80 ? `${content.slice(0, 80)}...` : content,
      link: '/appeals'
    });

    const message = await fetchInsertedMessage(connection, insertResult.insertId);

    await connection.commit();

    res.status(201).json({
      success: true,
      message: 'Сообщение отправлено',
      data: {
        message,
        status: mapAppealStatus(nextStatus)
      }
    });
  } catch (error) {
    if (connection) {
      await connection.rollback();
    }

    console.error('Send appeal message error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при отправке сообщения'
    });
  } finally {
    if (connection) {
      connection.release();
    }
  }
};

exports.submitSurvey = async (req, res) => {
  const surveyId = Number(req.params.id);
  const answers = req.body.answers;

  if (!surveyId) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный идентификатор опроса'
    });
  }

  try {
    await ensureNotificationsTable();
    const [surveyRows] = await pool.query(
      `SELECT Opros_ID, O_title, O_created_by, O_type, O_questions, O_is_active, O_start_date, O_end_date
       FROM opros
       WHERE Opros_ID = ?
       LIMIT 1`,
      [surveyId]
    );

    if (!surveyRows.length) {
      return res.status(404).json({
        success: false,
        error: 'Опрос не найден'
      });
    }

    const survey = surveyRows[0];
    const now = new Date();

    if (!survey.O_is_active) {
      return res.status(400).json({
        success: false,
        error: 'Этот опрос уже закрыт'
      });
    }

    if (survey.O_start_date && new Date(survey.O_start_date) > now) {
      return res.status(400).json({
        success: false,
        error: 'Опрос ещё не доступен для прохождения'
      });
    }

    if (survey.O_end_date && new Date(survey.O_end_date) < now) {
      return res.status(400).json({
        success: false,
        error: 'Срок прохождения опроса истёк'
      });
    }

    const questions = safeJsonParse(survey.O_questions, []);
    const normalizedAnswers = normalizeSurveyAnswers(questions, answers);
    const missingRequiredQuestion = questions.find((question) => {
      if (!question.required) {
        return false;
      }

      const currentAnswer = normalizedAnswers.find((item) => Number(item.question_id) === Number(question.id));
      return !currentAnswer || isEmptyAnswer(currentAnswer.answer);
    });

    if (missingRequiredQuestion) {
      return res.status(400).json({
        success: false,
        error: `Заполните обязательный вопрос: ${missingRequiredQuestion.text}`
      });
    }

    const score = calculateSurveyScore(survey.O_type, questions, normalizedAnswers);

    await pool.query(
      `INSERT INTO opros_result (
        Opros_ID, User_ID, OR_answers, SR_score, OR_is_completed, OR_started_at, OR_submitted_at, OR_ip_address, OR_device_info
      ) VALUES (?, ?, ?, ?, 1, NOW(), NOW(), ?, ?)
      ON DUPLICATE KEY UPDATE
        OR_answers = VALUES(OR_answers),
        SR_score = VALUES(SR_score),
        OR_is_completed = 1,
        OR_submitted_at = NOW(),
        OR_ip_address = VALUES(OR_ip_address),
        OR_device_info = VALUES(OR_device_info)`,
      [
        surveyId,
        req.user.userId,
        JSON.stringify(normalizedAnswers),
        score,
        req.ip || null,
        `${req.headers['user-agent'] || ''}`.slice(0, 100) || null
      ]
    );

    if (survey.O_created_by) {
      await createNotification(pool, {
        userId: survey.O_created_by,
        type: 'survey_completed',
        title: survey.O_type === 'test' ? 'Тест пройден' : 'Опрос заполнен',
        message: survey.O_type === 'test'
          ? `Сотрудник завершил тест "${survey.O_title}" с результатом ${score}%`
          : `Сотрудник отправил ответы по опросу "${survey.O_title}"`,
        link: '/surveys'
      });
    }

    res.json({
      success: true,
      message: 'Опрос сохранён',
      data: {
        score,
        answers: normalizedAnswers
      }
    });
  } catch (error) {
    console.error('Submit survey error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при сохранении опроса'
    });
  }
};

exports.startCourse = async (req, res) => {
  const courseId = Number(req.params.id);

  if (!courseId) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный идентификатор курса'
    });
  }

  try {
    const [courseRows] = await pool.query(
      `SELECT Course_ID, C_content_structure, C_is_published
       FROM course
       WHERE Course_ID = ?
       LIMIT 1`,
      [courseId]
    );

    if (!courseRows.length) {
      return res.status(404).json({
        success: false,
        error: 'Курс не найден'
      });
    }

    const course = courseRows[0];
    if (!course.C_is_published) {
      return res.status(400).json({
        success: false,
        error: 'Курс пока не опубликован'
      });
    }

    const content = safeJsonParse(course.C_content_structure, []);
    const totalModules = Math.max(countCourseModules(content), 1);

    const [existingRows] = await pool.query(
      `SELECT Enrollment_ID, E_current_lesson_index, E_progress_percent, E_completed_lessons, E_total_lessons,
              E_final_score, E_is_completed, E_is_certified, E_enrolled_at, E_last_accessed, E_completed_at
       FROM enrollment
       WHERE User_ID = ? AND Course_ID = ?
       LIMIT 1`,
      [req.user.userId, courseId]
    );

    if (existingRows.length) {
      await pool.query(
        `UPDATE enrollment
         SET E_last_accessed = NOW()
         WHERE Enrollment_ID = ?`,
        [existingRows[0].Enrollment_ID]
      );

      return res.json({
        success: true,
        message: 'Курс уже начат',
        data: {
          enrollment: {
            id: existingRows[0].Enrollment_ID,
            currentLessonIndex: Number(existingRows[0].E_current_lesson_index || 0),
            currentModuleIndex: Number(existingRows[0].E_current_lesson_index || 0),
            progressPercent: Number(existingRows[0].E_progress_percent || 0),
            completedLessons: Number(existingRows[0].E_completed_lessons || 0),
            completedModules: Number(existingRows[0].E_completed_lessons || 0),
            totalLessons: Number(existingRows[0].E_total_lessons || totalModules),
            totalModules: Number(existingRows[0].E_total_lessons || totalModules),
            finalScore: existingRows[0].E_final_score === null ? null : Number(existingRows[0].E_final_score),
            isCompleted: Boolean(existingRows[0].E_is_completed),
            isCertified: Boolean(existingRows[0].E_is_certified),
            enrolledAt: existingRows[0].E_enrolled_at,
            lastAccessed: existingRows[0].E_last_accessed,
            completedAt: existingRows[0].E_completed_at
          }
        }
      });
    }

    const [insertResult] = await pool.query(
      `INSERT INTO enrollment (
        User_ID, Course_ID, E_current_lesson_index, E_progress_percent, E_completed_lessons, E_total_lessons,
        E_final_score, E_is_completed, E_is_certified, E_enrolled_at, E_last_accessed, E_completed_at
      ) VALUES (?, ?, 0, 0, 0, ?, NULL, 0, 0, NOW(), NOW(), NULL)`,
      [req.user.userId, courseId, totalModules]
    );

    await createNotification(pool, {
      userId: assigneeId,
      type: 'task_assigned',
      title: 'Новая задача',
      message: `Вам назначена задача "${title}"`,
      link: '/tasks'
    });

    res.status(201).json({
      success: true,
      message: 'Вы записаны на курс',
      data: {
        enrollment: {
          id: insertResult.insertId,
          currentLessonIndex: 0,
          currentModuleIndex: 0,
          progressPercent: 0,
          completedLessons: 0,
          completedModules: 0,
          totalLessons: totalModules,
          totalModules,
          finalScore: null,
          isCompleted: false,
          isCertified: false,
          enrolledAt: new Date().toISOString(),
          lastAccessed: new Date().toISOString(),
          completedAt: null
        }
      }
    });
  } catch (error) {
    console.error('Start course error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при запуске курса'
    });
  }
};

exports.advanceCourseProgress = async (req, res) => {
  const courseId = Number(req.params.id);

  if (!courseId) {
    return res.status(400).json({
      success: false,
      error: 'Некорректный идентификатор курса'
    });
  }

  try {
    const [courseRows] = await pool.query(
      `SELECT Course_ID, C_content_structure, C_is_published
       FROM course
       WHERE Course_ID = ?
       LIMIT 1`,
      [courseId]
    );

    if (!courseRows.length) {
      return res.status(404).json({
        success: false,
        error: 'Курс не найден'
      });
    }

    if (!courseRows[0].C_is_published) {
      return res.status(400).json({
        success: false,
        error: 'Курс пока не опубликован'
      });
    }

    const totalModules = Math.max(countCourseModules(safeJsonParse(courseRows[0].C_content_structure, [])), 1);
    const [enrollmentRows] = await pool.query(
      `SELECT Enrollment_ID, E_current_lesson_index, E_progress_percent, E_completed_lessons, E_total_lessons, E_is_completed
       FROM enrollment
       WHERE User_ID = ? AND Course_ID = ?
       LIMIT 1`,
      [req.user.userId, courseId]
    );

    if (!enrollmentRows.length) {
      return res.status(400).json({
        success: false,
        error: 'Сначала начните курс'
      });
    }

    const enrollment = enrollmentRows[0];
    if (enrollment.E_is_completed) {
      return res.json({
        success: true,
        message: 'Курс уже завершён',
        data: {
          progressPercent: 100,
          completedLessons: Number(enrollment.E_total_lessons || totalModules),
          completedModules: Number(enrollment.E_total_lessons || totalModules),
          totalLessons: Number(enrollment.E_total_lessons || totalModules),
          totalModules: Number(enrollment.E_total_lessons || totalModules),
          currentLessonIndex: Math.max(Number(enrollment.E_total_lessons || totalModules) - 1, 0),
          currentModuleIndex: Math.max(Number(enrollment.E_total_lessons || totalModules) - 1, 0),
          isCompleted: true
        }
      });
    }

    const nextCompletedLessons = Math.min(Number(enrollment.E_completed_lessons || 0) + 1, Number(enrollment.E_total_lessons || totalModules));
    const nextProgressPercent = Math.round((nextCompletedLessons / Number(enrollment.E_total_lessons || totalModules)) * 100);
    const isCompleted = nextCompletedLessons >= Number(enrollment.E_total_lessons || totalModules);
    const nextLessonIndex = isCompleted
      ? Math.max(Number(enrollment.E_total_lessons || totalModules) - 1, 0)
      : nextCompletedLessons;

    await pool.query(
      `UPDATE enrollment
       SET E_current_lesson_index = ?,
           E_progress_percent = ?,
           E_completed_lessons = ?,
           E_is_completed = ?,
           E_final_score = CASE WHEN ? THEN 100 ELSE E_final_score END,
           E_last_accessed = NOW(),
           E_completed_at = CASE WHEN ? THEN COALESCE(E_completed_at, NOW()) ELSE NULL END
       WHERE Enrollment_ID = ?`,
      [
        nextLessonIndex,
        nextProgressPercent,
        nextCompletedLessons,
        isCompleted ? 1 : 0,
        isCompleted ? 1 : 0,
        isCompleted ? 1 : 0,
        enrollment.Enrollment_ID
      ]
    );

    res.json({
      success: true,
      message: isCompleted ? 'Курс завершён' : 'Прогресс обновлён',
      data: {
        progressPercent: nextProgressPercent,
        completedLessons: nextCompletedLessons,
        completedModules: nextCompletedLessons,
        totalLessons: Number(enrollment.E_total_lessons || totalModules),
        totalModules: Number(enrollment.E_total_lessons || totalModules),
        currentLessonIndex: nextLessonIndex,
        currentModuleIndex: nextLessonIndex,
        isCompleted
      }
    });
  } catch (error) {
    console.error('Advance course progress error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при обновлении прогресса курса'
    });
  }
};
