const { pool } = require('../config/database');

const DEPARTMENT_COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#14b8a6', '#f97316', '#8b5cf6', '#0ea5e9'];
const MANAGER_ROLES = ['hr', 'admin'];

let ensureAppealMessagesTablePromise;

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

    const [employeeRows, departmentRows, taskRows, appealRows, forumRows, courseRows, surveyRows] = await Promise.all([
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
          COALESCE(appeal_stats.appeal_count, 0) AS appeal_count,
          COALESCE(forum_stats.topic_count, 0) AS forum_topic_count,
          COALESCE(survey_stats.survey_count, 0) AS survey_count
        FROM user u
        LEFT JOIN role r ON r.Role_ID = u.Role_ID
        LEFT JOIN department d ON d.Department_ID = u.Department_ID
        LEFT JOIN v_task_kpi tk ON tk.User_ID = u.User_ID
        LEFT JOIN (
          SELECT User_ID, AVG(E_progress_percent) AS avg_progress, SUM(CASE WHEN E_is_completed = 1 THEN 1 ELSE 0 END) AS completed_courses
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
          SELECT User_ID, COUNT(*) AS survey_count
          FROM opros_result
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
          t.Task_ID, t.T_title, t.T_description, t.T_priority, t.T_status, t.T_deadline, t.T_created,
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
          vfa.Forum_them_ID, vfa.FT_name, vfa.author_name, vfa.department, vfa.FT_views_count, vfa.FT_posts_count,
          vfa.FT_created, vfa.FT_is_locked, ft.FT_category
        FROM v_forum_activity vfa
        JOIN forum_them ft ON ft.Forum_them_ID = vfa.Forum_them_ID
        ORDER BY vfa.FT_created DESC`
      ),
      pool.query(
        `SELECT
          c.Course_ID, c.C_name, c.C_description, c.C_category, c.C_estimated_hours, c.C_content_structure,
          c.C_is_published, c.C_created, creator.U_name AS creator_name, creator.U_surname AS creator_surname,
          COUNT(DISTINCT e.Enrollment_ID) AS enrolled_count,
          SUM(CASE WHEN e.E_is_completed = 1 THEN 1 ELSE 0 END) AS completed_count
        FROM course c
        JOIN user creator ON creator.User_ID = c.C_created_by
        LEFT JOIN enrollment e ON e.Course_ID = c.Course_ID
        GROUP BY c.Course_ID
        ORDER BY c.C_created DESC`
      ),
      pool.query(
        `SELECT
          o.Opros_ID, o.O_title, o.O_description, o.O_type, o.O_is_active, o.O_start_date, o.O_end_date,
          creator.U_name AS creator_name, creator.U_surname AS creator_surname,
          COUNT(DISTINCT r.Opros_Result_ID) AS response_count
        FROM opros o
        JOIN user creator ON creator.User_ID = o.O_created_by
        LEFT JOIN opros_result r ON r.Opros_ID = o.Opros_ID
        GROUP BY o.Opros_ID
        ORDER BY o.O_created DESC`
      )
    ]);

    const appealIds = appealRows[0].map((row) => row.Appeal_ID);
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

    const messagesByAppeal = appealMessageRows.reduce((accumulator, row) => {
      const currentMessages = accumulator.get(row.Appeal_ID) || [];
      currentMessages.push(buildPersistedMessage(row));
      accumulator.set(row.Appeal_ID, currentMessages);
      return accumulator;
    }, new Map());

    const employees = employeeRows[0].map((row) => {
      const fullName = formatPersonName(row.U_name, row.U_surname, row.U_lastname);
      const kpiBase = row.completion_rate || row.course_progress || Math.min(100, row.U_points_balance);
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
        completedCourses: Number(row.completed_courses || 0),
        appealCount: Number(row.appeal_count || 0),
        forumTopics: Number(row.forum_topic_count || 0),
        surveyCount: Number(row.survey_count || 0),
        pointsBalance: Number(row.U_points_balance || 0)
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
        title: row.T_title,
        description: row.T_description,
        assignee: row.assignee_name || 'Не назначен',
        department: row.department_name || 'Без отдела',
        deadline: row.T_deadline,
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
        subject: buildAppealSubject(row),
        from: row.author_name,
        department: row.department_name || 'Без отдела',
        date: row.A_created,
        status: mapAppealStatus(row.A_status),
        priority: row.A_priority || 'medium',
        category: row.A_category || row.A_type,
        description: row.A_content,
        response: row.A_response,
        messages,
        lastMessageAt: messages[messages.length - 1]?.createdAt || row.A_created
      };
    });

    const forumPosts = forumRows[0].map((row) => ({
      id: row.Forum_them_ID,
      title: row.FT_name,
      author: row.author_name,
      category: row.FT_category || 'Обсуждение',
      replies: Number(row.FT_posts_count || 0),
      views: Number(row.FT_views_count || 0),
      date: row.FT_created,
      pinned: Boolean(row.FT_is_locked) || row.FT_category === 'Объявления',
      tags: row.FT_category ? row.FT_category.toLowerCase().split(/\s+/).slice(0, 3) : []
    }));

    const courses = courseRows[0].map((row) => {
      const content = safeJsonParse(row.C_content_structure, []);
      const moduleCount = Array.isArray(content) ? content.length : 0;
      return {
        id: row.Course_ID,
        title: row.C_name,
        description: row.C_description,
        duration: row.C_estimated_hours ? `${row.C_estimated_hours} ч` : 'Не указано',
        modules: moduleCount,
        enrolled: Number(row.enrolled_count || 0),
        completed: Number(row.completed_count || 0),
        status: row.C_is_published ? 'active' : 'draft',
        category: row.C_category || 'Обучение',
        instructor: `${row.creator_name} ${row.creator_surname}`.trim()
      };
    });

    const employeeCount = employees.length || 1;
    const surveys = surveyRows[0].map((row) => ({
      id: row.Opros_ID,
      title: row.O_title,
      type: row.O_type === 'feedback' ? 'survey' : row.O_type,
      status: row.O_is_active ? 'active' : 'completed',
      responses: Number(row.response_count || 0),
      total: employeeCount,
      deadline: row.O_end_date,
      createdBy: `${row.creator_name} ${row.creator_surname}`.trim(),
      description: row.O_description
    }));

    const lastMonths = getLastMonths(6);

    const monthlyMap = new Map(lastMonths.map((month) => [month.key, {
      month: month.label,
      hires: 0,
      tasks: 0,
      appeals: 0,
      forum: 0,
      courses: 0
    }]));

    taskRows[0].forEach((row) => {
      const date = new Date(row.T_created);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).tasks += 1;
      }
    });

    appealRows[0].forEach((row) => {
      const date = new Date(row.A_created);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).appeals += 1;
      }
    });

    employeeRows[0].forEach((row) => {
      const date = new Date(row.U_hire_date);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).hires += 1;
      }
    });

    forumRows[0].forEach((row) => {
      const date = new Date(row.FT_created);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).forum += 1;
      }
    });

    courseRows[0].forEach((row) => {
      const date = new Date(row.C_created);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (monthlyMap.has(key)) {
        monthlyMap.get(key).courses += 1;
      }
    });

    const monthlyStats = lastMonths.map((month) => monthlyMap.get(month.key));

    const departmentMonthlyMap = new Map(lastMonths.map((month) => [month.key, { month: month.label }]));

    departments.forEach((department) => {
      lastMonths.forEach((month) => {
        departmentMonthlyMap.get(month.key)[department.code || department.name] = 0;
      });
    });

    taskRows[0].forEach((row) => {
      if (!row.department_name) {
        return;
      }

      const department = departments.find((item) => item.name === row.department_name);
      if (!department) {
        return;
      }

      const date = new Date(row.T_created);
      const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (departmentMonthlyMap.has(key)) {
        departmentMonthlyMap.get(key)[department.code || department.name] += 1;
      }
    });

    const departmentMonthlyStats = lastMonths.map((month) => departmentMonthlyMap.get(month.key));

    res.json({
      success: true,
      data: {
        employees,
        departments,
        tasks,
        appeals,
        forumPosts,
        courses,
        surveys,
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
