import sqlite3
import os
import time
import json
from datetime import datetime


class InvestmentDB:
    def __init__(self, db_path='investment.db'):
        self.db_path = db_path
        self.init_database()

    def get_connection(self):
        """获取数据库连接，添加重试机制"""
        max_retries = 5
        retry_delay = 0.1  # 100ms

        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                conn.row_factory = sqlite3.Row
                # 启用外键约束
                conn.execute("PRAGMA foreign_keys = ON")
                # 启用 WAL 模式，提高并发性能
                conn.execute("PRAGMA journal_mode = WAL")
                return conn
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # 指数退避
                else:
                    raise
        return None

    def init_database(self):
        """初始化数据库表结构"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 创建合伙项目表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,   
                    name TEXT NOT NULL,
                    description TEXT,
                    total_investment REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 创建合伙人表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS partners (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    investment REAL NOT NULL DEFAULT 0,
                    share_percentage REAL DEFAULT 0,
                    contact_info TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                )
            ''')

            # 创建操作日志表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS operation_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER,
                    operation_type TEXT,
                    details TEXT,
                    operated_by TEXT DEFAULT '系统',
                    operated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # 合伙人级别定义表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS partner_tiers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    tier_name TEXT NOT NULL,  -- 级别名称，如：GP, LP, Co-GP
                    description TEXT,
                    management_fee_rate REAL DEFAULT 0,  -- 管理费率（年化%）
                    performance_fee_rate REAL DEFAULT 0,  -- 绩效分成率（carry，%）
                    priority INTEGER DEFAULT 0,  -- 分配优先级（数字越小优先级越高）
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                )
            ''')

            # 合伙人级别分配表（替代原来的partners表中的share_percentage逻辑）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS partner_tier_assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    partner_id INTEGER NOT NULL,
                    tier_id INTEGER NOT NULL,
                    commitment_amount REAL DEFAULT 0,  -- 承诺投资额
                    invested_amount REAL DEFAULT 0,    -- 已投资额
                    distribution_share REAL DEFAULT 0, -- 收益分配比例
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (partner_id) REFERENCES partners (id) ON DELETE CASCADE,
                    FOREIGN KEY (tier_id) REFERENCES partner_tiers (id) ON DELETE CASCADE,
                    UNIQUE(partner_id, tier_id)
                )
            ''')

            # 收益分配记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS distributions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    distribution_date DATE NOT NULL,
                    total_amount REAL NOT NULL,  -- 分配总额
                    description TEXT,
                    details_json TEXT,  -- 详细的分配计算明细
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                )
            ''')

            # 设备管理表
            cursor.execute('''
                       CREATE TABLE IF NOT EXISTS equipment (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           project_id INTEGER NOT NULL,
                           equipment_name TEXT NOT NULL,
                           equipment_type TEXT NOT NULL,
                           specification TEXT,
                           quantity INTEGER DEFAULT 1,
                           unit_price REAL DEFAULT 0,
                           total_price REAL DEFAULT 0,
                           purchase_date DATE,
                           supplier TEXT,
                           warranty_period INTEGER, -- 保修期（月）
                           status TEXT DEFAULT '正常', -- 正常、维修、报废
                           location TEXT,
                           notes TEXT,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                       )
                   ''')

            # 装修项目管理表
            cursor.execute('''
                       CREATE TABLE IF NOT EXISTS decoration_items (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           project_id INTEGER NOT NULL,
                           item_name TEXT NOT NULL,
                           item_type TEXT NOT NULL, -- 硬装、软装、电气、管道
                           area TEXT, -- 区域：大堂、包间、走廊、卫生间等
                           specification TEXT,
                           unit TEXT DEFAULT '项',
                           quantity REAL DEFAULT 1,
                           unit_price REAL DEFAULT 0,
                           total_price REAL DEFAULT 0,
                           contractor TEXT,
                           start_date DATE,
                           end_date DATE,
                           status TEXT DEFAULT '未开始', -- 未开始、进行中、已完成
                           notes TEXT,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                       )
                   ''')

            # 物料库存表
            cursor.execute('''
                       CREATE TABLE IF NOT EXISTS inventory (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           project_id INTEGER NOT NULL,
                           item_name TEXT NOT NULL,
                           category TEXT NOT NULL, -- 洗浴用品、饮品、小吃、清洁用品
                           specification TEXT,
                           unit TEXT NOT NULL,
                           stock_quantity REAL DEFAULT 0,
                           min_quantity REAL DEFAULT 0, -- 最小库存量
                           unit_price REAL DEFAULT 0,
                           supplier TEXT,
                           last_purchase_date DATE,
                           expiration_date DATE,
                           notes TEXT,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                       )
                   ''')

            # 成本预算表
            cursor.execute('''
                       CREATE TABLE IF NOT EXISTS cost_budget (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           project_id INTEGER NOT NULL,
                           category TEXT NOT NULL, -- 设备、装修、物料、人工、租金、水电等
                           item_name TEXT NOT NULL,
                           budget_amount REAL DEFAULT 0,
                           actual_amount REAL DEFAULT 0,
                           variance REAL DEFAULT 0, -- 差异
                           status TEXT DEFAULT '预算中',
                           notes TEXT,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                       )
                   ''')

            # 收入预测表（针对足浴店特点）
            cursor.execute('''
                       CREATE TABLE IF NOT EXISTS revenue_forecast (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           project_id INTEGER NOT NULL,
                           forecast_month DATE NOT NULL,
                           forecast_type TEXT NOT NULL, -- 足浴、按摩、SPA、会员卡
                           unit_price REAL DEFAULT 0,
                           expected_quantity INTEGER DEFAULT 0,
                           expected_revenue REAL DEFAULT 0,
                           notes TEXT,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                       )
                   ''')

            # 房间/包间管理表
            cursor.execute('''
                       CREATE TABLE IF NOT EXISTS rooms (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           project_id INTEGER NOT NULL,
                           room_number TEXT NOT NULL,
                           room_type TEXT NOT NULL, -- 普通、VIP、豪华
                           capacity INTEGER DEFAULT 1, -- 可容纳人数
                           area REAL DEFAULT 0, -- 面积（平方米）
                           hourly_rate REAL DEFAULT 0, -- 小时费率
                           status TEXT DEFAULT '空闲', -- 空闲、使用中、清洁中、维修中
                           equipment_list TEXT, -- 配备的设备清单
                           notes TEXT,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
                           UNIQUE(project_id, room_number)
                       )
                   ''')

            # ================== 人员管理相关表 ==================

            # 员工基本信息表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS employees (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                project_id INTEGER NOT NULL,
                                employee_number TEXT UNIQUE,  -- 员工编号
                                name TEXT NOT NULL,
                                gender TEXT CHECK(gender IN ('男', '女', '其他')),
                                birth_date DATE,
                                id_card TEXT,  -- 身份证号
                                phone TEXT,
                                emergency_contact TEXT,  -- 紧急联系人
                                emergency_phone TEXT,  -- 紧急联系人电话
                                address TEXT,  -- 住址
                                position TEXT NOT NULL,  -- 职位：店长、技师、前台、清洁等
                                employment_date DATE NOT NULL,  -- 入职日期
                                status TEXT DEFAULT '在职' CHECK(status IN ('在职', '离职', '休假')),
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                            )
                        ''')

            # 员工技能/资质表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS employee_skills (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                employee_id INTEGER NOT NULL,
                                skill_type TEXT NOT NULL,  -- 技能类型：足浴、按摩、SPA等
                                skill_level TEXT,  -- 技能等级：初级、中级、高级、技师等
                                certification TEXT,  -- 证书名称
                                certification_date DATE,  -- 获证日期
                                experience_years INTEGER DEFAULT 0,  -- 从业年限
                                rating REAL DEFAULT 0,  -- 评分（0-5）
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE,
                                UNIQUE(employee_id, skill_type)
                            )
                        ''')

            # 排班表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS schedules (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                project_id INTEGER NOT NULL,
                                employee_id INTEGER NOT NULL,
                                schedule_date DATE NOT NULL,
                                shift_type TEXT NOT NULL,  -- 班次：早班、中班、晚班、全天
                                start_time TIME NOT NULL,
                                end_time TIME NOT NULL,
                                rest_hours REAL DEFAULT 1.0,  -- 休息时长（小时）
                                room_assignment TEXT,  -- 分配的房间
                                status TEXT DEFAULT '已安排',  -- 已安排、完成、请假、调班
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
                                FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE,
                                UNIQUE(employee_id, schedule_date, shift_type)
                            )
                        ''')

            # 考勤记录表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS attendance (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                schedule_id INTEGER NOT NULL,
                                check_in_time TIMESTAMP,  -- 上班打卡时间
                                check_out_time TIMESTAMP,  -- 下班打卡时间
                                actual_hours REAL DEFAULT 0,  -- 实际工作时长
                                late_minutes INTEGER DEFAULT 0,  -- 迟到分钟数
                                early_leave_minutes INTEGER DEFAULT 0,  -- 早退分钟数
                                attendance_status TEXT DEFAULT '正常',  -- 正常、迟到、早退、旷工、请假
                                notes TEXT,
                                recorded_by TEXT DEFAULT '系统',  -- 记录人
                                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (schedule_id) REFERENCES schedules (id) ON DELETE CASCADE
                            )
                        ''')

            # 薪资结构表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS salary_structure (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                project_id INTEGER NOT NULL,
                                position TEXT NOT NULL,  -- 职位
                                base_salary REAL DEFAULT 0,  -- 基本工资
                                hourly_rate REAL DEFAULT 0,  -- 时薪
                                commission_rate REAL DEFAULT 0,  -- 提成比例（%）
                                overtime_rate REAL DEFAULT 1.5,  -- 加班费率倍数
                                allowance REAL DEFAULT 0,  -- 补贴
                                social_insurance_rate REAL DEFAULT 0,  -- 社保比例
                                housing_fund_rate REAL DEFAULT 0,  -- 公积金比例
                                effective_date DATE NOT NULL,  -- 生效日期
                                is_active INTEGER DEFAULT 1,  -- 是否有效
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
                                UNIQUE(project_id, position, effective_date)
                            )
                        ''')

            # 薪资发放记录表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS salary_records (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                project_id INTEGER NOT NULL,
                                employee_id INTEGER NOT NULL,
                                salary_month DATE NOT NULL,  -- 薪资月份
                                base_salary REAL DEFAULT 0,  -- 基本工资
                                work_hours REAL DEFAULT 0,  -- 工作时长
                                hourly_wage REAL DEFAULT 0,  -- 时薪工资
                                overtime_hours REAL DEFAULT 0,  -- 加班时长
                                overtime_pay REAL DEFAULT 0,  -- 加班费
                                commission_amount REAL DEFAULT 0,  -- 提成
                                allowance REAL DEFAULT 0,  -- 补贴
                                performance_bonus REAL DEFAULT 0,  -- 绩效奖金
                                total_income REAL DEFAULT 0,  -- 总收入
                                social_insurance REAL DEFAULT 0,  -- 社保扣款
                                housing_fund REAL DEFAULT 0,  -- 公积金扣款
                                tax REAL DEFAULT 0,  -- 个税
                                other_deductions REAL DEFAULT 0,  -- 其他扣款
                                net_salary REAL DEFAULT 0,  -- 实发工资
                                payment_date DATE,  -- 发放日期
                                payment_status TEXT DEFAULT '未发放',  -- 未发放、已发放、已到账
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
                                FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE,
                                UNIQUE(employee_id, salary_month)
                            )
                        ''')

            # 绩效评估表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS performance_reviews (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                employee_id INTEGER NOT NULL,
                                review_date DATE NOT NULL,  -- 评估日期
                                review_period TEXT NOT NULL,  -- 评估周期：月度、季度、年度
                                reviewer_id INTEGER,  -- 评估人ID
                                reviewer_name TEXT,  -- 评估人姓名
                                service_quality_score INTEGER DEFAULT 0,  -- 服务质量评分
                                customer_feedback_score INTEGER DEFAULT 0,  -- 客户反馈评分
                                attendance_score INTEGER DEFAULT 0,  -- 考勤评分
                                efficiency_score INTEGER DEFAULT 0,  -- 工作效率评分
                                team_cooperation_score INTEGER DEFAULT 0,  -- 团队合作评分
                                total_score REAL DEFAULT 0,  -- 总分
                                rating_level TEXT DEFAULT '一般',  -- 评级：优秀、良好、一般、待改进
                                strengths TEXT,  -- 优点
                                areas_for_improvement TEXT,  -- 待改进方面
                                development_plan TEXT,  -- 发展计划
                                promotion_recommendation INTEGER DEFAULT 0,  -- 是否推荐晋升
                                salary_adjustment_percentage REAL DEFAULT 0,  -- 薪资调整百分比
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE,
                                FOREIGN KEY (reviewer_id) REFERENCES employees (id) ON DELETE SET NULL
                            )
                        ''')

            # 培训记录表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS training_records (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                project_id INTEGER NOT NULL,
                                training_name TEXT NOT NULL,  -- 培训名称
                                training_type TEXT NOT NULL,  -- 培训类型：岗前培训、技能提升、安全培训
                                trainer TEXT,  -- 培训师
                                training_date DATE NOT NULL,  -- 培训日期
                                duration_hours REAL DEFAULT 1.0,  -- 培训时长
                                location TEXT,  -- 培训地点
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
                            )
                        ''')

            # 培训参与记录表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS training_participants (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                training_id INTEGER NOT NULL,
                                employee_id INTEGER NOT NULL,
                                attendance_status TEXT DEFAULT '参加',  -- 参加、请假、缺席
                                test_score REAL,  -- 测试成绩
                                feedback TEXT,  -- 反馈
                                certification TEXT,  -- 获得的证书
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (training_id) REFERENCES training_records (id) ON DELETE CASCADE,
                                FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE,
                                UNIQUE(training_id, employee_id)
                            )
                        ''')

            # 员工离职记录表
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS employee_resignation (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                employee_id INTEGER NOT NULL,
                                resignation_date DATE NOT NULL,  -- 离职日期
                                resignation_type TEXT NOT NULL,  -- 离职类型：辞职、解雇、退休
                                reason TEXT,  -- 离职原因
                                exit_interview_notes TEXT,  -- 离职面谈记录
                                handover_completed INTEGER DEFAULT 0,  -- 交接是否完成
                                last_working_date DATE,  -- 最后工作日
                                notes TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (employee_id) REFERENCES employees (id) ON DELETE CASCADE
                            )
                        ''')

            # 创建索引提高查询性能
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_partners_project_id ON partners(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_project_id ON operation_logs(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_operated_at ON operation_logs(operated_at)')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tiers_project_id ON partner_tiers(project_id)')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_assignments_partner_id ON partner_tier_assignments(partner_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assignments_tier_id ON partner_tier_assignments(tier_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_distributions_project_id ON distributions(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_distributions_date ON distributions(distribution_date)')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_equipment_project_id ON equipment(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_decoration_project_id ON decoration_items(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_inventory_project_id ON inventory(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rooms_project_id ON rooms(project_id)')

            # 创建人员管理相关的索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_employees_project_id ON employees(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_employees_status ON employees(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_skills_employee_id ON employee_skills(employee_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_schedules_project_id ON schedules(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_schedules_employee_id ON schedules(employee_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_schedules_date ON schedules(schedule_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_schedule_id ON attendance(schedule_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_salary_project_id ON salary_structure(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_salary_records_employee_id ON salary_records(employee_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_salary_records_month ON salary_records(salary_month)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_performance_employee_id ON performance_reviews(employee_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_training_project_id ON training_records(project_id)')

            conn.commit()

    def create_project(self, name, description=""):
        """创建新项目"""
        if not name or not name.strip():
            raise ValueError("项目名称不能为空")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO projects (name, description) VALUES (?, ?)',
                (name.strip(), description.strip())
            )
            project_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'CREATE_PROJECT', f'创建项目: {name.strip()}')
            )

            conn.commit()
            return project_id

    def get_all_projects(self):
        """获取所有项目"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM projects ORDER BY created_at DESC')
            return [dict(row) for row in cursor.fetchall()]

    def get_project(self, project_id):
        """获取单个项目信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def add_partner(self, project_id, name, investment, contact_info="", notes=""):
        """添加合伙人到项目"""
        if not name or not name.strip():
            raise ValueError("合伙人姓名不能为空")

        if investment is None:
            raise ValueError("投资金额不能为空")

        try:
            investment_float = float(investment)
            if investment_float < 0:
                raise ValueError("投资金额不能为负数")
        except (ValueError, TypeError):
            raise ValueError("投资金额必须是有效的数字")

        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                '''INSERT INTO partners (project_id, name, investment, contact_info, notes) 
                   VALUES (?, ?, ?, ?, ?)''',
                (project_id, name.strip(), investment_float, contact_info.strip(), notes.strip())
            )
            partner_id = cursor.lastrowid

            # 更新项目总投资额
            self._update_project_total(conn, project_id)

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_PARTNER', f'添加合伙人: {name.strip()}, 投资额: {investment_float:.2f}')
            )

            conn.commit()
            return partner_id

    def get_project_partners(self, project_id):
        """获取项目的所有合伙人"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT * FROM partners WHERE project_id = ? ORDER BY id',
                (project_id,)
            )
            partners = [dict(row) for row in cursor.fetchall()]

            # 确保返回的investment是浮点数
            for partner in partners:
                partner['investment'] = float(partner['investment'])

            return partners

    def update_partner(self, partner_id, name=None, investment=None, contact_info=None, notes=None):
        """更新合伙人信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取旧信息用于日志
            cursor.execute('SELECT * FROM partners WHERE id = ?', (partner_id,))
            old_partner = cursor.fetchone()

            if not old_partner:
                raise ValueError("合伙人不存在")

            updates = []
            params = []
            investment_float = None

            if name is not None:
                if not name.strip():
                    raise ValueError("合伙人姓名不能为空")
                updates.append('name = ?')
                params.append(name.strip())
            if investment is not None:
                try:
                    investment_float = float(investment)
                    if investment_float < 0:
                        raise ValueError("投资金额不能为负数")
                except (ValueError, TypeError):
                    raise ValueError("投资金额必须是有效的数字")
                updates.append('investment = ?')
                params.append(investment_float)
            if contact_info is not None:
                updates.append('contact_info = ?')
                params.append(contact_info.strip())
            if notes is not None:
                updates.append('notes = ?')
                params.append(notes.strip())

            if updates:
                params.append(partner_id)
                query = f'UPDATE partners SET {", ".join(updates)} WHERE id = ?'
                cursor.execute(query, params)

                # 获取项目ID用于更新总投资额
                project_id = old_partner['project_id']

                # 更新项目总投资额
                self._update_project_total(conn, project_id)

                # 记录操作日志
                details = f'更新合伙人信息: {old_partner["name"]}'
                if investment is not None:
                    details += f', 投资额: {old_partner["investment"]} -> {investment_float:.2f}'
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (project_id, 'UPDATE_PARTNER', details)
                )

                conn.commit()

    def delete_partner(self, partner_id):
        """删除合伙人"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取合伙人信息用于日志
            cursor.execute('SELECT * FROM partners WHERE id = ?', (partner_id,))
            partner = cursor.fetchone()

            if partner:
                # 先检查是否只剩一个合伙人
                cursor.execute(
                    'SELECT COUNT(*) as count FROM partners WHERE project_id = ?',
                    (partner['project_id'],)
                )
                count = cursor.fetchone()['count']

                if count <= 1:
                    raise ValueError("至少需要保留一个合伙人")

                cursor.execute('DELETE FROM partners WHERE id = ?', (partner_id,))

                # 更新项目总投资额
                self._update_project_total(conn, partner['project_id'])

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (partner['project_id'], 'DELETE_PARTNER', f'删除合伙人: {partner["name"]}')
                )

                conn.commit()

    def _update_project_total(self, conn, project_id):
        """更新项目的总投资额和重新计算份额"""
        cursor = conn.cursor()

        # 计算总投资额
        cursor.execute(
            'SELECT SUM(investment) as total FROM partners WHERE project_id = ?',
            (project_id,)
        )
        result = cursor.fetchone()
        # 修复：正确处理空值情况
        if result and result['total'] is not None:
            total_investment = float(result['total'])
        else:
            total_investment = 0.0

        # 更新项目总投资额
        cursor.execute(
            'UPDATE projects SET total_investment = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
            (total_investment, project_id)
        )

        # 重新计算每个合伙人的份额
        if total_investment > 0:
            cursor.execute(
                'SELECT id, investment FROM partners WHERE project_id = ?',
                (project_id,)
            )
            partners = cursor.fetchall()

            for partner in partners:
                investment_float = float(partner['investment'])
                share_percentage = (investment_float / total_investment) * 100
                cursor.execute(
                    'UPDATE partners SET share_percentage = ? WHERE id = ?',
                    (share_percentage, partner['id'])
                )
        else:
            # 如果没有投资，将所有合伙人的份额设为0
            cursor.execute(
                'UPDATE partners SET share_percentage = 0 WHERE project_id = ?',
                (project_id,)
            )

    def delete_project(self, project_id):
        """删除项目及其所有合伙人"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取项目信息用于日志
            cursor.execute('SELECT name FROM projects WHERE id = ?', (project_id,))
            project = cursor.fetchone()

            if project:
                # 删除项目（级联删除合伙人）
                cursor.execute('DELETE FROM projects WHERE id = ?', (project_id,))

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (project_id, 'DELETE_PROJECT', f'删除项目: {project["name"]}')
                )

                conn.commit()

    def get_operation_logs(self, project_id=None, limit=50):
        """获取操作日志"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if project_id:
                cursor.execute(
                    '''SELECT * FROM operation_logs 
                       WHERE project_id = ? 
                       ORDER BY operated_at DESC 
                       LIMIT ?''',
                    (project_id, limit)
                )
            else:
                cursor.execute(
                    '''SELECT * FROM operation_logs 
                       ORDER BY operated_at DESC 
                       LIMIT ?''',
                    (limit,)
                )

            return [dict(row) for row in cursor.fetchall()]

    # ================== 多级合伙人系统方法 ==================

    def create_partner_tier(self, project_id, tier_name, description="",
                            management_fee_rate=0, performance_fee_rate=0, priority=0):
        """创建合伙人级别"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO partner_tiers 
                (project_id, tier_name, description, management_fee_rate, performance_fee_rate, priority)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (project_id, tier_name, description, management_fee_rate, performance_fee_rate, priority))

            tier_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'CREATE_TIER', f'创建合伙人级别: {tier_name}')
            )

            conn.commit()
            return tier_id

    def update_partner_tier(self, tier_id, tier_name=None, description=None,
                           management_fee_rate=None, performance_fee_rate=None, priority=None):
        """更新合伙人级别信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取旧信息用于日志
            cursor.execute('SELECT * FROM partner_tiers WHERE id = ?', (tier_id,))
            old_tier = cursor.fetchone()

            if not old_tier:
                raise ValueError("合伙人级别不存在")

            updates = []
            params = []

            if tier_name is not None:
                if not tier_name.strip():
                    raise ValueError("级别名称不能为空")
                updates.append('tier_name = ?')
                params.append(tier_name.strip())
            if description is not None:
                updates.append('description = ?')
                params.append(description.strip())
            if management_fee_rate is not None:
                try:
                    mgmt_rate = float(management_fee_rate)
                    if mgmt_rate < 0 or mgmt_rate > 100:
                        raise ValueError("管理费率必须在0-100之间")
                except (ValueError, TypeError):
                    raise ValueError("管理费率必须是有效的数字")
                updates.append('management_fee_rate = ?')
                params.append(mgmt_rate)
            if performance_fee_rate is not None:
                try:
                    perf_rate = float(performance_fee_rate)
                    if perf_rate < 0 or perf_rate > 100:
                        raise ValueError("绩效分成率必须在0-100之间")
                except (ValueError, TypeError):
                    raise ValueError("绩效分成率必须是有效的数字")
                updates.append('performance_fee_rate = ?')
                params.append(perf_rate)
            if priority is not None:
                try:
                    priority_int = int(priority)
                    if priority_int < 0:
                        raise ValueError("优先级不能为负数")
                except (ValueError, TypeError):
                    raise ValueError("优先级必须是有效的整数")
                updates.append('priority = ?')
                params.append(priority_int)

            if updates:
                params.append(tier_id)
                query = f'UPDATE partner_tiers SET {", ".join(updates)} WHERE id = ?'
                cursor.execute(query, params)

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (old_tier['project_id'], 'UPDATE_TIER', f'更新合伙人级别: {old_tier["tier_name"]}')
                )

                conn.commit()

    def delete_tier(self, tier_id):
        """删除合伙人级别"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取级别信息用于日志
            cursor.execute('SELECT * FROM partner_tiers WHERE id = ?', (tier_id,))
            tier = cursor.fetchone()

            if tier:
                # 检查是否有合伙人分配到此级别
                cursor.execute('SELECT COUNT(*) as count FROM partner_tier_assignments WHERE tier_id = ?', (tier_id,))
                count = cursor.fetchone()['count']

                if count > 0:
                    raise ValueError(f"有{count}个合伙人分配到此级别，无法删除")

                cursor.execute('DELETE FROM partner_tiers WHERE id = ?', (tier_id,))

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (tier['project_id'], 'DELETE_TIER', f'删除合伙人级别: {tier["tier_name"]}')
                )

                conn.commit()

    def get_project_tiers(self, project_id):
        """获取项目的所有合伙人级别"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM partner_tiers 
                WHERE project_id = ? 
                ORDER BY priority, created_at
            ''', (project_id,))
            return [dict(row) for row in cursor.fetchall()]

    def get_tier(self, tier_id):
        """获取单个级别信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM partner_tiers WHERE id = ?', (tier_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def assign_partner_to_tier(self, partner_id, tier_id, commitment_amount=0,
                               distribution_share=0):
        """将合伙人分配到指定级别"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 验证输入数据
            try:
                commitment_amount_float = float(commitment_amount)
                if commitment_amount_float < 0:
                    raise ValueError("承诺金额不能为负数")
            except (ValueError, TypeError):
                raise ValueError("承诺金额必须是有效的数字")

            try:
                distribution_share_float = float(distribution_share)
                if distribution_share_float < 0 or distribution_share_float > 100:
                    raise ValueError("分配比例必须在0-100之间")
            except (ValueError, TypeError):
                raise ValueError("分配比例必须是有效的数字")

            # 检查合伙人是否存在
            cursor.execute('SELECT name, project_id FROM partners WHERE id = ?', (partner_id,))
            partner = cursor.fetchone()
            if not partner:
                raise ValueError("合伙人不存在")

            # 检查级别是否存在
            cursor.execute('SELECT tier_name, project_id FROM partner_tiers WHERE id = ?', (tier_id,))
            tier = cursor.fetchone()
            if not tier:
                raise ValueError("合伙人级别不存在")

            # 检查是否属于同一个项目
            if partner['project_id'] != tier['project_id']:
                raise ValueError("合伙人和级别不属于同一个项目")

            # 检查合伙人是否已分配
            cursor.execute('''
                SELECT id, tier_id FROM partner_tier_assignments 
                WHERE partner_id = ?
            ''', (partner_id,))
            existing = cursor.fetchone()

            if existing:
                # 更新现有分配
                cursor.execute('''
                    UPDATE partner_tier_assignments 
                    SET tier_id = ?, commitment_amount = ?, 
                        distribution_share = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE partner_id = ?
                ''', (tier_id, commitment_amount_float, distribution_share_float, partner_id))
                action = '更新'
            else:
                # 创建新分配
                cursor.execute('''
                    INSERT INTO partner_tier_assignments 
                    (partner_id, tier_id, commitment_amount, distribution_share)
                    VALUES (?, ?, ?, ?)
                ''', (partner_id, tier_id, commitment_amount_float, distribution_share_float))
                action = '分配'

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (partner['project_id'], 'ASSIGN_TIER',
                 f'{action}合伙人 {partner["name"]} 到级别 {tier["tier_name"]}，承诺金额: {commitment_amount_float:.2f}，分配比例: {distribution_share_float:.2f}%')
            )

            conn.commit()

    def get_partner_tier_info(self, partner_id):
        """获取合伙人级别信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    a.*,
                    t.tier_name,
                    t.description,
                    t.management_fee_rate,
                    t.performance_fee_rate,
                    t.priority
                FROM partner_tier_assignments a
                JOIN partner_tiers t ON a.tier_id = t.id
                WHERE a.partner_id = ?
            ''', (partner_id,))

            result = cursor.fetchone()
            return dict(result) if result else None

    def calculate_distribution(self, project_id, distribution_amount):
        """计算收益分配"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取项目信息
            cursor.execute('SELECT total_investment FROM projects WHERE id = ?', (project_id,))
            project = cursor.fetchone()

            # 获取所有合伙人及其级别分配
            cursor.execute('''
                SELECT 
                    p.id as partner_id,
                    p.name,
                    p.investment,
                    t.tier_name,
                    t.management_fee_rate,
                    t.performance_fee_rate,
                    t.priority,
                    COALESCE(a.distribution_share, 0) as distribution_share,
                    COALESCE(a.commitment_amount, 0) as commitment_amount
                FROM partners p
                LEFT JOIN partner_tier_assignments a ON p.id = a.partner_id
                LEFT JOIN partner_tiers t ON a.tier_id = t.id
                WHERE p.project_id = ?
                ORDER BY t.priority, p.id
            ''', (project_id,))

            partners = cursor.fetchall()

            # 计算分配结果
            total_investment = project['total_investment'] if project else 0
            distribution_details = []
            remaining_amount = distribution_amount

            # 第一步：扣除管理费（如果有GP级别）
            gp_tiers = [p for p in partners if p['tier_name'] and 'GP' in p['tier_name'].upper()]
            management_fee = 0

            for gp in gp_tiers:
                if gp['management_fee_rate'] and gp['management_fee_rate'] > 0:
                    fee = distribution_amount * (gp['management_fee_rate'] / 100)
                    management_fee += fee
                    distribution_details.append({
                        'partner_id': gp['partner_id'],
                        'partner_name': gp['name'],
                        'tier': gp['tier_name'],
                        'type': 'management_fee',
                        'amount': fee,
                        'percentage': gp['management_fee_rate']
                    })

            remaining_amount -= management_fee

            # 第二步：优先返还投资本金
            if total_investment > 0:
                return_of_capital = min(remaining_amount, total_investment)
                if return_of_capital > 0:
                    for partner in partners:
                        if partner['investment'] > 0:
                            share = partner['investment'] / total_investment
                            amount = return_of_capital * share

                            distribution_details.append({
                                'partner_id': partner['partner_id'],
                                'partner_name': partner['name'],
                                'tier': partner['tier_name'],
                                'type': 'return_of_capital',
                                'amount': amount,
                                'percentage': share * 100
                            })

                    remaining_amount -= return_of_capital

            # 第三步：绩效分成（Carry）
            if remaining_amount > 0:
                # 首先计算哪些合伙人有权参与绩效分成
                eligible_partners = []

                for partner in partners:
                    if partner['performance_fee_rate'] and partner['performance_fee_rate'] > 0:
                        # 如果有设置绩效分成率
                        eligible_partners.append({
                            'partner': partner,
                            'share': partner['performance_fee_rate'] / 100
                        })
                    elif partner['distribution_share'] and partner['distribution_share'] > 0:
                        # 使用自定义分配比例
                        eligible_partners.append({
                            'partner': partner,
                            'share': partner['distribution_share'] / 100
                        })
                    elif partner['investment'] > 0:
                        # 默认按投资比例分配
                        eligible_partners.append({
                            'partner': partner,
                            'share': partner['investment'] / total_investment
                        })

                # 计算总分配比例
                total_share = sum(p['share'] for p in eligible_partners)

                if total_share > 0:
                    for eligible in eligible_partners:
                        partner = eligible['partner']
                        share = eligible['share'] / total_share  # 归一化
                        amount = remaining_amount * share

                        distribution_details.append({
                            'partner_id': partner['partner_id'],
                            'partner_name': partner['name'],
                            'tier': partner['tier_name'],
                            'type': 'performance_fee',
                            'amount': amount,
                            'percentage': share * 100
                        })

            # 第四步：保存分配记录
            cursor.execute('''
                INSERT INTO distributions 
                (project_id, distribution_date, total_amount, details_json)
                VALUES (?, DATE('now'), ?, ?)
            ''', (project_id, distribution_amount, json.dumps(distribution_details, ensure_ascii=False)))

            distribution_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'CALCULATE_DISTRIBUTION',
                 f'计算收益分配: {distribution_amount:.2f}元')
            )

            conn.commit()

            return {
                'distribution_id': distribution_id,
                'distribution_amount': distribution_amount,
                'management_fee': management_fee,
                'remaining_amount': remaining_amount,
                'details': distribution_details
            }

    def get_distribution_history(self, project_id, limit=50):
        """获取收益分配历史"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM distributions 
                WHERE project_id = ? 
                ORDER BY distribution_date DESC 
                LIMIT ?''', (project_id, limit))

            distributions = []
            for row in cursor.fetchall():
                dist = dict(row)
                if dist['details_json']:
                    dist['details'] = json.loads(dist['details_json'])
                distributions.append(dist)

            return distributions

    # 设备管理方法
    def add_equipment(self, project_id, equipment_name, equipment_type, specification="",
                      quantity=1, unit_price=0, purchase_date=None, supplier="",
                      warranty_period=None, status="正常", location="", notes=""):
        """添加设备"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            total_price = quantity * unit_price

            cursor.execute('''
                INSERT INTO equipment 
                (project_id, equipment_name, equipment_type, specification, quantity, 
                 unit_price, total_price, purchase_date, supplier, warranty_period, 
                 status, location, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, equipment_name, equipment_type, specification, quantity,
                  unit_price, total_price, purchase_date, supplier, warranty_period,
                  status, location, notes))

            equipment_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_EQUIPMENT', f'添加设备: {equipment_name}, 数量: {quantity}, 总价: {total_price:.2f}元')
            )

            # 更新成本预算
            self.update_cost_budget(conn, project_id, '设备', total_price)

            conn.commit()
            return equipment_id

    def get_project_equipment(self, project_id):
        """获取项目所有设备"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM equipment 
                WHERE project_id = ? 
                ORDER BY purchase_date DESC, equipment_name
            ''', (project_id,))
            return [dict(row) for row in cursor.fetchall()]

    def update_equipment(self, equipment_id, equipment_name=None, equipment_type=None,
                         specification=None, quantity=None, unit_price=None,
                         status=None, location=None, notes=None):
        """更新设备信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取原设备信息
            cursor.execute('SELECT * FROM equipment WHERE id = ?', (equipment_id,))
            old_equipment = cursor.fetchone()

            if not old_equipment:
                raise ValueError("设备不存在")

            updates = []
            params = []
            new_total_price = None

            if equipment_name is not None:
                updates.append('equipment_name = ?')
                params.append(equipment_name)
            if equipment_type is not None:
                updates.append('equipment_type = ?')
                params.append(equipment_type)
            if specification is not None:
                updates.append('specification = ?')
                params.append(specification)
            if quantity is not None:
                updates.append('quantity = ?')
                params.append(quantity)
                new_total_price = quantity * (unit_price or old_equipment['unit_price'])
            if unit_price is not None:
                updates.append('unit_price = ?')
                params.append(unit_price)
                new_total_price = (quantity or old_equipment['quantity']) * unit_price
            if status is not None:
                updates.append('status = ?')
                params.append(status)
            if location is not None:
                updates.append('location = ?')
                params.append(location)
            if notes is not None:
                updates.append('notes = ?')
                params.append(notes)

            if new_total_price is not None:
                updates.append('total_price = ?')
                params.append(new_total_price)

            if updates:
                params.append(equipment_id)
                query = f'UPDATE equipment SET {", ".join(updates)} WHERE id = ?'
                cursor.execute(query, params)

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (old_equipment['project_id'], 'UPDATE_EQUIPMENT',
                     f'更新设备信息: {old_equipment["equipment_name"]}')
                )

                conn.commit()

    def delete_equipment(self, equipment_id):
        """删除设备"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取设备信息用于日志
            cursor.execute('SELECT * FROM equipment WHERE id = ?', (equipment_id,))
            equipment = cursor.fetchone()

            if equipment:
                cursor.execute('DELETE FROM equipment WHERE id = ?', (equipment_id,))

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (equipment['project_id'], 'DELETE_EQUIPMENT',
                     f'删除设备: {equipment["equipment_name"]}')
                )

                conn.commit()

    # 装修管理方法
    def add_decoration_item(self, project_id, item_name, item_type, area="", specification="",
                            unit="项", quantity=1, unit_price=0, contractor="",
                            start_date=None, end_date=None, status="未开始", notes=""):
        """添加装修项目"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            total_price = quantity * unit_price

            cursor.execute('''
                INSERT INTO decoration_items 
                (project_id, item_name, item_type, area, specification, unit, 
                 quantity, unit_price, total_price, contractor, start_date, 
                 end_date, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, item_name, item_type, area, specification, unit,
                  quantity, unit_price, total_price, contractor, start_date,
                  end_date, status, notes))

            item_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_DECORATION', f'添加装修项目: {item_name}, 总价: {total_price:.2f}元')
            )

            # 更新成本预算
            self.update_cost_budget(conn, project_id, '装修', total_price)

            conn.commit()
            return item_id

    def get_project_decoration(self, project_id):
        """获取项目所有装修项目"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM decoration_items 
                WHERE project_id = ? 
                ORDER BY status, start_date, item_name
            ''', (project_id,))
            return [dict(row) for row in cursor.fetchall()]

    def update_decoration_item(self, item_id, **kwargs):
        """更新装修项目"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取原信息
            cursor.execute('SELECT * FROM decoration_items WHERE id = ?', (item_id,))
            old_item = cursor.fetchone()

            if not old_item:
                raise ValueError("装修项目不存在")

            allowed_fields = ['item_name', 'item_type', 'area', 'specification', 'unit',
                              'quantity', 'unit_price', 'contractor', 'start_date',
                              'end_date', 'status', 'notes']

            updates = []
            params = []

            for key, value in kwargs.items():
                if key in allowed_fields and value is not None:
                    updates.append(f'{key} = ?')
                    params.append(value)

            # 如果数量或单价更新，重新计算总价
            if 'quantity' in kwargs or 'unit_price' in kwargs:
                quantity = kwargs.get('quantity', old_item['quantity'])
                unit_price = kwargs.get('unit_price', old_item['unit_price'])
                total_price = quantity * unit_price
                updates.append('total_price = ?')
                params.append(total_price)

            if updates:
                params.append(item_id)
                query = f'UPDATE decoration_items SET {", ".join(updates)} WHERE id = ?'
                cursor.execute(query, params)

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (old_item['project_id'], 'UPDATE_DECORATION',
                     f'更新装修项目: {old_item["item_name"]}')
                )

                conn.commit()

    # 物料库存方法
    def add_inventory_item(self, project_id, item_name, category, specification="",
                           unit="件", stock_quantity=0, min_quantity=0, unit_price=0,
                           supplier="", last_purchase_date=None, expiration_date=None,
                           notes=""):
        """添加物料"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO inventory 
                (project_id, item_name, category, specification, unit, stock_quantity, 
                 min_quantity, unit_price, supplier, last_purchase_date, 
                 expiration_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, item_name, category, specification, unit, stock_quantity,
                  min_quantity, unit_price, supplier, last_purchase_date,
                  expiration_date, notes))

            inventory_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_INVENTORY', f'添加物料: {item_name}, 数量: {stock_quantity}{unit}')
            )

            conn.commit()
            return inventory_id

    def get_project_inventory(self, project_id):
        """获取项目所有物料"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM inventory 
                WHERE project_id = ? 
                ORDER BY category, item_name
            ''', (project_id,))
            inventory = [dict(row) for row in cursor.fetchall()]

            # 计算库存价值
            for item in inventory:
                item['stock_value'] = item['stock_quantity'] * item['unit_price']

            return inventory

    def update_inventory_quantity(self, inventory_id, change_amount, change_type="in"):
        """更新库存数量"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM inventory WHERE id = ?', (inventory_id,))
            item = cursor.fetchone()

            if not item:
                raise ValueError("物料不存在")

            new_quantity = item['stock_quantity']
            if change_type == "in":
                new_quantity += change_amount
            elif change_type == "out":
                new_quantity -= change_amount
            else:
                raise ValueError("无效的变更类型")

            if new_quantity < 0:
                raise ValueError("库存数量不能为负数")

            cursor.execute('''
                UPDATE inventory 
                SET stock_quantity = ?, last_purchase_date = DATE('now')
                WHERE id = ?
            ''', (new_quantity, inventory_id))

            # 记录操作日志
            operation_type = "INVENTORY_IN" if change_type == "in" else "INVENTORY_OUT"
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (item['project_id'], operation_type,
                 f'{"入库" if change_type == "in" else "出库"}: {item["item_name"]}, 数量: {change_amount}{item["unit"]}')
            )

            conn.commit()

    # 房间管理方法
    def add_room(self, project_id, room_number, room_type, capacity=1, area=0,
                 hourly_rate=0, status="空闲", equipment_list="", notes=""):
        """添加房间"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO rooms 
                (project_id, room_number, room_type, capacity, area, 
                 hourly_rate, status, equipment_list, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, room_number, room_type, capacity, area,
                  hourly_rate, status, equipment_list, notes))

            room_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_ROOM', f'添加房间: {room_number} ({room_type})')
            )

            conn.commit()
            return room_id

    def get_project_rooms(self, project_id):
        """获取项目所有房间"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM rooms 
                WHERE project_id = ? 
                ORDER BY room_type, room_number
            ''', (project_id,))
            return [dict(row) for row in cursor.fetchall()]

    def update_room_status(self, room_id, new_status):
        """更新房间状态"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM rooms WHERE id = ?', (room_id,))
            room = cursor.fetchone()

            if not room:
                raise ValueError("房间不存在")

            cursor.execute('''
                UPDATE rooms 
                SET status = ? 
                WHERE id = ?
            ''', (new_status, room_id))

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (room['project_id'], 'UPDATE_ROOM_STATUS',
                 f'更新房间状态: {room["room_number"]} {room["status"]} → {new_status}')
            )

            conn.commit()

    # 成本预算方法
    def add_cost_budget(self, project_id, category, item_name, budget_amount,
                        actual_amount=0, status="预算中", notes=""):
        """添加成本预算"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            variance = actual_amount - budget_amount

            cursor.execute('''
                INSERT INTO cost_budget 
                (project_id, category, item_name, budget_amount, 
                 actual_amount, variance, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, category, item_name, budget_amount,
                  actual_amount, variance, status, notes))

            budget_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_BUDGET', f'添加预算: {item_name} ({category}), 预算: {budget_amount:.2f}元')
            )

            conn.commit()
            return budget_id

    def get_project_budget(self, project_id):
        """获取项目成本预算"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM cost_budget 
                WHERE project_id = ? 
                ORDER BY category, item_name
            ''', (project_id,))
            return [dict(row) for row in cursor.fetchall()]

    def update_cost_budget(self, conn, project_id, category, actual_amount):
        """更新成本预算的实际金额（内部方法）"""
        cursor = conn.cursor()

        # 查找对应类别的预算
        cursor.execute('''
            SELECT id, budget_amount, actual_amount 
            FROM cost_budget 
            WHERE project_id = ? AND category = ?
        ''', (project_id, category))

        budget = cursor.fetchone()

        if budget:
            new_actual = (budget['actual_amount'] or 0) + actual_amount
            variance = new_actual - budget['budget_amount']

            cursor.execute('''
                UPDATE cost_budget 
                SET actual_amount = ?, variance = ?, 
                    status = CASE 
                        WHEN ? > budget_amount THEN '超支'
                        WHEN ? < budget_amount THEN '节约'
                        ELSE '正常'
                    END
                WHERE id = ?
            ''', (new_actual, variance, new_actual, new_actual, budget['id']))

    # 收入预测方法
    def add_revenue_forecast(self, project_id, forecast_month, forecast_type,
                             unit_price, expected_quantity, notes=""):
        """添加收入预测"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            expected_revenue = unit_price * expected_quantity

            cursor.execute('''
                INSERT INTO revenue_forecast 
                (project_id, forecast_month, forecast_type, 
                 unit_price, expected_quantity, expected_revenue, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, forecast_month, forecast_type,
                  unit_price, expected_quantity, expected_revenue, notes))

            forecast_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_REVENUE_FORECAST',
                 f'添加收入预测: {forecast_type}, {forecast_month}, 预计: {expected_revenue:.2f}元')
            )

            conn.commit()
            return forecast_id

    def get_revenue_forecast(self, project_id, months=6):
        """获取收入预测"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取最近几个月的预测
            cursor.execute('''
                SELECT * FROM revenue_forecast 
                WHERE project_id = ? 
                AND forecast_month >= DATE('now', ? || ' months')
                ORDER BY forecast_month DESC, forecast_type
            ''', (project_id, f'-{months}'))

            return [dict(row) for row in cursor.fetchall()]

    # 统计方法
    def get_business_stats(self, project_id):
        """获取业务统计信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            stats = {}

            # 设备统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as equipment_count,
                    SUM(total_price) as equipment_value,
                    SUM(CASE WHEN status = '维修' THEN 1 ELSE 0 END) as maintenance_count
                FROM equipment 
                WHERE project_id = ?
            ''', (project_id,))
            equipment_stats = cursor.fetchone()
            stats['equipment'] = dict(equipment_stats) if equipment_stats else {}

            # 装修统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as decoration_count,
                    SUM(total_price) as decoration_value,
                    SUM(CASE WHEN status = '已完成' THEN 1 ELSE 0 END) as completed_count
                FROM decoration_items 
                WHERE project_id = ?
            ''', (project_id,))
            decoration_stats = cursor.fetchone()
            stats['decoration'] = dict(decoration_stats) if decoration_stats else {}

            # 库存统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as inventory_count,
                    SUM(stock_quantity * unit_price) as inventory_value
                FROM inventory 
                WHERE project_id = ?
            ''', (project_id,))
            inventory_stats = cursor.fetchone()
            stats['inventory'] = dict(inventory_stats) if inventory_stats else {}

            # 预算统计
            cursor.execute('''
                SELECT 
                    SUM(budget_amount) as total_budget,
                    SUM(actual_amount) as total_actual,
                    AVG(CASE 
                        WHEN budget_amount > 0 THEN actual_amount / budget_amount * 100 
                        ELSE 0 
                    END) as execution_rate
                FROM cost_budget 
                WHERE project_id = ?
            ''', (project_id,))
            budget_stats = cursor.fetchone()
            stats['budget'] = dict(budget_stats) if budget_stats else {}

            # 房间统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as room_count,
                    SUM(CASE WHEN status = '空闲' THEN 1 ELSE 0 END) as available_rooms,
                    SUM(CASE WHEN status = '使用中' THEN 1 ELSE 0 END) as occupied_rooms,
                    SUM(CASE WHEN status = '清洁中' THEN 1 ELSE 0 END) as cleaning_rooms,
                    SUM(CASE WHEN status = '维修中' THEN 1 ELSE 0 END) as maintenance_rooms
                FROM rooms 
                WHERE project_id = ?
            ''', (project_id,))
            room_stats = cursor.fetchone()
            stats['rooms'] = dict(room_stats) if room_stats else {}

            return stats

    def get_equipment_stats(self, project_id):
        """获取设备统计信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COUNT(*) as equipment_count,
                    SUM(total_price) as equipment_value,
                    SUM(CASE WHEN status = '维修' THEN 1 ELSE 0 END) as maintenance_count
                FROM equipment 
                WHERE project_id = ?
            ''', (project_id,))
            return dict(cursor.fetchone())

    def get_decoration_stats(self, project_id):
        """获取装修统计信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COUNT(*) as decoration_count,
                    SUM(total_price) as decoration_value,
                    SUM(CASE WHEN status = '已完成' THEN 1 ELSE 0 END) as completed_count
                FROM decoration_items 
                WHERE project_id = ?
            ''', (project_id,))
            return dict(cursor.fetchone())

    def get_inventory_stats(self, project_id):
        """获取库存统计信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COUNT(*) as inventory_count,
                    SUM(stock_quantity * unit_price) as inventory_value
                FROM inventory 
                WHERE project_id = ?
            ''', (project_id,))
            return dict(cursor.fetchone())

    def get_budget_stats(self, project_id):
        """获取预算统计信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    SUM(budget_amount) as total_budget,
                    SUM(actual_amount) as total_actual,
                    AVG(CASE 
                        WHEN budget_amount > 0 THEN actual_amount / budget_amount * 100 
                        ELSE 0 
                    END) as execution_rate
                FROM cost_budget 
                WHERE project_id = ?
            ''', (project_id,))
            return dict(cursor.fetchone())

    def get_room_stats(self, project_id):
        """获取房间统计信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COUNT(*) as room_count,
                    SUM(CASE WHEN status = '空闲' THEN 1 ELSE 0 END) as available_rooms,
                    SUM(CASE WHEN status = '使用中' THEN 1 ELSE 0 END) as occupied_rooms,
                    SUM(CASE WHEN status = '清洁中' THEN 1 ELSE 0 END) as cleaning_rooms,
                    SUM(CASE WHEN status = '维修中' THEN 1 ELSE 0 END) as maintenance_rooms
                FROM rooms 
                WHERE project_id = ?
            ''', (project_id,))
            return dict(cursor.fetchone())

    # ================== 员工管理方法 ==================

    def add_employee(self, project_id, name, position, employment_date, employee_number=None,
                     gender=None, birth_date=None, id_card=None, phone=None,
                     emergency_contact=None, emergency_phone=None, address=None,
                     notes=None):
        """添加员工"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 生成员工编号（如果未提供）
            if not employee_number:
                employee_number = self._generate_employee_number(conn, project_id)

            cursor.execute('''
                INSERT INTO employees 
                (project_id, employee_number, name, gender, birth_date, id_card, 
                 phone, emergency_contact, emergency_phone, address, position, 
                 employment_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, employee_number, name, gender, birth_date, id_card,
                  phone, emergency_contact, emergency_phone, address, position,
                  employment_date, notes))

            employee_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_EMPLOYEE', f'添加员工: {name} ({position}), 编号: {employee_number}')
            )

            conn.commit()
            return employee_id

    def _generate_employee_number(self, conn, project_id):
        """生成员工编号"""
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) as count FROM employees 
            WHERE project_id = ? AND strftime('%Y', employment_date) = strftime('%Y', 'now')
        ''', (project_id,))
        count = cursor.fetchone()['count']

        year = datetime.now().strftime('%Y')
        project_code = f"P{project_id:03d}"
        employee_number = f"{project_code}{year}{count + 1:04d}"
        return employee_number

    def get_project_employees(self, project_id, status_filter=None):
        """获取项目所有员工"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if status_filter:
                cursor.execute('''
                    SELECT * FROM employees 
                    WHERE project_id = ? AND status = ?
                    ORDER BY position, name
                ''', (project_id, status_filter))
            else:
                cursor.execute('''
                    SELECT * FROM employees 
                    WHERE project_id = ? 
                    ORDER BY position, name
                ''', (project_id,))

            return [dict(row) for row in cursor.fetchall()]

    def get_employee_details(self, employee_id):
        """获取员工详细信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取基本信息
            cursor.execute('SELECT * FROM employees WHERE id = ?', (employee_id,))
            employee = cursor.fetchone()

            if not employee:
                return None

            result = dict(employee)

            # 获取技能信息
            cursor.execute('SELECT * FROM employee_skills WHERE employee_id = ?', (employee_id,))
            result['skills'] = [dict(row) for row in cursor.fetchall()]

            return result

    def update_employee(self, employee_id, **kwargs):
        """更新员工信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取原员工信息
            cursor.execute('SELECT * FROM employees WHERE id = ?', (employee_id,))
            old_employee = cursor.fetchone()

            if not old_employee:
                raise ValueError("员工不存在")

            allowed_fields = ['name', 'gender', 'birth_date', 'id_card', 'phone',
                              'emergency_contact', 'emergency_phone', 'address',
                              'position', 'status', 'notes']

            updates = []
            params = []

            for key, value in kwargs.items():
                if key in allowed_fields and value is not None:
                    updates.append(f'{key} = ?')
                    params.append(value)

            if updates:
                params.append(employee_id)
                query = f'UPDATE employees SET {", ".join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
                cursor.execute(query, params)

                # 记录操作日志
                cursor.execute(
                    '''INSERT INTO operation_logs (project_id, operation_type, details) 
                       VALUES (?, ?, ?)''',
                    (old_employee['project_id'], 'UPDATE_EMPLOYEE',
                     f'更新员工信息: {old_employee["name"]}')
                )

                conn.commit()

    def add_employee_skill(self, employee_id, skill_type, skill_level=None,
                           certification=None, certification_date=None,
                           experience_years=0, rating=0, notes=None):
        """添加员工技能"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取员工信息
            cursor.execute('SELECT * FROM employees WHERE id = ?', (employee_id,))
            employee = cursor.fetchone()

            if not employee:
                raise ValueError("员工不存在")

            cursor.execute('''
                INSERT INTO employee_skills 
                (employee_id, skill_type, skill_level, certification, 
                 certification_date, experience_years, rating, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(employee_id, skill_type) DO UPDATE SET
                    skill_level = excluded.skill_level,
                    certification = excluded.certification,
                    certification_date = excluded.certification_date,
                    experience_years = excluded.experience_years,
                    rating = excluded.rating,
                    notes = excluded.notes
            ''', (employee_id, skill_type, skill_level, certification,
                  certification_date, experience_years, rating, notes))

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (employee['project_id'], 'ADD_EMPLOYEE_SKILL',
                 f'为员工 {employee["name"]} 添加/更新技能: {skill_type}')
            )

            conn.commit()

    # ================== 排班管理方法 ==================

    def create_schedule(self, project_id, employee_id, schedule_date, shift_type,
                        start_time, end_time, rest_hours=1.0, room_assignment=None,
                        notes=None):
        """创建排班"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 检查员工是否存在且在职
            cursor.execute('''
                SELECT id, name, status FROM employees 
                WHERE id = ? AND project_id = ?
            ''', (employee_id, project_id))
            employee = cursor.fetchone()

            if not employee:
                raise ValueError("员工不存在或不属于此项目")

            if employee['status'] != '在职':
                raise ValueError(f"员工状态为{employee['status']}，无法安排班次")

            cursor.execute('''
                INSERT INTO schedules 
                (project_id, employee_id, schedule_date, shift_type, 
                 start_time, end_time, rest_hours, room_assignment, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, employee_id, schedule_date, shift_type,
                  start_time, end_time, rest_hours, room_assignment, notes))

            schedule_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'CREATE_SCHEDULE',
                 f'为员工 {employee["name"]} 安排{shift_type}: {schedule_date} {start_time}-{end_time}')
            )

            conn.commit()
            return schedule_id

    def get_employee_schedule(self, employee_id, start_date, end_date):
        """获取员工排班"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT s.*, e.name as employee_name, e.position
                FROM schedules s
                JOIN employees e ON s.employee_id = e.id
                WHERE s.employee_id = ? 
                AND s.schedule_date BETWEEN ? AND ?
                ORDER BY s.schedule_date, s.start_time
            ''', (employee_id, start_date, end_date))

            return [dict(row) for row in cursor.fetchall()]

    def get_project_schedule(self, project_id, schedule_date):
        """获取项目某日的排班"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT 
                    s.*,
                    e.name as employee_name,
                    e.position,
                    e.employee_number
                FROM schedules s
                JOIN employees e ON s.employee_id = e.id
                WHERE s.project_id = ? AND s.schedule_date = ?
                ORDER BY s.shift_type, s.start_time
            ''', (project_id, schedule_date))

            return [dict(row) for row in cursor.fetchall()]

    # ================== 考勤管理方法 ==================

    def record_attendance(self, schedule_id, check_in_time=None, check_out_time=None,
                          attendance_status='正常', notes=None):
        """记录考勤"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取排班信息
            cursor.execute('''
                SELECT s.*, e.name as employee_name, e.project_id
                FROM schedules s
                JOIN employees e ON s.employee_id = e.id
                WHERE s.id = ?
            ''', (schedule_id,))

            schedule = cursor.fetchone()

            if not schedule:
                raise ValueError("排班记录不存在")

            # 计算实际工作时长
            actual_hours = 0
            late_minutes = 0
            early_leave_minutes = 0

            if check_in_time and check_out_time:
                # 解析时间并计算
                check_in = datetime.strptime(check_in_time, '%H:%M')
                check_out = datetime.strptime(check_out_time, '%H:%M')
                scheduled_start = datetime.strptime(schedule['start_time'], '%H:%M')
                scheduled_end = datetime.strptime(schedule['end_time'], '%H:%M')

                # 计算迟到/早退
                if check_in > scheduled_start:
                    late_minutes = (check_in - scheduled_start).seconds // 60

                if check_out < scheduled_end:
                    early_leave_minutes = (scheduled_end - check_out).seconds // 60

                # 实际工作时长（减去休息时间）
                actual_hours = (check_out - check_in).seconds / 3600 - schedule['rest_hours']

            cursor.execute('''
                INSERT INTO attendance 
                (schedule_id, check_in_time, check_out_time, actual_hours,
                 late_minutes, early_leave_minutes, attendance_status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (schedule_id, check_in_time, check_out_time, actual_hours,
                  late_minutes, early_leave_minutes, attendance_status, notes))

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (schedule['project_id'], 'RECORD_ATTENDANCE',
                 f'记录考勤: {schedule["employee_name"]}, 状态: {attendance_status}')
            )

            conn.commit()

    def get_attendance_report(self, project_id, start_date, end_date):
        """获取考勤报表"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT 
                    e.id as employee_id,
                    e.name as employee_name,
                    e.employee_number,
                    e.position,
                    COUNT(s.id) as scheduled_days,
                    COUNT(a.id) as attendance_days,
                    SUM(CASE WHEN a.attendance_status = '迟到' THEN 1 ELSE 0 END) as late_days,
                    SUM(CASE WHEN a.attendance_status = '早退' THEN 1 ELSE 0 END) as early_leave_days,
                    SUM(CASE WHEN a.attendance_status = '旷工' THEN 1 ELSE 0 END) as absent_days,
                    SUM(CASE WHEN a.attendance_status = '请假' THEN 1 ELSE 0 END) as leave_days,
                    SUM(a.actual_hours) as total_hours,
                    AVG(CASE WHEN a.attendance_status = '正常' THEN 1 ELSE 0 END) * 100 as attendance_rate
                FROM employees e
                LEFT JOIN schedules s ON e.id = s.employee_id 
                    AND s.schedule_date BETWEEN ? AND ?
                LEFT JOIN attendance a ON s.id = a.schedule_id
                WHERE e.project_id = ? AND e.status = '在职'
                GROUP BY e.id
                ORDER BY e.position, e.name
            ''', (start_date, end_date, project_id))

            return [dict(row) for row in cursor.fetchall()]

    # ================== 薪资管理方法 ==================

    def set_salary_structure(self, project_id, position, base_salary, hourly_rate,
                             commission_rate, overtime_rate=1.5, allowance=0,
                             social_insurance_rate=0, housing_fund_rate=0,
                             effective_date=None, notes=None):
        """设置薪资结构"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if not effective_date:
                effective_date = datetime.now().date().isoformat()

            # 停用旧的薪资结构
            cursor.execute('''
                UPDATE salary_structure 
                SET is_active = 0 
                WHERE project_id = ? AND position = ? AND is_active = 1
            ''', (project_id, position))

            # 创建新的薪资结构
            cursor.execute('''
                INSERT INTO salary_structure 
                (project_id, position, base_salary, hourly_rate, commission_rate,
                 overtime_rate, allowance, social_insurance_rate, housing_fund_rate,
                 effective_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, position, base_salary, hourly_rate, commission_rate,
                  overtime_rate, allowance, social_insurance_rate, housing_fund_rate,
                  effective_date, notes))

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'SET_SALARY_STRUCTURE',
                 f'设置薪资结构: {position}, 基本工资: {base_salary:.2f}元')
            )

            conn.commit()

    def calculate_salary(self, project_id, salary_month):
        """计算工资"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取所有在职员工
            cursor.execute('''
                SELECT e.*, ss.*
                FROM employees e
                LEFT JOIN salary_structure ss ON e.project_id = ss.project_id 
                    AND e.position = ss.position 
                    AND ss.is_active = 1
                WHERE e.project_id = ? AND e.status = '在职'
            ''', (project_id,))

            employees = cursor.fetchall()

            for emp in employees:
                # 计算考勤相关数据
                cursor.execute('''
                    SELECT 
                        SUM(a.actual_hours) as total_hours,
                        SUM(CASE WHEN a.attendance_status = '迟到' THEN 1 ELSE 0 END) as late_days,
                        SUM(CASE WHEN a.attendance_status = '早退' THEN 1 ELSE 0 END) as early_leave_days,
                        SUM(CASE WHEN a.attendance_status = '加班' THEN a.actual_hours ELSE 0 END) as overtime_hours
                    FROM schedules s
                    JOIN attendance a ON s.id = a.schedule_id
                    WHERE s.employee_id = ? 
                    AND strftime('%Y-%m', s.schedule_date) = ?
                ''', (emp['id'], salary_month))

                attendance_data = cursor.fetchone() or {}

                # 计算提成（这里需要根据业务逻辑调整）
                cursor.execute('''
                    -- 需要根据实际业务逻辑计算提成
                    SELECT 0 as commission_amount
                ''')

                commission_data = cursor.fetchone()

                # 计算各项工资组成
                base_salary = emp['base_salary'] or 0
                work_hours = attendance_data.get('total_hours', 0) or 0
                hourly_wage = work_hours * (emp['hourly_rate'] or 0)
                overtime_hours = attendance_data.get('overtime_hours', 0) or 0
                overtime_pay = overtime_hours * (emp['hourly_rate'] or 0) * (emp['overtime_rate'] or 1.5)
                commission_amount = commission_data.get('commission_amount', 0) or 0
                allowance = emp['allowance'] or 0

                # 总收入
                total_income = base_salary + hourly_wage + overtime_pay + commission_amount + allowance

                # 扣款
                social_insurance = total_income * (emp['social_insurance_rate'] or 0) / 100
                housing_fund = total_income * (emp['housing_fund_rate'] or 0) / 100
                tax = self._calculate_tax(total_income - social_insurance - housing_fund)
                other_deductions = 0  # 其他扣款，可根据考勤调整

                if attendance_data.get('late_days', 0):
                    other_deductions += attendance_data['late_days'] * 20  # 每次迟到扣20元
                if attendance_data.get('early_leave_days', 0):
                    other_deductions += attendance_data['early_leave_days'] * 20  # 每次早退扣20元

                # 实发工资
                net_salary = total_income - social_insurance - housing_fund - tax - other_deductions

                # 保存工资记录
                cursor.execute('''
                    INSERT INTO salary_records 
                    (project_id, employee_id, salary_month, base_salary, work_hours,
                     hourly_wage, overtime_hours, overtime_pay, commission_amount,
                     allowance, total_income, social_insurance, housing_fund,
                     tax, other_deductions, net_salary, payment_status, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(employee_id, salary_month) DO UPDATE SET
                        base_salary = excluded.base_salary,
                        work_hours = excluded.work_hours,
                        hourly_wage = excluded.hourly_wage,
                        overtime_hours = excluded.overtime_hours,
                        overtime_pay = excluded.overtime_pay,
                        commission_amount = excluded.commission_amount,
                        allowance = excluded.allowance,
                        total_income = excluded.total_income,
                        social_insurance = excluded.social_insurance,
                        housing_fund = excluded.housing_fund,
                        tax = excluded.tax,
                        other_deductions = excluded.other_deductions,
                        net_salary = excluded.net_salary
                ''', (project_id, emp['id'], salary_month, base_salary, work_hours,
                      hourly_wage, overtime_hours, overtime_pay, commission_amount,
                      allowance, total_income, social_insurance, housing_fund,
                      tax, other_deductions, net_salary, '未发放', None))

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'CALCULATE_SALARY', f'计算工资: {salary_month}')
            )

            conn.commit()

    def _calculate_tax(self, taxable_income):
        """计算个人所得税（简化版）"""
        if taxable_income <= 5000:
            return 0
        elif taxable_income <= 8000:
            return (taxable_income - 5000) * 0.03
        elif taxable_income <= 17000:
            return (taxable_income - 8000) * 0.1 + 90
        elif taxable_income <= 30000:
            return (taxable_income - 17000) * 0.2 + 990
        else:
            return (taxable_income - 30000) * 0.25 + 3590

    def get_salary_records(self, project_id, salary_month=None):
        """获取工资记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if salary_month:
                cursor.execute('''
                    SELECT sr.*, e.name as employee_name, e.employee_number, e.position
                    FROM salary_records sr
                    JOIN employees e ON sr.employee_id = e.id
                    WHERE sr.project_id = ? AND sr.salary_month = ?
                    ORDER BY e.position, e.name
                ''', (project_id, salary_month))
            else:
                cursor.execute('''
                    SELECT sr.*, e.name as employee_name, e.employee_number, e.position
                    FROM salary_records sr
                    JOIN employees e ON sr.employee_id = e.id
                    WHERE sr.project_id = ?
                    ORDER BY sr.salary_month DESC, e.position, e.name
                ''', (project_id,))

            return [dict(row) for row in cursor.fetchall()]

    # ================== 绩效评估方法 ==================

    def add_performance_review(self, employee_id, review_date, review_period,
                               reviewer_id=None, reviewer_name=None,
                               service_quality_score=0, customer_feedback_score=0,
                               attendance_score=0, efficiency_score=0,
                               team_cooperation_score=0, strengths=None,
                               areas_for_improvement=None, development_plan=None,
                               promotion_recommendation=0, salary_adjustment_percentage=0,
                               notes=None):
        """添加绩效评估"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取员工信息
            cursor.execute('SELECT * FROM employees WHERE id = ?', (employee_id,))
            employee = cursor.fetchone()

            if not employee:
                raise ValueError("员工不存在")

            # 计算总分
            total_score = (service_quality_score + customer_feedback_score +
                           attendance_score + efficiency_score + team_cooperation_score) / 5

            # 确定评级
            if total_score >= 90:
                rating_level = '优秀'
            elif total_score >= 80:
                rating_level = '良好'
            elif total_score >= 60:
                rating_level = '一般'
            else:
                rating_level = '待改进'

            cursor.execute('''
                INSERT INTO performance_reviews 
                (employee_id, review_date, review_period, reviewer_id, reviewer_name,
                 service_quality_score, customer_feedback_score, attendance_score,
                 efficiency_score, team_cooperation_score, total_score, rating_level,
                 strengths, areas_for_improvement, development_plan, 
                 promotion_recommendation, salary_adjustment_percentage, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (employee_id, review_date, review_period, reviewer_id, reviewer_name,
                  service_quality_score, customer_feedback_score, attendance_score,
                  efficiency_score, team_cooperation_score, total_score, rating_level,
                  strengths, areas_for_improvement, development_plan,
                  promotion_recommendation, salary_adjustment_percentage, notes))

            review_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (employee['project_id'], 'ADD_PERFORMANCE_REVIEW',
                 f'为员工 {employee["name"]} 进行绩效评估, 评级: {rating_level}')
            )

            conn.commit()
            return review_id

    def get_employee_performance(self, employee_id, limit=10):
        """获取员工绩效记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM performance_reviews 
                WHERE employee_id = ? 
                ORDER BY review_date DESC 
                LIMIT ?
            ''', (employee_id, limit))

            return [dict(row) for row in cursor.fetchall()]

    # ================== 培训管理方法 ==================

    def add_training_record(self, project_id, training_name, training_type,
                            trainer=None, training_date=None, duration_hours=1.0,
                            location=None, notes=None):
        """添加培训记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if not training_date:
                training_date = datetime.now().date().isoformat()

            cursor.execute('''
                INSERT INTO training_records 
                (project_id, training_name, training_type, trainer, training_date,
                 duration_hours, location, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (project_id, training_name, training_type, trainer, training_date,
                  duration_hours, location, notes))

            training_id = cursor.lastrowid

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (project_id, 'ADD_TRAINING', f'添加培训记录: {training_name}')
            )

            conn.commit()
            return training_id

    def register_training_participant(self, training_id, employee_id,
                                      attendance_status='参加', test_score=None,
                                      feedback=None, certification=None, notes=None):
        """注册培训参与者"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取员工信息
            cursor.execute('SELECT * FROM employees WHERE id = ?', (employee_id,))
            employee = cursor.fetchone()

            if not employee:
                raise ValueError("员工不存在")

            # 获取培训信息
            cursor.execute('SELECT * FROM training_records WHERE id = ?', (training_id,))
            training = cursor.fetchone()

            if not training:
                raise ValueError("培训记录不存在")

            cursor.execute('''
                INSERT INTO training_participants 
                (training_id, employee_id, attendance_status, test_score, 
                 feedback, certification, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (training_id, employee_id, attendance_status, test_score,
                  feedback, certification, notes))

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (employee['project_id'], 'REGISTER_TRAINING',
                 f'员工 {employee["name"]} 参加培训: {training["training_name"]}')
            )

            conn.commit()

    # ================== 离职管理方法 ==================

    def record_resignation(self, employee_id, resignation_date, resignation_type,
                           reason=None, exit_interview_notes=None,
                           handover_completed=0, last_working_date=None,
                           notes=None):
        """记录离职信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取员工信息
            cursor.execute('SELECT * FROM employees WHERE id = ?', (employee_id,))
            employee = cursor.fetchone()

            if not employee:
                raise ValueError("员工不存在")

            # 更新员工状态
            cursor.execute('''
                UPDATE employees 
                SET status = '离职', updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (employee_id,))

            # 记录离职信息
            cursor.execute('''
                INSERT INTO employee_resignation 
                (employee_id, resignation_date, resignation_type, reason,
                 exit_interview_notes, handover_completed, last_working_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (employee_id, resignation_date, resignation_type, reason,
                  exit_interview_notes, handover_completed, last_working_date, notes))

            # 记录操作日志
            cursor.execute(
                '''INSERT INTO operation_logs (project_id, operation_type, details) 
                   VALUES (?, ?, ?)''',
                (employee['project_id'], 'RECORD_RESIGNATION',
                 f'员工离职: {employee["name"]}, 类型: {resignation_type}')
            )

            conn.commit()

    # ================== 人员统计方法 ==================

    def get_personnel_stats(self, project_id):
        """获取人员统计信息"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            stats = {}

            # 员工总数统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_employees,
                    SUM(CASE WHEN status = '在职' THEN 1 ELSE 0 END) as active_employees,
                    SUM(CASE WHEN status = '离职' THEN 1 ELSE 0 END) as resigned_employees,
                    SUM(CASE WHEN status = '休假' THEN 1 ELSE 0 END) as on_leave_employees,
                    COUNT(DISTINCT position) as position_count
                FROM employees 
                WHERE project_id = ?
            ''', (project_id,))
            employee_stats = cursor.fetchone()
            stats['employees'] = dict(employee_stats) if employee_stats else {}

            # 职位分布统计
            cursor.execute('''
                SELECT 
                    position,
                    COUNT(*) as count,
                    AVG(strftime('%Y', 'now') - strftime('%Y', employment_date)) as avg_service_years
                FROM employees 
                WHERE project_id = ? AND status = '在职'
                GROUP BY position
                ORDER BY count DESC
            ''', (project_id,))
            position_stats = cursor.fetchall()
            stats['positions'] = [dict(row) for row in position_stats]

            # 技能统计
            cursor.execute('''
                SELECT 
                    es.skill_type,
                    COUNT(DISTINCT es.employee_id) as employee_count,
                    AVG(es.skill_level) as avg_level
                FROM employee_skills es
                JOIN employees e ON es.employee_id = e.id
                WHERE e.project_id = ? AND e.status = '在职'
                GROUP BY es.skill_type
                ORDER BY employee_count DESC
            ''', (project_id,))
            skill_stats = cursor.fetchall()
            stats['skills'] = [dict(row) for row in skill_stats]

            # 考勤统计
            cursor.execute('''
                SELECT 
                    AVG(CASE WHEN a.attendance_status = '正常' THEN 1 ELSE 0 END) * 100 as attendance_rate,
                    AVG(a.late_minutes) as avg_late_minutes,
                    AVG(a.early_leave_minutes) as avg_early_leave_minutes,
                    AVG(a.actual_hours) as avg_work_hours
                FROM attendance a
                JOIN schedules s ON a.schedule_id = s.id
                WHERE s.project_id = ? 
                AND strftime('%Y-%m', s.schedule_date) = strftime('%Y-%m', 'now')
            ''', (project_id,))
            attendance_stats = cursor.fetchone()
            stats['attendance'] = dict(attendance_stats) if attendance_stats else {}

            return stats

    def get_employee_summary(self, employee_id):
        """获取员工综合信息摘要"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 获取基本信息
            cursor.execute('SELECT * FROM employees WHERE id = ?', (employee_id,))
            employee = cursor.fetchone()

            if not employee:
                return None

            summary = dict(employee)

            # 获取技能信息
            cursor.execute('''
                SELECT * FROM employee_skills 
                WHERE employee_id = ? 
                ORDER BY skill_level DESC
            ''', (employee_id,))
            summary['skills'] = [dict(row) for row in cursor.fetchall()]

            # 获取最近绩效
            cursor.execute('''
                SELECT * FROM performance_reviews 
                WHERE employee_id = ? 
                ORDER BY review_date DESC 
                LIMIT 1
            ''', (employee_id,))
            performance = cursor.fetchone()
            summary['latest_performance'] = dict(performance) if performance else None

            # 获取最近考勤统计
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT s.schedule_date) as scheduled_days,
                    COUNT(a.id) as attendance_days,
                    SUM(CASE WHEN a.attendance_status = '迟到' THEN 1 ELSE 0 END) as late_days,
                    SUM(CASE WHEN a.attendance_status = '早退' THEN 1 ELSE 0 END) as early_leave_days,
                    AVG(a.actual_hours) as avg_work_hours
                FROM schedules s
                LEFT JOIN attendance a ON s.id = a.schedule_id
                WHERE s.employee_id = ? 
                AND strftime('%Y-%m', s.schedule_date) = strftime('%Y-%m', 'now')
            ''', (employee_id,))
            attendance_summary = cursor.fetchone()
            summary['attendance_summary'] = dict(attendance_summary) if attendance_summary else {}

            # 获取最近工资
            cursor.execute('''
                SELECT * FROM salary_records 
                WHERE employee_id = ? 
                ORDER BY salary_month DESC 
                LIMIT 1
            ''', (employee_id,))
            salary = cursor.fetchone()
            summary['latest_salary'] = dict(salary) if salary else None

            return summary

    def get_staffing_needs(self, project_id):
        """分析人员需求"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            needs = []

            # 分析每个职位的需求
            cursor.execute('''
                WITH shift_analysis AS (
                    SELECT 
                        s.shift_type,
                        COUNT(DISTINCT s.employee_id) as required_staff,
                        COUNT(DISTINCT e.id) as current_staff
                    FROM schedules s
                    JOIN employees e ON s.employee_id = e.id
                    WHERE s.project_id = ? 
                    AND s.schedule_date >= DATE('now')
                    AND s.schedule_date < DATE('now', '+7 days')
                    AND e.status = '在职'
                    GROUP BY s.shift_type
                )
                SELECT 
                    shift_type,
                    required_staff,
                    current_staff,
                    required_staff - current_staff as shortage
                FROM shift_analysis
                WHERE required_staff > current_staff
                ORDER BY shortage DESC
            ''', (project_id,))

            shift_needs = cursor.fetchall()
            needs.extend([dict(row) for row in shift_needs])

            # 分析技能需求
            cursor.execute('''
                WITH service_demand AS (
                    SELECT 
                        r.forecast_type,
                        SUM(r.expected_quantity) as expected_demand
                    FROM revenue_forecast r
                    WHERE r.project_id = ? 
                    AND r.forecast_month >= DATE('now')
                    AND r.forecast_month < DATE('now', '+3 months')
                    GROUP BY r.forecast_type
                ),
                current_skills AS (
                    SELECT 
                        es.skill_type,
                        COUNT(DISTINCT es.employee_id) as skilled_staff
                    FROM employee_skills es
                    JOIN employees e ON es.employee_id = e.id
                    WHERE e.project_id = ? AND e.status = '在职'
                    GROUP BY es.skill_type
                )
                SELECT 
                    d.forecast_type as service_type,
                    d.expected_demand,
                    COALESCE(c.skilled_staff, 0) as current_staff,
                    CASE 
                        WHEN d.expected_demand > COALESCE(c.skilled_staff, 0) * 50 
                        THEN CEIL((d.expected_demand - COALESCE(c.skilled_staff, 0) * 50) / 50)
                        ELSE 0
                    END as additional_staff_needed
                FROM service_demand d
                LEFT JOIN current_skills c ON d.forecast_type = c.skill_type
                WHERE d.expected_demand > COALESCE(c.skilled_staff, 0) * 50
                ORDER BY additional_staff_needed DESC
            ''', (project_id, project_id))

            skill_needs = cursor.fetchall()
            needs.extend([dict(row) for row in skill_needs])

            return needs