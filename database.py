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
                LIMIT ?
            ''', (project_id, limit))

            distributions = []
            for row in cursor.fetchall():
                dist = dict(row)
                if dist['details_json']:
                    dist['details'] = json.loads(dist['details_json'])
                distributions.append(dist)

            return distributions

    # 在InvestmentDB类中添加以下方法

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
                (
                project_id, 'ADD_EQUIPMENT', f'添加设备: {equipment_name}, 数量: {quantity}, 总价: {total_price:.2f}元')
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

    # 在 InvestmentDB 类中添加以下方法：

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