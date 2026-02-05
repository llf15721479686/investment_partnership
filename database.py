import sqlite3
import os
import time
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

            # 创建索引提高查询性能
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_partners_project_id ON partners(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_project_id ON operation_logs(project_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_operated_at ON operation_logs(operated_at)')

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