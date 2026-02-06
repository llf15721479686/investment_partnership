import sys
import os
import sqlite3
from pathlib import Path
from datetime import datetime
import urllib.parse
import json

# 确保数据库模块可以导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import InvestmentDB

# HTML文件路径
HTML_FILE = 'index.html'


def load_html_template():
    """加载HTML模板文件"""
    try:
        with open(HTML_FILE, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"错误: 找不到HTML文件 {HTML_FILE}")
        print(f"请确保 {HTML_FILE} 与 app.py 在同一目录")
        return None
    except Exception as e:
        print(f"读取HTML文件时出错: {e}")
        return None


def main():
    """启动应用"""
    import http.server
    import socketserver

    # 创建数据库实例
    db = InvestmentDB()

    # 加载HTML模板
    HTML_TEMPLATE = load_html_template()
    if HTML_TEMPLATE is None:
        print("无法启动服务器：HTML模板加载失败")
        return

    class InvestmentHandler(http.server.SimpleHTTPRequestHandler):
        """自定义请求处理器"""

        def do_GET(self):
            """处理GET请求"""
            # API路由
            if self.path.startswith('/api/'):
                self.handle_api_request()
            # 静态文件或主页
            else:
                if self.path == '/' or self.path == '/index.html':
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(HTML_TEMPLATE.encode('utf-8'))
                else:
                    # 尝试提供其他静态文件（CSS、JS等）
                    super().do_GET()

        def do_POST(self):
            """处理POST请求"""
            if self.path.startswith('/api/'):
                self.handle_api_request()
            else:
                self.send_error(404, "文件未找到")

        def do_PUT(self):
            """处理PUT请求"""
            if self.path.startswith('/api/'):
                self.handle_api_request()
            else:
                self.send_error(404, "文件未找到")

        def do_DELETE(self):
            """处理DELETE请求"""
            if self.path.startswith('/api/'):
                self.handle_api_request()
            else:
                self.send_error(404, "文件未找到")

        def handle_api_request(self):
            """处理API请求"""
            try:
                print(f"API请求: {self.path}, 方法: {self.command}")

                # ============ 员工管理相关API ============
                # 先处理带有查询参数的路径
                if self.path.startswith('/api/employees'):
                    self.handle_employees()
                elif self.path.startswith('/api/employee/'):
                    self.handle_employee_detail()
                elif self.path.startswith('/api/employee_summary'):
                    self.handle_employee_summary()
                elif self.path.startswith('/api/employee_details'):
                    self.handle_employee_details()
                elif self.path.startswith('/api/salary'):
                    self.handle_salary()
                elif self.path.startswith('/api/schedule'):
                    self.handle_schedule()
                elif self.path.startswith('/api/create_schedule'):
                    self.handle_create_schedule()  # 新增排班
                elif self.path.startswith('/api/attendance'):
                    self.handle_attendance()
                elif self.path.startswith('/api/performance'):
                    self.handle_performance()
                elif self.path.startswith('/api/training'):
                    self.handle_training()
                elif self.path.startswith('/api/personnel_stats'):
                    self.handle_personnel_stats()
                # 添加缺失的API端点
                elif self.path.startswith('/api/project_schedule'):
                    self.handle_schedule()  # 复用同一个处理方法
                elif self.path.startswith('/api/attendance_report'):
                    self.handle_attendance()  # 复用同一个处理方法
                elif self.path.startswith('/api/salary_records'):
                    self.handle_salary()  # 复用同一个处理方法
                elif self.path.startswith('/api/training_stats'):
                    self.handle_training()  # 复用同一个处理方法
                elif self.path.startswith('/api/upcoming_trainings'):
                    self.handle_training()  # 复用同一个处理方法
                elif self.path.startswith('/api/recent_performance'):
                    self.handle_performance()  # 复用同一个处理方法
                # ============ 原有API ============
                elif self.path == '/api/projects':
                    self.handle_projects()
                elif self.path.startswith('/api/project/'):
                    if '/tiers' in self.path:
                        self.handle_project_tiers()
                    elif '/distribution' in self.path:
                        self.handle_distribution()
                    elif '/partners' in self.path:
                        self.handle_project_partners()
                    else:
                        self.handle_single_project()
                elif self.path.startswith('/api/equipment/'):
                    self.handle_equipment()
                elif self.path.startswith('/api/decoration/'):
                    self.handle_decoration()
                elif self.path.startswith('/api/inventory/'):
                    self.handle_inventory()
                elif self.path.startswith('/api/rooms/'):
                    self.handle_rooms()
                elif self.path.startswith('/api/budget/'):
                    self.handle_budget()
                elif self.path.startswith('/api/revenue/'):
                    self.handle_revenue()
                elif self.path.startswith('/api/partner/'):
                    if '/tier' in self.path:
                        self.handle_partner_tier()
                    else:
                        self.handle_partner()
                elif self.path.startswith('/api/tier/'):
                    self.handle_tier()
                elif self.path.startswith('/api/logs'):
                    self.handle_logs()
                elif self.path.startswith('/api/stats/'):
                    self.handle_stats()
                else:
                    print(f"API端点未找到: {self.path}")
                    self.send_json_response({
                        'success': False,
                        'error': f'API端点未找到: {self.path}'
                    }, status=404)
            except Exception as e:
                print(f"处理API请求时出错: {e}")
                import traceback
                traceback.print_exc()
                try:
                    error_msg = str(e)
                    if isinstance(e, UnicodeEncodeError):
                        error_msg = "编码错误"
                    self.send_json_response({
                        'success': False,
                        'error': error_msg
                    }, status=500)
                except Exception as send_error:
                    print(f"发送错误响应时也失败了: {send_error}")
                    try:
                        self.send_response(500)
                        self.send_header('Content-type', 'application/json; charset=utf-8')
                        self.end_headers()
                        self.wfile.write(b'{"success": false, "error": "Internal server error"}')
                    except:
                        pass

        def get_request_data(self):
            """获取请求数据 - 修复编码问题"""
            print(f"\n=== 进入get_request_data()方法 ===")

            try:
                if 'Content-Length' not in self.headers:
                    print("Content-Length不存在于headers中")
                    return {}

                content_length = int(self.headers['Content-Length'])
                print(f"Content-Length: {content_length}")

                raw_data = self.rfile.read(content_length)
                print(f"读取的原始数据长度: {len(raw_data)} 字节")

                if not raw_data:
                    print("原始数据为空")
                    return {}

                # 直接使用UTF-8解码，如果失败则使用错误忽略
                try:
                    decoded_data = raw_data.decode('utf-8')
                    print(f"UTF-8解码成功")
                except UnicodeDecodeError:
                    print(f"UTF-8解码失败，使用错误忽略模式")
                    decoded_data = raw_data.decode('utf-8', errors='ignore')

                print(f"解码后的数据前200字符: {decoded_data[:200]}")

                try:
                    data = json.loads(decoded_data)
                    print(f"JSON解析成功，返回数据")
                    return data
                except json.JSONDecodeError as e:
                    print(f"JSON解析错误: {e}")
                    print(f"解码后的数据: {decoded_data[:100]}")
                    return {}
            except Exception as e:
                print(f"get_request_data()方法出现异常: {e}")
                import traceback
                traceback.print_exc()
                return {}

        def send_json_response(self, data, status=200):
            """发送JSON响应 - 修复编码问题"""
            self.send_response(status)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            try:
                # 使用简单的方式处理数据
                json_str = json.dumps(data, ensure_ascii=False, default=str)
                json_bytes = json_str.encode('utf-8')
                self.wfile.write(json_bytes)
            except Exception as e:
                print(f"发送JSON响应时出错: {e}")
                # 发送最简单的错误响应
                try:
                    error_json = '{"success": false, "error": "响应错误"}'
                    self.wfile.write(error_json.encode('utf-8'))
                except:
                    pass

        # ================== 员工管理相关处理方法 ==================

        def handle_employees(self):
            """处理员工列表请求"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            project_id = params.get('project_id', [None])[0]

            print(f"=== 处理员工列表请求 ===")
            print(f"请求路径: {self.path}")
            print(f"请求方法: {self.command}")
            print(f"查询参数: {params}")
            print(f"提取的project_id: {project_id}")

            if not project_id:
                print("错误: 缺少project_id参数")
                self.send_json_response({'success': False, 'error': '需要project_id参数'}, status=400)
                return

            if self.command == 'GET':
                try:
                    status_filter = params.get('status', [None])[0]
                    print(f"GET请求 - 状态过滤: {status_filter}")
                    employees = db.get_project_employees(int(project_id), status_filter)
                    print(f"获取到 {len(employees)} 名员工")
                    self.send_json_response(employees)
                except Exception as e:
                    print(f"GET请求异常: {str(e)}")
                    self.send_json_response({'success': False, 'error': str(e)}, status=500)


            elif self.command == 'POST':

                print(f"=== 处理员工添加请求 ===")

                # 简化：直接使用最安全的方式获取数据

                try:

                    content_length = int(self.headers.get('Content-Length', 0))

                    if content_length > 0:

                        raw_data = self.rfile.read(content_length)

                        decoded_data = raw_data.decode('utf-8', errors='ignore')

                        data = json.loads(decoded_data)

                    else:

                        data = {}

                except Exception as e:

                    print(f"解析请求数据失败: {e}")

                    self.send_json_response({'success': False, 'error': f'数据解析失败: {str(e)}'}, status=400)

                    return

                print(f"解析到的数据: {data}")

                try:

                    # 调用数据库方法

                    employee_id = db.add_employee(

                        int(project_id),

                        data.get('name', ''),

                        data.get('position', ''),

                        data.get('employment_date', ''),

                        data.get('employee_number'),

                        data.get('gender'),

                        data.get('birth_date'),

                        data.get('id_card'),

                        data.get('phone'),

                        data.get('emergency_contact'),

                        data.get('emergency_phone'),

                        data.get('address'),

                        data.get('notes')

                    )

                    print(f"员工添加成功，ID: {employee_id}")

                    self.send_json_response({'success': True, 'employee_id': employee_id})


                except Exception as e:

                    print(f"添加员工到数据库失败: {e}")

                    import traceback

                    traceback.print_exc()

                    self.send_json_response({'success': False, 'error': f'数据库操作失败: {str(e)}'}, status=400)

                print(f"=== 员工添加请求处理完成 ===\n")

            print(f"=== 请求处理完成 ===\n")

        def handle_employee_detail(self):
            """处理单个员工请求"""
            parts = self.path.split('/')
            employee_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not employee_id:
                self.send_json_response({'success': False, 'error': '无效的员工ID'}, status=400)
                return

            if self.command == 'GET':
                try:
                    employee = db.get_employee_details(employee_id)
                    if employee:
                        self.send_json_response(employee)
                    else:
                        self.send_json_response({'success': False, 'error': '员工未找到'}, status=404)
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=500)

            elif self.command == 'PUT':
                data = self.get_request_data()
                try:
                    db.update_employee(employee_id, **data)
                    self.send_json_response({'success': True})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_salary(self):
            """处理薪资相关请求"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            project_id = params.get('project_id', [None])[0]
            salary_month = params.get('month', [None])[0]

            if not project_id:
                self.send_json_response({'success': False, 'error': '需要project_id参数'}, status=400)
                return

            if self.command == 'GET':
                try:
                    salary_records = db.get_salary_records(int(project_id), salary_month)
                    self.send_json_response(salary_records)
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=500)

        def handle_schedule(self):
            """处理排班相关请求 - 支持多种路径格式"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            print(f"=== 处理排班请求 ===")
            print(f"请求路径: {self.path}")
            print(f"请求参数: {params}")

            if self.command == 'GET':
                # 统一处理参数名称
                # 支持 schedule_date 和 date 两种参数名
                schedule_date = params.get('schedule_date', [None])[0] or params.get('date', [None])[0]
                project_id = params.get('project_id', [None])[0]
                employee_id = params.get('employee_id', [None])[0]
                start_date = params.get('start_date', [None])[0]
                end_date = params.get('end_date', [None])[0]

                print(f"解析后的参数:")
                print(f"  project_id: {project_id}")
                print(f"  schedule_date: {schedule_date}")
                print(f"  employee_id: {employee_id}")
                print(f"  start_date: {start_date}")
                print(f"  end_date: {end_date}")

                if project_id and schedule_date:
                    # 获取特定日期的项目排班
                    try:
                        schedule = db.get_project_schedule(int(project_id), schedule_date)
                        self.send_json_response(schedule)
                        return
                    except Exception as e:
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)
                        return
                elif employee_id and start_date and end_date:
                    # 获取员工时间段内的排班
                    try:
                        schedule = db.get_employee_schedule(int(employee_id), start_date, end_date)
                        self.send_json_response(schedule)
                        return
                    except Exception as e:
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)
                        return
                else:
                    # 参数不足
                    error_msg = '需要参数'
                    if not project_id and not employee_id:
                        error_msg = '需要project_id或employee_id参数'
                    elif project_id and not schedule_date:
                        error_msg = '需要schedule_date参数'
                    elif employee_id and (not start_date or not end_date):
                        error_msg = '需要start_date和end_date参数'

                    self.send_json_response({'success': False, 'error': error_msg}, status=400)
                    return

        def handle_attendance(self):
            """处理考勤相关请求"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            project_id = params.get('project_id', [None])[0]
            start_date = params.get('start_date', [None])[0]
            end_date = params.get('end_date', [None])[0]

            if self.command == 'GET':
                if project_id and start_date and end_date:
                    try:
                        report = db.get_attendance_report(int(project_id), start_date, end_date)
                        self.send_json_response(report)
                    except Exception as e:
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)
                else:
                    self.send_json_response({'success': False, 'error': '需要project_id、start_date和end_date参数'},
                                            status=400)

        def handle_performance(self):
            """处理绩效评估请求"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            employee_id = params.get('employee_id', [None])[0]
            limit = params.get('limit', [10])[0]

            if self.command == 'GET':
                if employee_id:
                    try:
                        performance = db.get_employee_performance(int(employee_id), int(limit))
                        self.send_json_response(performance)
                    except Exception as e:
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)
                else:
                    self.send_json_response({'success': False, 'error': '需要employee_id参数'}, status=400)

        def handle_training(self):
            """处理培训相关请求"""
            # 这里可以添加培训相关的处理方法
            self.send_json_response({'success': False, 'error': '培训功能暂未实现'}, status=501)

        def handle_personnel_stats(self):
            """处理人员统计请求"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            project_id = params.get('project_id', [None])[0]

            if self.command == 'GET':
                if project_id:
                    try:
                        stats = db.get_personnel_stats(int(project_id))
                        self.send_json_response(stats)
                    except Exception as e:
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)
                else:
                    self.send_json_response({'success': False, 'error': '需要project_id参数'}, status=400)

        # ================== 原有处理方法 ==================

        def handle_projects(self):
            """处理项目相关请求"""
            if self.command == 'GET':
                # 获取所有项目
                projects = db.get_all_projects()
                self.send_json_response(projects)

            elif self.command == 'POST':
                # 创建新项目
                data = self.get_request_data()
                try:
                    project_id = db.create_project(data.get('name', ''), data.get('description', ''))
                    self.send_json_response({'success': True, 'project_id': project_id})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_single_project(self):
            """处理单个项目请求"""
            # 解析项目ID
            parts = self.path.split('/')
            project_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not project_id:
                self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                return

            if self.command == 'GET':
                # 获取项目信息
                project = db.get_project(project_id)
                if project:
                    self.send_json_response(project)
                else:
                    self.send_json_response({'success': False, 'error': '项目未找到'}, status=404)

            elif self.command == 'DELETE':
                # 删除项目
                try:
                    db.delete_project(project_id)
                    self.send_json_response({'success': True})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_project_partners(self):
            """处理项目合伙人请求"""
            # 解析项目ID
            parts = self.path.split('/')
            project_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not project_id:
                self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                return

            if self.command == 'GET':
                # 获取项目合伙人
                partners = db.get_project_partners(project_id)
                self.send_json_response(partners)

            elif self.command == 'POST':
                # 添加合伙人
                data = self.get_request_data()
                try:
                    partner_id = db.add_partner(
                        project_id,
                        data.get('name', ''),
                        float(data.get('investment', 0)),
                        data.get('contact_info', ''),
                        data.get('notes', '')
                    )
                    self.send_json_response({'success': True, 'partner_id': partner_id})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_partner(self):
            """处理合伙人请求"""
            # 解析合伙人ID
            parts = self.path.split('/')
            partner_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not partner_id:
                self.send_json_response({'success': False, 'error': '无效的合伙人ID'}, status=400)
                return

            if self.command == 'PUT':
                # 更新合伙人
                data = self.get_request_data()
                try:
                    db.update_partner(
                        partner_id,
                        name=data.get('name'),
                        investment=data.get('investment'),
                        contact_info=data.get('contact_info'),
                        notes=data.get('notes')
                    )
                    self.send_json_response({'success': True})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

            elif self.command == 'DELETE':
                # 删除合伙人
                try:
                    db.delete_partner(partner_id)
                    self.send_json_response({'success': True})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_logs(self):
            """处理操作日志请求"""
            # 解析查询参数
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            # 确定project_id
            project_id = None
            if 'project_id' in params:
                project_id = int(params['project_id'][0])

            # 获取limit参数
            limit = 50
            if 'limit' in params:
                limit = int(params['limit'][0])

            logs = db.get_operation_logs(project_id=project_id, limit=limit)
            self.send_json_response(logs)

        def log_message(self, format, *args):
            """重写日志方法，减少输出"""
            pass

        def handle_project_tiers(self):
            """处理项目合伙人级别请求"""
            parts = self.path.split('/')
            project_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not project_id:
                self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                return

            if self.command == 'GET':
                # 获取项目所有合伙人级别
                tiers = db.get_project_tiers(project_id)
                self.send_json_response(tiers)

            elif self.command == 'POST':
                # 创建新的合伙人级别
                data = self.get_request_data()
                try:
                    tier_id = db.create_partner_tier(
                        project_id,
                        data.get('tier_name', ''),
                        data.get('description', ''),
                        float(data.get('management_fee_rate', 0)),
                        float(data.get('performance_fee_rate', 0)),
                        int(data.get('priority', 0))
                    )
                    self.send_json_response({'success': True, 'tier_id': tier_id})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_tier(self):
            """处理合伙人级别请求"""
            # 解析级别ID
            parts = self.path.split('/')
            tier_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not tier_id:
                self.send_json_response({'success': False, 'error': '无效的级别ID'}, status=400)
                return

            if self.command == 'GET':
                # 获取级别信息
                tier = db.get_tier(tier_id)
                if tier:
                    self.send_json_response(tier)
                else:
                    self.send_json_response({'success': False, 'error': '级别未找到'}, status=404)

            elif self.command == 'PUT':
                # 更新级别信息
                data = self.get_request_data()
                try:
                    db.update_partner_tier(
                        tier_id,
                        tier_name=data.get('tier_name'),
                        description=data.get('description'),
                        management_fee_rate=data.get('management_fee_rate'),
                        performance_fee_rate=data.get('performance_fee_rate'),
                        priority=data.get('priority')
                    )
                    self.send_json_response({'success': True})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

            elif self.command == 'DELETE':
                # 删除级别
                try:
                    db.delete_tier(tier_id)
                    self.send_json_response({'success': True})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_partner_tier(self):
            """处理合伙人级别分配请求"""
            parts = self.path.split('/')
            partner_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not partner_id:
                self.send_json_response({'success': False, 'error': '无效的合伙人ID'}, status=400)
                return

            if self.command == 'POST':
                # 分配合伙人到级别
                data = self.get_request_data()
                try:
                    db.assign_partner_to_tier(
                        partner_id,
                        int(data.get('tier_id', 0)),
                        float(data.get('commitment_amount', 0)),
                        float(data.get('distribution_share', 0))
                    )
                    self.send_json_response({'success': True})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)
            elif self.command == 'GET':
                # 获取合伙人级别信息
                try:
                    tier_info = db.get_partner_tier_info(partner_id)
                    if tier_info:
                        self.send_json_response(tier_info)
                    else:
                        self.send_json_response({'success': False, 'error': '合伙人未分配级别'})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_distribution(self):
            """处理收益分配请求"""
            parts = self.path.split('/')
            project_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

            if not project_id:
                self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                return

            if self.command == 'POST':
                # 计算收益分配
                data = self.get_request_data()
                try:
                    distribution_amount = float(data.get('distribution_amount', 0))
                    if distribution_amount <= 0:
                        raise ValueError('分配金额必须大于0')

                    result = db.calculate_distribution(project_id, distribution_amount)
                    self.send_json_response({'success': True, 'data': result})
                except Exception as e:
                    self.send_json_response({'success': False, 'error': str(e)}, status=400)

            elif self.command == 'GET':
                # 获取分配历史
                query_components = urllib.parse.urlparse(self.path)
                params = urllib.parse.parse_qs(query_components.query)

                limit = 50
                if 'limit' in params:
                    limit = int(params['limit'][0])

                history = db.get_distribution_history(project_id, limit=limit)
                self.send_json_response(history)

        def handle_equipment(self):
            """处理设备相关请求"""
            parts = self.path.split('/')

            if len(parts) >= 4 and parts[2] == 'equipment':
                if parts[3] == 'project' and len(parts) > 4:
                    project_id = int(parts[4]) if parts[4].isdigit() else None

                    if not project_id:
                        self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                        return

                    if self.command == 'GET':
                        # 获取项目设备列表
                        try:
                            equipment = db.get_project_equipment(project_id)
                            self.send_json_response(equipment)
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=500)

                    elif self.command == 'POST':
                        # 添加设备
                        data = self.get_request_data()
                        print(f"处理设备添加请求，项目ID: {project_id}, 数据: {data}")
                        try:
                            equipment_id = db.add_equipment(
                                project_id,
                                data.get('equipment_name', ''),
                                data.get('equipment_type', ''),
                                data.get('specification', ''),
                                int(data.get('quantity', 1)),
                                float(data.get('unit_price', 0)),
                                data.get('purchase_date'),
                                data.get('supplier', ''),
                                data.get('warranty_period'),
                                data.get('status', '正常'),
                                data.get('location', ''),
                                data.get('notes', '')
                            )
                            print(f"设备添加成功，ID: {equipment_id}")
                            self.send_json_response({'success': True, 'equipment_id': equipment_id})
                        except Exception as e:
                            print(f"添加设备时出错: {e}")
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                elif parts[3].isdigit():
                    equipment_id = int(parts[3])

                    if self.command == 'PUT':
                        # 更新设备
                        data = self.get_request_data()
                        try:
                            db.update_equipment(
                                equipment_id,
                                equipment_name=data.get('equipment_name'),
                                equipment_type=data.get('equipment_type'),
                                specification=data.get('specification'),
                                quantity=data.get('quantity'),
                                unit_price=data.get('unit_price'),
                                status=data.get('status'),
                                location=data.get('location'),
                                notes=data.get('notes')
                            )
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                    elif self.command == 'DELETE':
                        # 删除设备
                        try:
                            db.delete_equipment(equipment_id)
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_decoration(self):
            """处理装修项目请求"""
            parts = self.path.split('/')

            if len(parts) >= 4 and parts[2] == 'decoration':
                if parts[3] == 'project' and len(parts) > 4:
                    project_id = int(parts[4]) if parts[4].isdigit() else None

                    if not project_id:
                        self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                        return

                    if self.command == 'GET':
                        # 获取项目装修列表
                        try:
                            decoration = db.get_project_decoration(project_id)
                            self.send_json_response(decoration)
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=500)

                    elif self.command == 'POST':
                        # 添加装修项目
                        data = self.get_request_data()
                        try:
                            item_id = db.add_decoration_item(
                                project_id,
                                data.get('item_name', ''),
                                data.get('item_type', ''),
                                data.get('area', ''),
                                data.get('specification', ''),
                                data.get('unit', '项'),
                                float(data.get('quantity', 1)),
                                float(data.get('unit_price', 0)),
                                data.get('contractor', ''),
                                data.get('start_date'),
                                data.get('end_date'),
                                data.get('status', '未开始'),
                                data.get('notes', '')
                            )
                            self.send_json_response({'success': True, 'item_id': item_id})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                elif parts[3].isdigit():
                    item_id = int(parts[3])

                    if self.command == 'PUT':
                        # 更新装修项目
                        data = self.get_request_data()
                        try:
                            # 只传递有值的字段
                            update_data = {}
                            if 'item_name' in data: update_data['item_name'] = data['item_name']
                            if 'item_type' in data: update_data['item_type'] = data['item_type']
                            if 'area' in data: update_data['area'] = data['area']
                            if 'specification' in data: update_data['specification'] = data['specification']
                            if 'unit' in data: update_data['unit'] = data['unit']
                            if 'quantity' in data: update_data['quantity'] = data['quantity']
                            if 'unit_price' in data: update_data['unit_price'] = data['unit_price']
                            if 'contractor' in data: update_data['contractor'] = data['contractor']
                            if 'start_date' in data: update_data['start_date'] = data['start_date']
                            if 'end_date' in data: update_data['end_date'] = data['end_date']
                            if 'status' in data: update_data['status'] = data['status']
                            if 'notes' in data: update_data['notes'] = data['notes']

                            db.update_decoration_item(item_id, **update_data)
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_inventory(self):
            """处理物料库存请求"""
            parts = self.path.split('/')

            if len(parts) >= 4 and parts[2] == 'inventory':
                if parts[3] == 'project' and len(parts) > 4:
                    project_id = int(parts[4]) if parts[4].isdigit() else None

                    if not project_id:
                        self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                        return

                    if self.command == 'GET':
                        # 获取项目物料库存
                        try:
                            inventory = db.get_project_inventory(project_id)
                            self.send_json_response(inventory)
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=500)

                    elif self.command == 'POST':
                        # 添加物料
                        data = self.get_request_data()
                        try:
                            inventory_id = db.add_inventory_item(
                                project_id,
                                data.get('item_name', ''),
                                data.get('category', ''),
                                data.get('specification', ''),
                                data.get('unit', '件'),
                                float(data.get('stock_quantity', 0)),
                                float(data.get('min_quantity', 0)),
                                float(data.get('unit_price', 0)),
                                data.get('supplier', ''),
                                data.get('last_purchase_date'),
                                data.get('expiration_date'),
                                data.get('notes', '')
                            )
                            self.send_json_response({'success': True, 'inventory_id': inventory_id})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                elif parts[3] == 'update' and len(parts) > 4 and parts[4].isdigit():
                    inventory_id = int(parts[4])

                    if self.command == 'POST':
                        # 更新库存数量
                        data = self.get_request_data()
                        try:
                            db.update_inventory_quantity(
                                inventory_id,
                                float(data.get('change_amount', 0)),
                                data.get('change_type', 'in')
                            )
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                elif parts[3].isdigit():
                    inventory_id = int(parts[3])

                    if self.command == 'PUT':
                        # 更新物料信息
                        data = self.get_request_data()
                        try:
                            # 这里需要实现更新物料的方法
                            # 暂时返回成功
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_rooms(self):
            """处理房间管理请求"""
            parts = self.path.split('/')

            if len(parts) >= 4 and parts[2] == 'rooms':
                if parts[3] == 'project' and len(parts) > 4:
                    project_id = int(parts[4]) if parts[4].isdigit() else None

                    if not project_id:
                        self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                        return

                    if self.command == 'GET':
                        # 获取项目房间列表
                        try:
                            rooms = db.get_project_rooms(project_id)
                            self.send_json_response(rooms)
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=500)

                    elif self.command == 'POST':
                        # 添加房间
                        data = self.get_request_data()
                        try:
                            room_id = db.add_room(
                                project_id,
                                data.get('room_number', ''),
                                data.get('room_type', ''),
                                int(data.get('capacity', 1)),
                                float(data.get('area', 0)),
                                float(data.get('hourly_rate', 0)),
                                data.get('status', '空闲'),
                                data.get('equipment_list', ''),
                                data.get('notes', '')
                            )
                            self.send_json_response({'success': True, 'room_id': room_id})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                elif parts[3] == 'update-status' and len(parts) > 4 and parts[4].isdigit():
                    room_id = int(parts[4])

                    if self.command == 'POST':
                        # 更新房间状态
                        data = self.get_request_data()
                        try:
                            db.update_room_status(room_id, data.get('status', '空闲'))
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_budget(self):
            """处理成本预算请求"""
            parts = self.path.split('/')

            if len(parts) >= 4 and parts[2] == 'budget':
                if parts[3] == 'project' and len(parts) > 4:
                    project_id = int(parts[4]) if parts[4].isdigit() else None

                    if not project_id:
                        self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                        return

                    if self.command == 'GET':
                        # 获取项目预算
                        try:
                            budget = db.get_project_budget(project_id)
                            self.send_json_response(budget)
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=500)

                    elif self.command == 'POST':
                        # 添加预算项
                        data = self.get_request_data()
                        try:
                            budget_id = db.add_cost_budget(
                                project_id,
                                data.get('category', ''),
                                data.get('item_name', ''),
                                float(data.get('budget_amount', 0)),
                                float(data.get('actual_amount', 0)),
                                data.get('status', '预算中'),
                                data.get('notes', '')
                            )
                            self.send_json_response({'success': True, 'budget_id': budget_id})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                elif parts[3].isdigit():
                    budget_id = int(parts[3])

                    if self.command == 'PUT':
                        # 更新预算项
                        data = self.get_request_data()
                        try:
                            # 这里需要实现更新预算的方法
                            # 暂时返回成功
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                    elif self.command == 'DELETE':
                        # 删除预算项
                        try:
                            # 这里需要实现删除预算的方法
                            # 暂时返回成功
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_revenue(self):
            """处理收入预测请求"""
            parts = self.path.split('/')

            if len(parts) >= 4 and parts[2] == 'revenue':
                if parts[3] == 'project' and len(parts) > 4:
                    project_id = int(parts[4]) if parts[4].isdigit() else None

                    if not project_id:
                        self.send_json_response({'success': False, 'error': '无效的项目ID'}, status=400)
                        return

                    if self.command == 'GET':
                        # 获取收入预测（支持月份参数）
                        query_components = urllib.parse.urlparse(self.path)
                        params = urllib.parse.parse_qs(query_components.query)

                        months = 6
                        if 'months' in params:
                            months = int(params['months'][0])

                        try:
                            forecast = db.get_revenue_forecast(project_id, months)
                            self.send_json_response(forecast)
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=500)

                    elif self.command == 'POST':
                        # 添加收入预测
                        data = self.get_request_data()
                        try:
                            forecast_id = db.add_revenue_forecast(
                                project_id,
                                data.get('forecast_month', ''),
                                data.get('service_type', ''),
                                float(data.get('unit_price', 0)),
                                int(data.get('expected_quantity', 0)),
                                data.get('notes', '')
                            )
                            self.send_json_response({'success': True, 'forecast_id': forecast_id})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                elif parts[3].isdigit():
                    revenue_id = int(parts[3])

                    if self.command == 'PUT':
                        # 更新收入预测
                        data = self.get_request_data()
                        try:
                            # 这里需要实现更新收入预测的方法
                            # 暂时返回成功
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

                    elif self.command == 'DELETE':
                        # 删除收入预测
                        try:
                            # 这里需要实现删除收入预测的方法
                            # 暂时返回成功
                            self.send_json_response({'success': True})
                        except Exception as e:
                            self.send_json_response({'success': False, 'error': str(e)}, status=400)

        def handle_stats(self):
            """处理统计信息请求"""
            parts = self.path.split('/')

            if len(parts) >= 4 and parts[2] == 'stats' and parts[3].isdigit():
                project_id = int(parts[3])

                if self.command == 'GET':
                    try:
                        stats = db.get_business_stats(project_id)
                        self.send_json_response(stats)
                    except Exception as e:
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)

        def handle_employee_summary(self):
            """处理员工摘要信息请求"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            employee_id = params.get('employee_id', [None])[0]

            if self.command == 'GET':
                if employee_id:
                    try:
                        # 调用数据库方法获取员工摘要信息
                        # 这里需要实现get_employee_summary方法
                        summary = db.get_employee_summary(int(employee_id))
                        if summary:
                            self.send_json_response(summary)
                        else:
                            self.send_json_response({'success': False, 'error': '员工未找到'}, status=404)
                    except Exception as e:
                        print(f"获取员工摘要失败: {e}")
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)
                else:
                    self.send_json_response({'success': False, 'error': '需要employee_id参数'}, status=400)

        def handle_employee_details(self):
            """处理员工详细信息请求"""
            query_components = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(query_components.query)

            employee_id = params.get('employee_id', [None])[0]

            if self.command == 'GET':
                if employee_id:
                    try:
                        # 调用数据库方法获取员工详细信息
                        details = db.get_employee_details(int(employee_id))
                        if details:
                            self.send_json_response(details)
                        else:
                            self.send_json_response({'success': False, 'error': '员工未找到'}, status=404)
                    except Exception as e:
                        print(f"获取员工详情失败: {e}")
                        self.send_json_response({'success': False, 'error': str(e)}, status=500)
                else:
                    self.send_json_response({'success': False, 'error': '需要employee_id参数'}, status=400)

        def handle_create_schedule(self):
            """处理创建排班请求"""
            print(f"=== 处理创建排班请求 ===")
            print(f"请求路径: {self.path}")
            print(f"请求方法: {self.command}")

            if self.command == 'POST':
                try:
                    # 获取请求数据
                    data = self.get_request_data()
                    print(f"创建排班请求数据: {data}")

                    # 验证必要参数
                    required_fields = ['project_id', 'employee_id', 'schedule_date']
                    for field in required_fields:
                        if field not in data:
                            self.send_json_response({
                                'success': False,
                                'error': f'缺少必要参数: {field}'
                            }, status=400)
                            return

                    # 调用数据库方法创建排班 - 修正参数列表
                    schedule_id = db.create_schedule(
                        project_id=int(data['project_id']),
                        employee_id=int(data['employee_id']),
                        schedule_date=data['schedule_date'],
                        shift_type=data.get('shift_type', '白班'),
                        start_time=data.get('start_time', '09:00'),
                        end_time=data.get('end_time', '18:00'),
                        rest_hours=float(data.get('rest_hours', 1.0)),  # 添加 rest_hours 参数
                        room_assignment=data.get('room_assignment', None),  # 添加 room_assignment 参数
                        notes=data.get('notes', '')
                    )

                    print(f"排班创建成功，ID: {schedule_id}")
                    self.send_json_response({
                        'success': True,
                        'schedule_id': schedule_id,
                        'message': '排班创建成功'
                    })

                except Exception as e:
                    print(f"创建排班失败: {e}")
                    import traceback
                    traceback.print_exc()
                    self.send_json_response({
                        'success': False,
                        'error': f'创建排班失败: {str(e)}'
                    }, status=400)
            else:
                self.send_json_response({
                    'success': False,
                    'error': '只支持POST方法'
                }, status=405)

    # 启动服务器
    PORT = 8080
    Handler = InvestmentHandler

    print(f"启动多人合伙投资份额分配系统...")
    print(f"服务器地址: http://localhost:{PORT}")
    print(f"数据库文件: investment.db")
    print(f"HTML文件: {HTML_FILE}")
    print(f"按 Ctrl+C 停止服务器")

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n服务器已停止")


if __name__ == "__main__":
    main()