import sys
import os
import sqlite3
from pathlib import Path
from datetime import datetime

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
    import json
    import urllib.parse

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
                if self.path == '/api/projects':
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
                    self.send_error(404, "API端点未找到")
            except Exception as e:
                print(f"处理API请求时出错: {e}")
                self.send_json_response({
                    'success': False,
                    'error': str(e)
                }, status=500)

        def get_request_data(self):
            """获取请求数据 - 修复编码问题"""
            if 'Content-Length' not in self.headers:
                return {}

            content_length = int(self.headers['Content-Length'])
            raw_data = self.rfile.read(content_length)

            if not raw_data:
                return {}

            # 优先使用 UTF-8 编码
            try:
                decoded_data = raw_data.decode('utf-8')
                data = json.loads(decoded_data)
                return data
            except UnicodeDecodeError:
                # 如果是 UTF-8 解码失败，尝试其他编码
                try:
                    decoded_data = raw_data.decode('gbk')
                    data = json.loads(decoded_data)
                    return data
                except Exception:
                    # 如果还是失败，尝试忽略错误
                    try:
                        decoded_data = raw_data.decode('utf-8', errors='ignore')
                        data = json.loads(decoded_data)
                        return data
                    except Exception as e:
                        print(f"无法解码请求数据: {e}")
                        print(f"数据前100字节: {raw_data[:100]}")
                        return {}
            except json.JSONDecodeError as e:
                print(f"JSON解析错误: {e}")
                print(f"原始数据: {raw_data[:100]}...")
                return {}

        def send_json_response(self, data, status=200):
            """发送JSON响应"""
            self.send_response(status)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            # 关键：使用ensure_ascii=False来正确处理中文字符
            json_str = json.dumps(data, ensure_ascii=False)
            self.wfile.write(json_str.encode('utf-8'))

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