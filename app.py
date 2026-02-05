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
                if self.path == '/api/projects':
                    self.handle_projects()
                elif self.path.startswith('/api/project/'):
                    if '/partners' in self.path:
                        self.handle_project_partners()
                    else:
                        self.handle_single_project()
                elif self.path.startswith('/api/partner/'):
                    self.handle_partner()
                elif self.path.startswith('/api/logs'):
                    self.handle_logs()
                else:
                    self.send_error(404, "API端点未找到")
            except Exception as e:
                # 直接发送JSON错误响应，避免Unicode编码问题
                self.send_json_response({
                    'success': False,
                    'error': str(e)
                }, status=500)

        def get_request_data(self):
            """获取请求数据"""
            if 'Content-Length' not in self.headers:
                return {}

            content_length = int(self.headers['Content-Length'])
            raw_data = self.rfile.read(content_length)

            try:
                return json.loads(raw_data.decode('utf-8'))
            except:
                return {}

        def send_json_response(self, data, status=200):
            """发送JSON响应"""
            self.send_response(status)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

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
