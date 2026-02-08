"""
飞书机器人处理 - 整合Kimi提取、项目匹配、网页存档
"""

import os
import json
import requests
import re
from datetime import datetime

from kimi_extractor import KimiExtractor
from project_matcher import ProjectMatcher
from archiver import WebArchiver


class FeishuBot:
    """飞书机器人 - 完整版"""
    
    def __init__(self):
        # 飞书配置
        self.webhook = os.environ.get('FEISHU_BOT_WEBHOOK')
        self.app_id = os.environ.get('FEISHU_APP_ID')
        self.app_secret = os.environ.get('FEISHU_APP_SECRET')
        
        # 表格配置
        self.main_base = os.environ.get('FEISHU_BASE_ID')  # 主表
        self.main_table = os.environ.get('FEISHU_TABLE_ID')
        self.detail_base = os.environ.get('FEISHU_DETAIL_BASE_ID', self.main_base)  # 明细表
        self.detail_table = os.environ.get('FEISHU_DETAIL_TABLE_ID')
        
        # 初始化组件
        self.extractor = KimiExtractor()
        self.matcher = ProjectMatcher()
        self.archiver = WebArchiver()
        
        self.token = None
    
    def get_token(self):
        """获取飞书token"""
        if self.token:
            return self.token
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": self.app_id,
            "app_secret": self.app_secret
        })
        self.token = resp.json().get("tenant_access_token")
        return self.token
    
    def send_message(self, content, msg_type="text"):
        """发送消息到群"""
        if not self.webhook:
            print("没有配置 WEBHOOK")
            return False
        
        if msg_type == "text":
            data = {"msg_type": "text", "content": {"text": content}}
        else:
            data = msg_type
        
        try:
            resp = requests.post(self.webhook, json=data, timeout=10)
            return resp.json().get("code") == 0
        except Exception as e:
            print(f"发送消息失败: {e}")
            return False
    
    def fetch_webpage(self, url):
        """获取网页内容"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'utf-8'
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            title = soup.title.string if soup.title else ""
            # 提取正文（简化）
            content = ""
            for tag in soup.find_all(['p', 'article', 'div']):
                text = tag.get_text(strip=True)
                if len(text) > 50:  # 过滤短文本
                    content += text + "\n"
                    if len(content) > 8000:
                        break
            
            return {
                'success': True,
                'title': title,
                'content': content[:6000],
                'html': resp.text
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_existing_projects(self):
        """获取主表所有项目（用于匹配）"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.main_base}/tables/{self.main_table}/records"
        headers = {"Authorization": f"Bearer {self.get_token()}"}
        
        projects = []
        page_token = None
        
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            resp = requests.get(url, headers=headers, params=params)
            result = resp.json()
            
            if result.get("code") != 0:
                print(f"获取项目失败: {result}")
                break
            
            items = result.get("data", {}).get("items", [])
            for item in items:
                fields = item.get("fields", {})
                fields['_record_id'] = item.get("record_id")
                projects.append(fields)
            
            page_token = result.get("data", {}).get("page_token")
            if not result.get("data", {}).get("has_more"):
                break
        
        return projects
    
    def add_to_main_table(self, project_data):
        """添加到项目主表"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.main_base}/tables/{self.main_table}/records"
        headers = {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json"
        }
        
        # 准备字段
        fields = {}
        for key, val in project_data.items():
            if key.startswith('_'):
                continue
            
            # 处理超链接
            if key == '来源URL' and val:
                if isinstance(val, str) and val.startswith('http'):
                    fields[key] = {"link": val, "text": "查看原文"}
                else:
                    fields[key] = val
            # 处理日期时间（转毫秒时间戳）
            elif key in ['创建时间', '最后更新时间'] and val:
                try:
                    dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                    fields[key] = int(dt.timestamp()) * 1000
                except:
                    fields[key] = val
            else:
                fields[key] = val
        
        resp = requests.post(url, headers=headers, json={"fields": fields})
        result = resp.json()
        
        if result.get("code") == 0:
            return result['data']['record']['record_id']
        else:
            print(f"添加主表失败: {result}")
            return None
    
    def update_main_table(self, record_id, project_data):
        """更新项目主表"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.main_base}/tables/{self.main_table}/records/{record_id}"
        headers = {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json"
        }
        
        # 过滤字段
        fields = {k: v for k, v in project_data.items() if not k.startswith('_')}
        
        resp = requests.put(url, headers=headers, json={"fields": fields})
        return resp.json().get("code") == 0
    
    def add_to_detail_table(self, detail_data):
        """添加到信息明细表"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.detail_base}/tables/{self.detail_table}/records"
        headers = {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json"
        }
        
        # 处理字段
        fields = {}
        for key, val in detail_data.items():
            if key == '来源URL' and val and isinstance(val, str):
                fields[key] = {"link": val, "text": "查看原文"}
            else:
                fields[key] = val
        
        resp = requests.post(url, headers=headers, json={"fields": fields})
        return resp.json().get("code") == 0
    
    def process_message(self, message_text):
        """处理用户消息（完整流程）"""
        # 1. 提取链接
        url_pattern = r'https?://[^\s<>\"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, message_text)
        
        if not urls:
            self.send_message("⚠️ 没有找到链接，请发送包含链接的消息")
            return False
        
        url = urls[0]
        self.send_message(f"🤖 收到链接，开始处理...\n{url[:60]}...")
        
        # 2. 获取网页
        self.send_message("📄 正在获取网页内容...")
        web_data = self.fetch_webpage(url)
        
        if not web_data['success']:
            self.send_message(f"⚠️ 获取网页失败: {web_data.get('error')}")
            return False
        
        # 3. 网页存档
        self.send_message("💾 正在存档网页...")
        archive_info = self.archiver.archive(url, "temp")
        
        # 4. Kimi提取
        self.send_message("🧠 正在AI提取信息（约10-20秒）...")
        try:
            extracted, raw = self.extractor.extract(
                url,
                web_data['title'],
                web_data['content']
            )
        except Exception as e:
            self.send_message(f"⚠️ AI提取失败: {e}")
            return False
        
        # 5. 项目匹配
        self.send_message("🔍 正在匹配项目...")
        existing_projects = self.get_existing_projects()
        matched_project, similarity = self.matcher.find_match(extracted, existing_projects)
        
        # 6. 处理结果
        if matched_project:
            # 合并现有项目
            self.send_message(f"📌 找到相似项目（相似度{similarity:.0%}），正在合并信息...")
            
            merged, conflicts, updates = self.matcher.merge_projects(
                matched_project,
                extracted,
                {'数据来源': '用户提交-飞书机器人'}
            )
            
            # 更新主表
            record_id = matched_project['_record_id']
            self.update_main_table(record_id, merged)
            
            # 添加明细
            detail = {
                '关联项目ID': merged['项目ID'],
                '数据来源': '用户提交-飞书机器人',
                '来源URL': url,
                '抓取时间': int(datetime.now().timestamp()) * 1000,
                '原始标题': web_data['title'],
                '原始摘要': extracted.get('_summary', '')[:500],
                '提取完整度': extracted.get('_completeness', '0%'),
                'HTML存档链接': archive_info.get('html_path', '') if archive_info else '',
                '数据置信度': '中',
                '原始提取JSON': json.dumps(extracted, ensure_ascii=False)[:2000]
            }
            self.add_to_detail_table(detail)
            
            # 发送结果
            conflict_text = ""
            if conflicts:
                conflict_text = f"\n⚠️ 发现 {len(conflicts)} 处信息冲突，请人工确认"
            
            card = {
                "msg_type": "interactive",
                "card": {
                    "config": {"wide_screen_mode": True},
                    "header": {
                        "title": {"tag": "plain_text", "content": "✅ 信息已合并"},
                        "template": "green"
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": f"**{merged['项目名称']}**\n\n"
                                          f"📍 {merged.get('地理位置', '未识别')}\n"
                                          f"💧 规模：{merged.get('近期规模', '未识别')} 万吨/日\n"
                                          f"💰 投资：{merged.get('工程总投资', '未识别')} 亿元\n"
                                          f"📊 完整度：{merged.get('信息完整度', '0%')}\n"
                                          f"📎 来源数：{merged.get('信息来源数量', 1)} 个{conflict_text}"
                            }
                        },
                        {
                            "tag": "action",
                            "actions": [
                                {
                                    "tag": "button",
                                    "text": {"tag": "plain_text", "content": "查看项目详情"},
                                    "type": "primary",
                                    "url": f"https://fcnilpup9rvl.feishu.cn/base/{self.main_base}"
                                }
                            ]
                        }
                    ]
                }
            }
            self.send_message("", card)
            
        else:
            # 新项目
            self.send_message("🆕 未找到匹配项目，创建新项目...")
            
            new_project = self.matcher.create_new_project(
                extracted,
                {'数据来源': '用户提交-飞书机器人'}
            )
            
            # 添加主表
            record_id = self.add_to_main_table(new_project)
            
            if record_id:
                # 添加明细
                detail = {
                    '关联项目ID': new_project['项目ID'],
                    '数据来源': '用户提交-飞书机器人',
                    '来源URL': url,
                    '抓取时间': int(datetime.now().timestamp()) * 1000,
                    '原始标题': web_data['title'],
                    '原始摘要': extracted.get('_summary', '')[:500],
                    '提取完整度': extracted.get('_completeness', '0%'),
                    'HTML存档链接': archive_info.get('html_path', '') if archive_info else '',
                    '数据置信度': '中',
                    '原始提取JSON': json.dumps(extracted, ensure_ascii=False)[:2000]
                }
                self.add_to_detail_table(detail)
                
                # 发送结果
                card = {
                    "msg_type": "interactive",
                    "card": {
                        "config": {"wide_screen_mode": True},
                        "header": {
                            "title": {"tag": "plain_text", "content": "✅ 新项目已创建"},
                            "template": "blue"
                        },
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**{new_project['项目名称']}**\n\n"
                                              f"📍 {new_project.get('地理位置', '未识别')}\n"
                                              f"💧 规模：{new_project.get('近期规模', '未识别')} 万吨/日\n"
                                              f"💰 投资：{new_project.get('工程总投资', '未识别')} 亿元\n"
                                              f"📊 完整度：{new_project.get('信息完整度', '0%')}\n"
                                              f"🆔 项目ID：{new_project['项目ID'][:8]}..."
                                }
                            },
                            {
                                "tag": "action",
                                "actions": [
                                    {
                                        "tag": "button",
                                        "text": {"tag": "plain_text", "content": "查看表格"},
                                        "type": "primary",
                                        "url": f"https://fcnilpup9rvl.feishu.cn/base/{self.main_base}"
                                    }
                                ]
                            }
                        ]
                    }
                }
                self.send_message("", card)
            else:
                self.send_message("⚠️ 创建项目失败")
        
        return True


def main():
    """命令行入口"""
    import sys
    
    message = sys.argv[1] if len(sys.argv) > 1 else "测试 https://example.com"
    
    bot = FeishuBot()
    bot.process_message(message)


if __name__ == "__main__":
    main()
