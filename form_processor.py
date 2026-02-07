"""
处理飞书表单提交的"随时丢"数据
"""

import os
import json
import requests
from datetime import datetime, timedelta

class FormProcessor:
    def __init__(self):
        self.form_base_id = os.environ.get('FEISHU_FORM_BASE_ID')
        self.form_table_id = os.environ.get('FEISHU_FORM_TABLE_ID')
        self.main_base_id = os.environ.get('FEISHU_BASE_ID')
        self.main_table_id = os.environ.get('FEISHU_TABLE_ID')
        self.app_id = os.environ.get('FEISHU_APP_ID')
        self.app_secret = os.environ.get('FEISHU_APP_SECRET')
        self.token = None
        
        self._get_token()
    
    def _get_token(self):
        """获取飞书token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": self.app_id,
            "app_secret": self.app_secret
        })
        self.token = resp.json().get("tenant_access_token")
        print(f"Token获取: {'成功' if self.token else '失败'}")
    
    def get_form_records(self):
        """获取待处理的表单记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.form_base_id}/tables/{self.form_table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        records = []
        page_token = None
        
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            resp = requests.get(url, headers=headers, params=params)
            result = resp.json()
            
            if result.get("code") != 0:
                print(f"获取表单数据失败: {result}")
                break
            
            items = result.get("data", {}).get("items", [])
            for item in items:
                fields = item.get("fields", {})
                
                # 自动填充缺失字段
                record = {
                    "record_id": item.get("record_id"),
                    "url": self._get_url_value(fields.get("来源URL")),
                    "remark": fields.get("原文摘要", ""),
                    "submit_time": fields.get("抓取时间") or item.get("created_time") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "data_source": fields.get("数据来源") or "用户提交",
                    "status": fields.get("处理状态") or "待处理"
                }
                
                # 只取未处理的
                if record["status"] in ["待处理", "", None]:
                    records.append(record)
                    print(f"找到待处理记录: {record['url'][:50]}...")
            
            page_token = result.get("data", {}).get("page_token")
            if not result.get("data", {}).get("has_more"):
                break
        
        print(f"\n总计找到 {len(records)} 条待处理记录")
        return records
    
    def _get_url_value(self, url_field):
        """处理超链接字段（可能是对象或字符串）"""
        if isinstance(url_field, dict):
            return url_field.get("link", "")
        return str(url_field) if url_field else ""
    
    def extract_from_url(self, url):
        """从URL提取内容"""
        import requests
        from bs4 import BeautifulSoup
        import re
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 提取标题
            title = soup.title.string if soup.title else ''
            h1 = soup.find('h1')
            if h1:
                title = h1.get_text(strip=True)
            
            # 提取正文
            content = ''
            for selector in ['article', '.content', '.article', '#content', '.detail']:
                tag = soup.select_one(selector)
                if tag:
                    content = tag.get_text(separator='\n', strip=True)
                    break
            
            if not content:
                content = soup.get_text(separator='\n', strip=True)
            
            # 简单提取
            text = title + ' ' + content
            
            data = {
                "项目名称": title[:100] if title else "未识别项目",
                "数据来源": "用户提交",
                "来源URL": url,
                "原文摘要": content[:2000],
                "近期规模_万吨每日": None,
                "工程总投资_亿元": None,
                "地理位置": "",
                "投资方总包方": "",
                "抓取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "数据置信度": "低",  # 手动提交需要人工确认
                "处理状态": "待清洗"
            }
            
            # 尝试提取数字
            scale_match = re.search(r'(\d+\.?\d*)\s*万\s*吨', text)
            if scale_match:
                data["近期规模_万吨每日"] = float(scale_match.group(1))
            
            inv_match = re.search(r'(\d+\.?\d*)\s*亿', text)
            if inv_match:
                data["工程总投资_亿元"] = float(inv_match.group(1))
            
            return data
            
        except Exception as e:
            print(f"提取失败: {e}")
            return None
    
    def push_to_main(self, data):
        """推送到主数据表"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.main_base_id}/tables/{self.main_table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # 处理超链接格式
        url_value = data.get("来源URL", "")
        if url_value and url_value.startswith("http"):
            url_field = {"link": url_value, "text": "查看原文"}
        else:
            url_field = url_value
        
        record_data = {
            "fields": {
                "项目名称": data.get("项目名称", ""),
                "数据来源": data.get("数据来源", "用户提交"),
                "来源URL": url_field,
                "原文摘要": data.get("原文摘要", "")[:2000],
                "近期规模_万吨每日": data.get("近期规模_万吨每日"),
                "工程总投资_亿元": data.get("工程总投资_亿元"),
                "地理位置": data.get("地理位置", ""),
                "投资方总包方": data.get("投资方总包方", ""),
               "抓取时间": int(datetime.now().timestamp()) * 1000,  # 飞书需要毫秒时间戳,
                "数据置信度": data.get("数据置信度", "低"),
                "处理状态": data.get("处理状态", "待清洗")
            }
        }
        
        resp = requests.post(url, headers=headers, json=record_data)
        result = resp.json()
        
        if result.get("code") == 0:
            print("  ✓ 推送到主表成功")
            return True
        else:
            print(f"  ✗ 推送失败: {result.get('msg', '未知错误')}")
            return False
    
    def mark_processed(self, record_id):
        """标记为已处理"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.form_base_id}/tables/{self.form_table_id}/records/{record_id}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        data = {
            "fields": {
                "处理状态": "已处理"
            }
        }
        resp = requests.put(url, headers=headers, json=data)
        return resp.json().get("code") == 0
    
    def process_all(self):
        """主流程"""
        records = self.get_form_records()
        if not records:
            print("没有待处理的表单提交")
            return 0
        
        success = 0
        for record in records:
            print(f"\n处理记录: {record['url'][:60]}...")
            
            # 提取内容
            data = self.extract_from_url(record['url'])
            if not data:
                print("  提取内容失败，跳过")
                continue
            
            # 推送到主表
            if self.push_to_main(data):
                # 标记已处理
                if self.mark_processed(record['record_id']):
                    print("  ✓ 完成")
                    success += 1
                else:
                    print("  ⚠️ 推送成功但标记失败")
            else:
                print("  ✗ 推送失败")
        
        print(f"\n总计: 处理 {len(records)} 条，成功 {success} 条")
        return success

if __name__ == "__main__":
    processor = FormProcessor()
    processor.process_all()
