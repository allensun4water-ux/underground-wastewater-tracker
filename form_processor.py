"""
处理飞书表单提交的"随时丢"数据
读取表单表格 → 提取链接内容 → 推送到主数据表
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
        self.token = None
        
        # 获取token
        self._get_token()
    
    def _get_token(self):
        """获取飞书token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": os.environ.get('FEISHU_APP_ID'),
            "app_secret": os.environ.get('FEISHU_APP_SECRET')
        })
        self.token = resp.json().get("tenant_access_token")
    
    def get_form_records(self, hours=24):
        """获取最近提交的表单记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.form_base_id}/tables/{self.form_table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        # 计算时间过滤（最近24小时）
        since = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        
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
                # 只取未处理的（处理状态为空或"待处理"）
                if fields.get("处理状态") in [None, "", "待处理"]:
                    records.append({
                        "record_id": item.get("record_id"),
                        "url": fields.get("来源URL", ""),
                        "remark": fields.get("原文摘要", ""),
                        "submit_time": fields.get("抓取时间", "")
                    })
            
            page_token = result.get("data", {}).get("page_token")
            if not result.get("data", {}).get("has_more"):
                break
        
        print(f"找到 {len(records)} 条待处理表单记录")
        return records
    
    def extract_from_url(self, url):
        """从URL提取内容（简化版，实际调用manual_processor逻辑）"""
        import subprocess
        result = subprocess.run(
            ["python", "manual_processor.py", url],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            with open('data_to_upload.json', 'r') as f:
                data = json.load(f)
            return data[0] if data else None
        return None
    
    def mark_processed(self, record_id):
        """标记表单记录为已处理"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.form_base_id}/tables/{self.form_table_id}/records/{record_id}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        data = {
            "fields": {
                "处理状态": "已处理",
                "数据置信度": "中"
            }
        }
        resp = requests.put(url, headers=headers, json=data)
        return resp.json().get("code") == 0
    
    def process_all(self):
        """处理所有待处理记录"""
        records = self.get_form_records()
        if not records:
            print("没有新的表单提交")
            return 0
        
        processed = 0
        for record in records:
            print(f"\n处理: {record['url'][:60]}...")
            
            # 提取内容
            data = self.extract_from_url(record['url'])
            if not data:
                print("  提取失败，跳过")
                continue
            
            # 保存为JSON（给uploader使用）
            with open('form_data_temp.json', 'w', encoding='utf-8') as f:
                json.dump([data], f, ensure_ascii=False)
            
            # 推送到主表（调用uploader）
            import subprocess
            env = os.environ.copy()
            env['FEISHU_BASE_ID'] = self.main_base_id
            env['FEISHU_TABLE_ID'] = self.main_table_id
            
            result = subprocess.run(
                ["python", "feishu_uploader.py", "form_data_temp.json"],
                capture_output=True,
                text=True,
                env=env
            )
            
            if "成功" in result.stdout or "上传完成" in result.stdout:
                # 标记表单为已处理
                if self.mark_processed(record['record_id']):
                    print("  ✓ 处理完成并已标记")
                    processed += 1
                else:
                    print("  ⚠️ 推送成功但标记失败")
            else:
                print(f"  ✗ 推送失败: {result.stdout[:200]}")
        
        return processed

if __name__ == "__main__":
    processor = FormProcessor()
    count = processor.process_all()
    print(f"\n总计处理: {count} 条表单提交")
