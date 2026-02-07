"""
飞书多维表格数据推送模块
自动将爬虫数据推送到飞书，支持去重和字段映射
"""

import requests
import json
import os
from datetime import datetime

class FeishuUploader:
    def __init__(self):
        self.app_id = os.environ.get('FEISHU_APP_ID')
        self.app_secret = os.environ.get('FEISHU_APP_SECRET')
        self.base_id = os.environ.get('FEISHU_BASE_ID')
        self.table_id = os.environ.get('FEISHU_TABLE_ID')
        self.access_token = None
        
        if not all([self.app_id, self.app_secret, self.base_id, self.table_id]):
            raise ValueError("缺少飞书配置环境变量")
    
    def get_access_token(self):
        """获取飞书 access_token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
        headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        resp = requests.post(url, headers=headers, json=data)
        result = resp.json()
        
        if result.get("code") == 0:
            self.access_token = result["tenant_access_token"]
            return self.access_token
        else:
            raise Exception(f"获取token失败: {result}")
    
    def get_existing_records(self):
        """获取已有记录（用于去重）"""
        if not self.access_token:
            self.get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.base_id}/tables/{self.table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        
        all_records = []
        page_token = None
        
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            resp = requests.get(url, headers=headers, params=params)
            result = resp.json()
            
            if result.get("code") != 0:
                raise Exception(f"获取记录失败: {result}")
            
            items = result.get("data", {}).get("items", [])
            for item in items:
                fields = item.get("fields", {})
                all_records.append({
                    "record_id": item.get("record_id"),
                    "url": fields.get("来源URL", {}).get("link", "") if isinstance(fields.get("来源URL"), dict) else fields.get("来源URL", ""),
                    "title": fields.get("项目名称", "")
                })
            
            # 检查是否还有下一页
            page_token = result.get("data", {}).get("page_token")
            has_more = result.get("data", {}).get("has_more", False)
            
            if not has_more or not page_token:
                break
        
        return all_records
    
    def map_to_feishu_fields(self, item):
        """
        将爬虫数据映射到飞书字段格式
        根据你的56字段模板，这里先映射核心字段
        """
        # 处理超链接字段
        url = item.get("来源URL", "")
        title = item.get("项目名称", "")
        
        return {
            "项目名称": title,
            "数据来源": item.get("数据来源", ""),
            "来源URL": {
                "link": url,
                "text": "查看原文" if len(title) < 5 else title[:50]
            } if url else title,
            "原文摘要": item.get("原文摘要", "")[:2000],  # 飞书文本字段限制
            "近期规模_万吨每日": self._extract_number(item.get("近期规模", "")),
            "工程总投资_亿元": self._extract_number(item.get("工程总投资", "")),
            "地理位置": item.get("地理位置", ""),
            "投资方总包方": item.get("投资方/总包方", ""),
            "抓取时间": item.get("抓取时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "数据置信度": item.get("数据置信度", "中"),
            "处理状态": "待清洗",
            "关联项目ID": ""
        }
    
    def _extract_number(self, value):
        """从字符串提取数字"""
        if isinstance(value, (int, float)):
            return float(value)
        if not value:
            return None
        
        import re
        match = re.search(r'(\d+\.?\d*)', str(value))
        return float(match.group(1)) if match else None
    
    def add_records(self, records):
        """批量添加记录到飞书"""
        if not self.access_token:
            self.get_access_token()
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.base_id}/tables/{self.table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        # 飞书限制每次最多 500 条
        batch_size = 100
        success_count = 0
        failed_records = []
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            data = {
                "records": [{"fields": r} for r in batch]
            }
            
            resp = requests.post(url, headers=headers, json=data)
            result = resp.json()
            
            if result.get("code") == 0:
                success_count += len(batch)
                print(f"✓ 成功添加 {len(batch)} 条记录")
            else:
                print(f"✗ 批量添加失败: {result}")
                failed_records.extend(batch)
        
        return success_count, failed_records
    
    def upload_data(self, crawler_data):
        """
        主入口：上传爬虫数据，自动去重
        crawler_data: 爬虫生成的列表，每个元素是字典
        """
        print(f"开始上传数据，共 {len(crawler_data)} 条...")
        
        # 1. 获取已有记录（用于去重）
        print("获取已有记录进行去重...")
        existing = self.get_existing_records()
        existing_urls = {r["url"] for r in existing if r["url"]}
        print(f"已有 {len(existing)} 条记录，{len(existing_urls)} 个唯一URL")
        
        # 2. 过滤新数据
        new_records = []
        skipped = 0
        
        for item in crawler_data:
            url = item.get("来源URL", "")
            if url and url in existing_urls:
                skipped += 1
                continue
            
            mapped = self.map_to_feishu_fields(item)
            new_records.append(mapped)
        
        print(f"新数据 {len(new_records)} 条，跳过重复 {skipped} 条")
        
        if not new_records:
            print("没有新数据需要上传")
            return 0
        
        # 3. 批量上传
        print(f"开始上传 {len(new_records)} 条新记录...")
        success, failed = self.add_records(new_records)
        
        print(f"\n上传完成: 成功 {success} 条，失败 {len(failed)} 条")
        if failed:
            print("失败记录示例:", failed[:2])
        
        return success

def main():
    """命令行入口：从JSON文件读取数据并上传"""
    import sys
    
    json_file = sys.argv[1] if len(sys.argv) > 1 else "underground_wastewater_data.json"
    
    if not os.path.exists(json_file):
        print(f"错误: 找不到文件 {json_file}")
        return
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"从 {json_file} 读取了 {len(data)} 条数据")
    
    uploader = FeishuUploader()
    uploaded = uploader.upload_data(data)
    
    print(f"\n总计上传: {uploaded} 条新记录到飞书")

if __name__ == "__main__":
    main()
