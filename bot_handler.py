# -*- coding: utf-8 -*-
import json
import os
import urllib.request
import urllib.error
import re
from datetime import datetime

def http_post(url, headers=None, data=None, timeout=10):
    """HTTP POST"""
    req_headers = headers or {}
    if data:
        json_data = json.dumps(data).encode('utf-8')
        req_headers['Content-Type'] = 'application/json'
    else:
        json_data = None
    
    req = urllib.request.Request(url, data=json_data, headers=req_headers, method='POST')
    
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8')
    except Exception as e:
        return 0, str(e)

# 配置
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GITHUB_REPO = "allensun4water-ux/underground-wastewater-tracker"
FEISHU_APP_ID = os.environ.get('FEISHU_APP_ID')
FEISHU_APP_SECRET = os.environ.get('FEISHU_APP_SECRET')

def extract_with_kimi(url, title, content):
    """调用 Kimi API 提取信息"""
    if not KIMI_API_KEY:
        print("没有 KIMI_API_KEY，使用简单提取")
        return simple_extract(url, title, content)
    
    api_url = "https://api.moonshot.cn/v1/chat/completions"
    
    prompt = f"""从以下新闻中提取地下式污水处理厂的信息，返回JSON格式。

URL: {url}
标题: {title}
正文: {content[:6000]}

请提取以下字段，没有的信息填 null：
- 项目名称（标准全称）
- 近期规模（万吨/日，数字）
- 工程总投资（亿元，数字）
- 地理位置（省·市）
- 投资方/总包方（公司全称）
- 设计方（公司全称）
- 施工方（公司全称）
- 水处理流程（如AAO+MBR）
- 执行标准（如一级A、地表IV类）
- 原文摘要（前500字）

返回格式：
{{
    "项目名称": "...",
    "近期规模": 15,
    "工程总投资": 12.5,
    "地理位置": "浙江嘉兴",
    "投资方/总包方": "...",
    "设计方": null,
    "施工方": null,
    "水处理流程": "AAO+MBR",
    "执行标准": "一级A",
    "原文摘要": "..."
}}
"""
    
    headers = {
        "Authorization": f"Bearer {KIMI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "moonshot-v1-8k",
        "messages": [
            {"role": "system", "content": "你是专业的环保工程信息提取助手。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }
    
    try:
        status, resp_text = http_post(api_url, headers=headers, data=data, timeout=60)
        if status == 200:
            result = json.loads(resp_text)
            content = result['choices'][0]['message']['content']
            extracted = json.loads(content)
            extracted['_source'] = 'kimi'
            return extracted
        else:
            print(f"Kimi API 错误: {status} {resp_text[:200]}")
            return simple_extract(url, title, content)
    except Exception as e:
        print(f"Kimi 调用异常: {e}")
        return simple_extract(url, title, content)

def simple_extract(url, title, content):
    """简单正则提取（备用）"""
    data = {
        "项目名称": title[:50] if title else "未识别项目",
        "近期规模": None,
        "工程总投资": None,
        "地理位置": "",
        "投资方/总包方": "",
        "设计方": None,
        "施工方": None,
        "水处理流程": "",
        "执行标准": "",
        "原文摘要": content[:500],
        "_source": "simple"
    }
    
    # 规模
    scale_match = re.search(r'(\d+\.?\d*)\s*万\s*吨', content)
    if scale_match:
        data["近期规模"] = float(scale_match.group(1))
    
    # 投资
    inv_match = re.search(r'(\d+\.?\d*)\s*亿', content)
    if inv_match:
        data["工程总投资"] = float(inv_match.group(1))
    
    # 地理位置
    provinces = ['北京', '天津', '上海', '重庆', '河北', '山西', '辽宁', '吉林', 
                 '黑龙江', '江苏', '浙江', '安徽', '福建', '江西', '山东', '河南',
                 '湖北', '湖南', '广东', '海南', '四川', '贵州', '云南', '陕西',
                 '甘肃', '青海', '内蒙古', '广西', '西藏', '宁夏', '新疆']
    for prov in provinces:
        if prov in content:
            data["地理位置"] = prov
            break
    
    # 公司
    company_pattern = r'(中国.*?公司|.*?集团|.*?市政|.*?环保|.*?水务|.*?建设)'
    companies = re.findall(company_pattern, content)
    if companies:
        data["投资方/总包方"] = companies[0][:50]
    
    # 工艺
    if 'AAO' in content or 'A2O' in content:
        data["水处理流程"] = "AAO"
    elif 'MBR' in content:
        data["水处理流程"] = "MBR"
    
    # 标准
    if '一级A' in content:
        data["执行标准"] = "一级A"
    elif '地表IV' in content or '地表四' in content:
        data["执行标准"] = "地表IV类"
    
    return data

def send_feishu_message(chat_id, content):
    """发送消息到飞书"""
    try:
        # 获取token
        token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        status, resp_text = http_post(token_url, data={
            "app_id": FEISHU_APP_ID,
            "app_secret": FEISHU_APP_SECRET
        })
        
        if status != 200:
            print(f"获取token失败: {status}")
            return False
        
        token = json.loads(resp_text).get("tenant_access_token")
        if not token:
            return False
        
        # 发送消息
        url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
        headers = {"Authorization": f"Bearer {token}"}
        data = {
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": content[:500]})
        }
        
        status, _ = http_post(url, headers=headers, data=data)
        return status == 200
    except Exception as e:
        print(f"发送消息失败: {e}")
        return False

def fetch_webpage(url):
    """获取网页内容"""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode('utf-8')
            
            # 简单提取标题和正文
            title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
            title = title_match.group(1).strip() if title_match else ""
            
            # 去除标签，保留文本
            text = re.sub(r'<[^>]+>', ' ', html)
            text = re.sub(r'\s+', ' ', text).strip()
            
            return {"success": True, "title": title, "content": text[:8000]}
    except Exception as e:
        return {"success": False, "error": str(e)}

def archive_webpage(url, project_id):
    """存档网页"""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode('utf-8')
            
            # 保存文件
            import hashlib
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            filename = f"web_archives/{project_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{url_hash}.html"
            
            os.makedirs("web_archives", exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html)
            
            return {"success": True, "path": filename}
    except Exception as e:
        return {"success": False, "error": str(e)}

def push_to_feishu(extracted, url):
    """推送到飞书多维表格（只推主表存在的11个字段）"""
    
    # 准备token
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    status, resp_text = http_post(token_url, data={
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_SECRET
    })
    
    if status != 200:
        return False, f"获取token失败: {status}"
    
    token = json.loads(resp_text).get("tenant_access_token")
    if not token:
        return False, "token为空"
    
    # 主表配置
    base_id = os.environ.get('FEISHU_BASE_ID')
    table_id = os.environ.get('FEISHU_TABLE_ID')
    
    # 只推送主表存在的11个字段
    url_field = {"link": url, "text": "查看原文"} if url else ""
    
    fields = {
        "项目名称": extracted.get("项目名称", "未识别"),
        "数据来源": "用户提交-飞书机器人",
        "来源URL": url_field,
        "原文摘要": extracted.get("原文摘要", "")[:2000],
        "近期规模_万吨每日": extracted.get("近期规模"),
        "工程总投资_亿元": extracted.get("工程总投资"),
        "地理位置": extracted.get("地理位置", ""),
        "投资方总包方": extracted.get("投资方/总包方", ""),
        "抓取时间": int(datetime.now().timestamp()) * 1000,
        "数据置信度": "高" if extracted.get("_source") == "kimi" else "中",
        "处理状态": "待清洗"
    }
    
    # 过滤None值
    for k, v in fields.items():
        if v is None:
            fields[k] = ""
    
    record_data = {"fields": fields}
    
    # 推送
    push_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    status, resp_text = http_post(push_url, headers=headers, data=record_data)
    
    if status == 200:
        return True, "成功"
    else:
        return False, f"{status}: {resp_text[:200]}"

def main():
    """主入口"""
    import sys
    
    # 从命令行获取消息
    message = sys.argv[1] if len(sys.argv) > 1 else ""
    
    print(f"收到消息: {message[:100]}")
    
    # 提取链接
    urls = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', message)
    if not urls:
        print("没有找到链接")
        return
    
    url = urls[0]
    print(f"处理链接: {url}")
    
    # 获取网页
    web_data = fetch_webpage(url)
    if not web_data["success"]:
        print(f"获取网页失败: {web_data['error']}")
        return
    
    title = web_data["title"]
    content = web_data["content"]
    
    print(f"网页标题: {title[:50]}")
    
    # 存档
    archive = archive_webpage(url, "temp")
    if archive["success"]:
        print(f"✓ 网页存档: {archive['path']}")
    
    # Kimi提取
    print("调用Kimi提取...")
    extracted = extract_with_kimi(url, title, content)
    
    print(f"提取结果:")
    print(f"  项目名称: {extracted.get('项目名称')}")
    print(f"  规模: {extracted.get('近期规模')}")
    print(f"  投资: {extracted.get('工程总投资')}")
    print(f"  来源: {extracted.get('_source')}")
    
    # 推送到飞书表格
    print("推送到飞书表格...")
    success, msg = push_to_feishu(extracted, url)
    
    if success:
        print("✓ 推送成功")
    else:
        print(f"✗ 推送失败: {msg}")

if __name__ == "__main__":
    main()
