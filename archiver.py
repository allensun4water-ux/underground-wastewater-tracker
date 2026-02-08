"""
网页存档模块 - 保存原始HTML，防止链接失效
"""

import os
import requests
import hashlib
from datetime import datetime
from urllib.parse import urlparse


class WebArchiver:
    """网页存档器"""
    
    def __init__(self, archive_dir="web_archives"):
        self.archive_dir = archive_dir
        os.makedirs(archive_dir, exist_ok=True)
    
    def archive(self, url, project_id):
        """
        存档网页
        返回: archive_info 字典
        """
        if not url or not url.startswith('http'):
            return None
        
        # 生成存档文件名
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_project_id = re.sub(r'[^\w]', '_', str(project_id))[:20]
        
        filename_base = f"{safe_project_id}_{timestamp}_{url_hash}"
        
        try:
            # 下载网页
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            resp = requests.get(url, headers=headers, timeout=20)
            resp.encoding = 'utf-8'
            
            # 保存原始HTML
            html_path = os.path.join(self.archive_dir, f"{filename_base}.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(resp.text)
            
            # 保存元数据
            meta = {
                'url': url,
                'project_id': project_id,
                'archive_time': datetime.now().isoformat(),
                'content_type': resp.headers.get('Content-Type', 'unknown'),
                'content_length': len(resp.text),
                'status_code': resp.status_code,
                'html_file': f"{filename_base}.html"
            }
            
            meta_path = os.path.join(self.archive_dir, f"{filename_base}.json")
            with open(meta_path, 'w', encoding='utf-8') as f:
                import json
                json.dump(meta, f, ensure_ascii=False, indent=2)
            
            print(f"✓ 网页存档成功: {html_path}")
            
            return {
                'success': True,
                'html_path': html_path,
                'meta_path': meta_path,
                'filename': filename_base,
                'archive_time': meta['archive_time'],
                'size_kb': len(resp.text) / 1024
            }
            
        except Exception as e:
            print(f"✗ 网页存档失败 {url}: {e}")
            return {
                'success': False,
                'error': str(e),
                'url': url
            }
    
    def get_archive_url(self, filename):
        """
        获取存档文件的访问链接
        实际项目中可以上传到云存储，返回公网URL
        """
        # 这里返回本地路径，后续可改为云存储URL
        return f"file://{os.path.abspath(self.archive_dir)}/{filename}.html"
    
    def upload_to_github(self, filename):
        """
        将存档提交到GitHub（作为备份）
        需要在GitHub Actions中调用
        """
        # 实现：git add && git commit && git push
        pass


if __name__ == "__main__":
    # 测试
    archiver = WebArchiver()
    result = archiver.archive(
        "https://www.h2o-china.com/news/xxxxx",
        "proj_abc123"
    )
    print(result)
