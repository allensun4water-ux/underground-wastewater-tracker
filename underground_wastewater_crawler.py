# underground_wastewater_crawler.py
# 地下式污水处理厂信息采集爬虫
# 支持：中国水网、E20环境平台、北极星环保网

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import hashlib

class BaseCrawler:
    """基础爬虫类，统一输出格式"""
    
    def __init__(self, source_name):
        self.source_name = source_name
        self.results = []
    
    def extract_underground_features(self, text):
        """识别地下厂特征关键词"""
        underground_keywords = [
            '地下式', '全地下', '地埋式', '下沉式', '地下污水', 
            '地下厂', '箱体', '地下空间', '覆土', '地下一层', '地下二层'
        ]
        return any(kw in text for kw in underground_keywords)
    
    def parse_scale(self, text):
        """提取处理规模（万吨/日）"""
        patterns = [
            r'(\d+\.?\d*)\s*万\s*吨[/\\/]日',
            r'(\d+\.?\d*)\s*万\s*m?³?[/\\/]d',
            r'处理规模\D*(\d+\.?\d*)\s*万',
            r'(\d+\.?\d*)\s*万\s*t/d',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return float(match.group(1))
        return None
    
    def parse_investment(self, text):
        """提取投资额（亿元）"""
        patterns = [
            r'(\d+\.?\d*)\s*亿\s*元',
            r'投资\D*(\d+\.?\d*)\s*亿',
            r'总投资\D*(\d+\.?\d*)',
            r'(\d+\.?\d*)\s*亿',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                val = float(match.group(1))
                if val > 0.1 and val < 500:  # 合理范围过滤
                    return val
        return None
    
    def parse_location(self, text):
        """提取地理位置"""
        # 省市区匹配
        prov_pattern = r'(北京|天津|上海|重庆|河北|山西|辽宁|吉林|黑龙江|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海|台湾|内蒙古|广西|西藏|宁夏|新疆|香港|澳门)(?:省|市|自治区)?'
        city_pattern = r'(石家庄|太原|呼和浩特|沈阳|长春|哈尔滨|南京|杭州|合肥|福州|南昌|济南|郑州|武汉|长沙|广州|南宁|海口|成都|贵阳|昆明|拉萨|西安|兰州|西宁|银川|乌鲁木齐|大连|青岛|宁波|厦门|深圳|苏州|无锡|佛山|东莞|常州|徐州|南通|温州|绍兴|嘉兴|烟台|威海|泉州|珠海|中山|惠州|金华|台州|盐城|扬州|镇江|泰州|唐山|保定|邯郸|张家口|承德|沧州|廊坊|衡水|大同|阳泉|长治|晋城|朔州|晋中|运城|忻州|临汾|吕梁|包头|乌海|赤峰|通辽|鄂尔多斯|呼伦贝尔|巴彦淖尔|乌兰察布|兴安盟|锡林郭勒盟|阿拉善盟|鞍山|抚顺|本溪|丹东|锦州|营口|阜新|辽阳|盘锦|铁岭|朝阳|葫芦岛|吉林|四平|辽源|通化|白山|松原|白城|延边朝鲜族自治州|齐齐哈尔|鸡西|鹤岗|双鸭山|大庆|伊春|佳木斯|七台河|牡丹江|黑河|绥化|大兴安岭地区|南京|无锡|徐州|常州|苏州|南通|连云港|淮安|盐城|扬州|镇江|泰州|宿迁|杭州|宁波|温州|嘉兴|湖州|绍兴|金华|衢州|舟山|台州|丽水|合肥|芜湖|蚌埠|淮南|马鞍山|淮北|铜陵|安庆|黄山|滁州|阜阳|宿州|六安|亳州|池州|宣城|福州|厦门|莆田|三明|泉州|漳州|南平|龙岩|宁德|南昌|景德镇|萍乡|九江|新余|鹰潭|赣州|吉安|宜春|抚州|上饶|济南|青岛|淄博|枣庄|东营|烟台|潍坊|济宁|泰安|威海|日照|莱芜|临沂|德州|聊城|滨州|菏泽|郑州|开封|洛阳|平顶山|安阳|鹤壁|新乡|焦作|濮阳|许昌|漯河|三门峡|南阳|商丘|信阳|周口|驻马店|武汉|黄石|十堰|宜昌|襄阳|鄂州|荆门|孝感|荆州|黄冈|咸宁|随州|恩施土家族苗族自治州|长沙|株洲|湘潭|衡阳|邵阳|岳阳|常德|张家界|益阳|郴州|永州|怀化|娄底|湘西土家族苗族自治州|广州|韶关|深圳|珠海|汕头|佛山|江门|湛江|茂名|肇庆|惠州|梅州|汕尾|河源|阳江|清远|东莞|中山|潮州|揭阳|云浮|南宁|柳州|桂林|梧州|北海|防城港|钦州|贵港|玉林|百色|贺州|河池|来宾|崇左|海口|三亚|三沙|儋州|成都|自贡|攀枝花|泸州|德阳|绵阳|广元|遂宁|内江|乐山|南充|眉山|宜宾|广安|达州|雅安|巴中|资阳|阿坝藏族羌族自治州|甘孜藏族自治州|凉山彝族自治州|贵阳|六盘水|遵义|安顺|毕节|铜仁|黔西南布依族苗族自治州|黔东南苗族侗族自治州|黔南布依族苗族自治州|昆明|曲靖|玉溪|保山|昭通|丽江|普洱|临沧|楚雄彝族自治州|红河哈尼族彝族自治州|文山壮族苗族自治州|西双版纳傣族自治州|大理白族自治州|德宏傣族景颇族自治州|怒江傈僳族自治州|迪庆藏族自治州|拉萨|日喀则|昌都|林芝|山南|那曲|阿里地区|西安|铜川|宝鸡|咸阳|渭南|延安|汉中|榆林|安康|商洛|兰州|嘉峪关|金昌|白银|天水|武威|张掖|平凉|酒泉|庆阳|定西|陇南|临夏回族自治州|甘南藏族自治州|西宁|海东市|海北藏族自治州|黄南藏族自治州|海南藏族自治州|果洛藏族自治州|玉树藏族自治州|海西蒙古族藏族自治州|银川|石嘴山|吴忠|固原|中卫|乌鲁木齐|克拉玛依|吐鲁番|哈密|昌吉回族自治州|博尔塔拉蒙古自治州|巴音郭楞蒙古自治州|阿克苏地区|克孜勒苏柯尔克孜自治州|喀什地区|和田地区|伊犁哈萨克自治州|塔城地区|阿勒泰地区|石河子|阿拉尔|图木舒克|五家渠|北屯|铁门关|双河|可克达拉|昆玉|胡杨河|新星)'
        
        prov_match = re.search(prov_pattern, text)
        city_match = re.search(city_pattern, text)
        
        location = ''
        if prov_match:
            location = prov_match.group(1)
        if city_match:
            location += city_match.group(1) if not location else '·' + city_match.group(1)
        return location if location else None
    
    def standardize_output(self, raw_data):
        """统一输出格式（对应56字段模板的核心字段）"""
        return {
            # 基础信息
            '项目名称': raw_data.get('title', ''),
            '近期规模': raw_data.get('scale', ''),
            '远期总规模': '',  # 需从正文中解析"一期/二期"
            '箱体占地面积': '',
            '厂区占地面积': '',
            '工程总投资': raw_data.get('investment', ''),
            '运行时间': raw_data.get('publish_time', ''),
            '投资方/总包方': raw_data.get('company', ''),
            '设计方': '',
            '施工方': '',
            
            # 来源追踪（关键）
            '数据来源': self.source_name,
            '来源URL': raw_data.get('url', ''),
            '抓取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '原始标题': raw_data.get('title', ''),
            '原文摘要': raw_data.get('summary', '')[:500],  # 前500字
            
            # 置信度标记
            '数据置信度': '中',  # 高/中/低，需人工确认
            '处理状态': '待清洗',
            
            # 扩展字段（占位，后续Kimi处理填充）
            '执行标准': '',
            '设计进水COD': '',
            '设计出水COD': '',
            '水处理流程': '',
            '臭气处理工艺': '',
            '污泥处理工艺': '',
            '施工总时长': '',
            '地面开发模式': '',
        }

class H2OChinaCrawler(BaseCrawler):
    """中国水网爬虫"""
    
    def __init__(self):
        super().__init__('中国水网')
        self.base_url = 'https://www.h2o-china.com'
        self.search_url = 'https://www.h2o-china.com/news/search?keyword=地下式污水'
        
    def fetch_list(self, page=1):
        """获取列表页"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            url = f'{self.search_url}&page={page}'
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            items = []
            # 根据实际页面结构调整选择器
            news_list = soup.select('.news-list li') or soup.select('.list-item') or soup.find_all('div', class_=re.compile('item|list'))
            
            for item in news_list:
                try:
                    title_tag = item.select_one('a') or item.find('a')
                    if not title_tag:
                        continue
                        
                    title = title_tag.get_text(strip=True)
                    link = title_tag.get('href', '')
                    if link and not link.startswith('http'):
                        link = self.base_url + link
                    
                    # 过滤地下厂相关
                    if not self.extract_underground_features(title):
                        continue
                    
                    # 获取摘要
                    summary_tag = item.select_one('.summary') or item.select_one('.content') or item.find('p')
                    summary = summary_tag.get_text(strip=True) if summary_tag else ''
                    
                    # 获取时间
                    time_tag = item.select_one('.time') or item.select_one('.date') or item.find('span', string=re.compile(r'\d{4}'))
                    pub_time = time_tag.get_text(strip=True) if time_tag else ''
                    
                    items.append({
                        'title': title,
                        'url': link,
                        'summary': summary,
                        'publish_time': pub_time,
                        'scale': self.parse_scale(title + summary),
                        'investment': self.parse_investment(title + summary),
                        'company': '',  # 详情页再提取
                        'location': self.parse_location(title + summary)
                    })
                except Exception as e:
                    print(f'解析列表项出错: {e}')
                    continue
            
            return items
        except Exception as e:
            print(f'获取列表页失败: {e}')
            return []
    
    def fetch_detail(self, url):
        """获取详情页（提取更完整信息）"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 提取正文
            content_div = soup.select_one('.content-detail') or soup.select_one('.article-content') or soup.find('div', class_=re.compile('content|article'))
            content = content_div.get_text(strip=True) if content_div else ''
            
            # 提取中标单位（常见模式）
            company_patterns = [
                r'中标单位[：:]\s*([^\n,，]+)',
                r'中标人[：:]\s*([^\n,，]+)',
                r'中标供应商[：:]\s*([^\n,，]+)',
                r'中标方[：:]\s*([^\n,，]+)',
                r'中标结果[：:]\s*([^\n,，]+)',
            ]
            company = ''
            for pattern in company_patterns:
                match = re.search(pattern, content)
                if match:
                    company = match.group(1).strip()
                    break
            
            return {
                'full_content': content[:2000],  # 前2000字
                'company': company,
                'scale': self.parse_scale(content),
                'investment': self.parse_investment(content),
                'location': self.parse_location(content) or self.parse_location(content)
            }
        except Exception as e:
            print(f'获取详情页失败 {url}: {e}')
            return {}

class E20Crawler(BaseCrawler):
    """E20环境平台爬虫"""
    
    def __init__(self):
        super().__init__('E20环境平台')
        self.base_url = 'https://www.e20.com.cn'
        self.search_url = 'https://www.e20.com.cn/search?keyword=地下式污水'
        
    def fetch_list(self, page=1):
        """E20标讯采集"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            # E20可能需要登录或有反爬，先尝试公开页面
            url = f'{self.search_url}&page={page}' if page > 1 else self.search_url
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            items = []
            # E20页面结构（需根据实际调整）
            news_list = soup.select('.news-item') or soup.select('.list-box') or soup.find_all('div', class_=re.compile('item'))
            
            for item in news_list:
                try:
                    title_tag = item.select_one('h3 a') or item.select_one('a')
                    if not title_tag:
                        continue
                    
                    title = title_tag.get_text(strip=True)
                    link = title_tag.get('href', '')
                    if link and not link.startswith('http'):
                        link = self.base_url + link
                    
                    if not self.extract_underground_features(title):
                        continue
                    
                    summary_tag = item.select_one('.intro') or item.select_one('p')
                    summary = summary_tag.get_text(strip=True) if summary_tag else ''
                    
                    time_tag = item.select_one('.time') or item.find('span', string=re.compile(r'\d{4}-\d{2}'))
                    pub_time = time_tag.get_text(strip=True) if time_tag else ''
                    
                    items.append({
                        'title': title,
                        'url': link,
                        'summary': summary,
                        'publish_time': pub_time,
                        'scale': self.parse_scale(title + summary),
                        'investment': self.parse_investment(title + summary),
                        'company': '',
                        'location': self.parse_location(title + summary)
                    })
                except Exception as e:
                    print(f'解析E20列表项出错: {e}')
                    continue
            
            return items
        except Exception as e:
            print(f'获取E20列表失败: {e}')
            return []

class BjXCrawler(BaseCrawler):
    """北极星环保网爬虫"""
    
    def __init__(self):
        super().__init__('北极星环保网')
        self.base_url = 'https://huanbao.bjx.com.cn'
        self.search_url = 'https://huanbao.bjx.com.cn/Search?keyword=地下式污水'
        
    def fetch_list(self, page=1):
        """北极星环保网"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            url = f'{self.search_url}&page={page}' if page > 1 else self.search_url
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            items = []
            # 北极星页面结构
            news_list = soup.select('.list_detail') or soup.select('.list-item') or soup.find_all('dl', class_=re.compile('list'))
            
            for item in news_list:
                try:
                    title_tag = item.select_one('h3 a') or item.select_one('dt a') or item.select_one('a')
                    if not title_tag:
                        continue
                    
                    title = title_tag.get_text(strip=True)
                    link = title_tag.get('href', '')
                    if link and not link.startswith('http'):
                        link = self.base_url + link
                    
                    if not self.extract_underground_features(title):
                        continue
                    
                    # 北极星摘要在dd标签
                    summary_tag = item.select_one('dd') or item.select_one('.summary') or item.find('p')
                    summary = summary_tag.get_text(strip=True) if summary_tag else ''
                    
                    # 时间
                    time_tag = item.select_one('.time') or item.find('span', string=re.compile(r'\d{4}'))
                    pub_time = time_tag.get_text(strip=True) if time_tag else ''
                    
                    items.append({
                        'title': title,
                        'url': link,
                        'summary': summary,
                        'publish_time': pub_time,
                        'scale': self.parse_scale(title + summary),
                        'investment': self.parse_investment(title + summary),
                        'company': '',
                        'location': self.parse_location(title + summary)
                    })
                except Exception as e:
                    print(f'解析北极星列表项出错: {e}')
                    continue
            
            return items
        except Exception as e:
            print(f'获取北极星列表失败: {e}')
            return []

def run_all_crawlers(pages=2):
    """运行所有爬虫"""
    all_results = []
    
    crawlers = [
        H2OChinaCrawler(),
        E20Crawler(),
        BjXCrawler()
    ]
    
    for crawler in crawlers:
        print(f'\n=== 开始抓取: {crawler.source_name} ===')
        for page in range(1, pages + 1):
            print(f'  正在获取第{page}页...')
            items = crawler.fetch_list(page)
            if not items:
                break
            
            # 获取详情页补充信息（可选，会慢一些）
            for item in items:
                if item.get('url'):
                    # 可以在这里调用fetch_detail，但会大幅增加时间
                    # detail = crawler.fetch_detail(item['url'])
                    # item.update(detail)
                    pass
            
            # 标准化输出
            for item in items:
                std_item = crawler.standardize_output(item)
                all_results.append(std_item)
                print(f'    ✓ {std_item["项目名称"][:30]}... [{std_item["数据来源"]}]')
        
        print(f'  {crawler.source_name} 完成，本站点共{len([r for r in all_results if r["数据来源"]==crawler.source_name])}条')
    
    # 去重（基于URL）
    seen_urls = set()
    unique_results = []
    for r in all_results:
        if r['来源URL'] not in seen_urls:
            seen_urls.add(r['来源URL'])
            unique_results.append(r)
    
    print(f'\n=== 总计: {len(unique_results)}条不重复数据 ===')
    return unique_results

def save_to_json(data, filename='underground_wastewater_data.json'):
    """保存为JSON"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f'数据已保存: {filename}')

def save_to_csv(data, filename='underground_wastewater_data.csv'):
    """保存为CSV（方便导入飞书）"""
    import csv
    
    if not data:
        return
    
    # 获取所有字段
    fields = list(data[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)
    print(f'数据已保存: {filename}')

if __name__ == '__main__':
    # 运行爬虫（默认每站抓2页）
    results = run_all_crawlers(pages=2)
    
    # 保存数据
    save_to_json(results)
    save_to_csv(results)
    
    # 打印样本
    if results:
        print('\n=== 数据样本 ===')
        print(json.dumps(results[0], ensure_ascii=False, indent=2))
