"""
项目实体识别与匹配引擎
解决多源信息合并、去重、冲突检测
"""

import hashlib
import re
from difflib import SequenceMatcher
from datetime import datetime


class ProjectMatcher:
    """项目实体识别与合并"""
    
    def __init__(self):
        self.match_threshold = 0.80  # 相似度阈值
        self.location_pattern = re.compile(
            r'(北京|天津|上海|重庆|河北|山西|辽宁|吉林|黑龙江|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海|内蒙古|广西|西藏|宁夏|新疆|香港|澳门|台湾)'
        )
    
    def normalize_name(self, name):
        """
        标准化项目名称，提取核心特征
        """
        if not name:
            return ""
        
        # 转为小写，统一处理
        name = name.lower()
        
        # 去掉常见修饰词
        modifiers = [
            '首座', '首台', '首个', '第一', '最大', '最新',
            '花园式', '智慧', '生态', '绿色', '环保', '智能',
            '全地下', '地埋式', '地下式', '半地下',
            '污水', '净水', '水处理', '再生水',
            '厂', '项目', '工程', '一期', '二期', '改扩建'
        ]
        
        for mod in modifiers:
            name = name.replace(mod, '')
        
        # 去掉标点、空格
        name = re.sub(r'[^\w]', '', name)
        
        return name.strip()
    
    def extract_location(self, text):
        """从文本提取地理位置"""
        if not text:
            return ""
        
        # 匹配省份
        prov_match = self.location_pattern.search(text)
        if prov_match:
            province = prov_match.group(1)
            
            # 尝试匹配城市（常见城市）
            city_pattern = r'(杭州|宁波|温州|嘉兴|湖州|绍兴|金华|衢州|舟山|台州|丽水|南京|苏州|无锡|常州|徐州|南通|连云港|淮安|盐城|扬州|镇江|泰州|宿迁|广州|深圳|珠海|汕头|佛山|韶关|湛江|肇庆|江门|茂名|惠州|梅州|汕尾|河源|阳江|清远|东莞|中山|潮州|揭阳|云浮)'
            city_match = re.search(city_pattern, text)
            
            if city_match:
                return f"{province}·{city_match.group(1)}"
            return province
        
        return ""
    
    def generate_fingerprint(self, project_data):
        """
        生成项目指纹（唯一标识）
        组合：地理位置 + 标准化名称 + 规模
        """
        location = project_data.get('地理位置', '') or self.extract_location(project_data.get('项目名称', ''))
        name = self.normalize_name(project_data.get('项目名称', ''))
        scale = str(project_data.get('近期规模', ''))
        
        # 如果名称太短，用原始名称
        if len(name) < 4:
            name = project_data.get('项目名称', '')[:10]
        
        fingerprint = f"{location}_{name}_{scale}"
        
        # 生成短ID
        return hashlib.md5(fingerprint.encode()).hexdigest()[:12]
    
    def calculate_similarity(self, proj1, proj2):
        """
        计算两个项目的相似度（0-1）
        """
        scores = []
        
        # 1. 名称相似度（权重35%）
        name1 = self.normalize_name(proj1.get('项目名称', ''))
        name2 = self.normalize_name(proj2.get('项目名称', ''))
        if name1 and name2:
            name_sim = SequenceMatcher(None, name1, name2).ratio()
            scores.append(name_sim * 0.35)
        
        # 2. 地理位置（权重25%）
        loc1 = proj1.get('地理位置', '') or self.extract_location(proj1.get('项目名称', ''))
        loc2 = proj2.get('地理位置', '') or self.extract_location(proj2.get('项目名称', ''))
        if loc1 and loc2:
            if loc1 == loc2:
                scores.append(0.25)
            elif loc1.split('·')[0] == loc2.split('·')[0]:  # 同省不同市
                scores.append(0.15)
        
        # 3. 规模（权重20%）
        scale1 = proj1.get('近期规模')
        scale2 = proj2.get('近期规模')
        if scale1 and scale2:
            try:
                s1, s2 = float(scale1), float(scale2)
                if max(s1, s2) > 0:
                    scale_sim = 1.0 - abs(s1 - s2) / max(s1, s2)
                    scores.append(max(0, scale_sim) * 0.20)
            except:
                pass
        
        # 4. 投资（权重10%）
        inv1 = proj1.get('工程总投资')
        inv2 = proj2.get('工程总投资')
        if inv1 and inv2:
            try:
                i1, i2 = float(inv1), float(inv2)
                if max(i1, i2) > 0:
                    inv_sim = 1.0 - abs(i1 - i2) / max(i1, i2)
                    scores.append(max(0, inv_sim) * 0.10)
            except:
                pass
        
        # 5. 工艺（权重10%）
        proc1 = proj1.get('水处理流程', '')
        proc2 = proj2.get('水处理流程', '')
        if proc1 and proc2:
            # 简单判断是否有共同关键词
            keywords1 = set(re.findall(r'[A-Za-z]+', proc1.upper()))
            keywords2 = set(re.findall(r'[A-Za-z]+', proc2.upper()))
            if keywords1 & keywords2:  # 有交集
                scores.append(0.10)
        
        total = sum(scores)
        return min(total, 1.0)  # 最高1.0
    
    def find_match(self, new_project, existing_projects):
        """
        在现有项目中查找最佳匹配
        返回: (matched_project, similarity) 或 (None, 0)
        """
        best_match = None
        best_score = 0
        
        for exist in existing_projects:
            score = self.calculate_similarity(new_project, exist)
            if score > best_score:
                best_score = score
                best_match = exist
        
        if best_score >= self.match_threshold:
            return best_match, best_score
        
        return None, 0
    
    def merge_projects(self, existing, new_data, source_info):
        """
        合并两个来源的项目信息
        策略：补充空字段，标记冲突
        """
        merged = existing.copy()
        conflicts = []
        updates = []
        
        # 遍历新数据的所有字段
        for key, new_val in new_data.items():
            # 跳过元数据字段
            if key.startswith('_'):
                continue
            
            if not new_val:
                continue
            
            old_val = existing.get(key)
            source_key = f"{key}_来源"
            old_source = existing.get(source_key, '未知')
            
            if not old_val:
                # 旧数据为空，直接补充
                merged[key] = new_val
                merged[source_key] = source_info.get('数据来源', '未知')
                updates.append(f"{key}: 补充 ({new_val})")
            
            elif old_val == new_val:
                # 一致，跳过
                continue
            
            else:
                # 冲突！保留旧值，但记录冲突
                conflict_info = {
                    '字段': key,
                    '当前值': old_val,
                    '新值': new_val,
                    '当前来源': old_source,
                    '新来源': source_info.get('数据来源', '未知'),
                    '时间': datetime.now().isoformat()
                }
                conflicts.append(conflict_info)
                
                # 标记冲突
                merged[f"{key}_冲突"] = True
                merged['需要人工确认'] = True
        
        # 更新统计
        merged['信息来源数量'] = existing.get('信息来源数量', 1) + 1
        merged['最后更新时间'] = datetime.now().isoformat()
        
        if updates:
            merged['_更新记录'] = existing.get('_更新记录', []) + [{
                '时间': datetime.now().isoformat(),
                '更新': updates,
                '来源': source_info.get('数据来源', '未知')
            }]
        
        if conflicts:
            merged['_冲突记录'] = existing.get('_冲突记录', []) + conflicts
        
        # 重新计算完整度
        completeness = self._recalculate_completeness(merged)
        merged['信息完整度'] = completeness
        
        return merged, conflicts, updates
    
    def _recalculate_completeness(self, data):
        """重新计算完整度"""
        important_fields = [
            '项目名称', '近期规模', '工程总投资', '地理位置',
            '水处理流程', '投资方/总包方', '设计方', '施工方'
        ]
        
        filled = sum(1 for f in important_fields if data.get(f))
        return f"{(filled / len(important_fields)) * 100:.0f}%"
    
    def create_new_project(self, project_data, source_info):
        """创建新项目记录"""
        project_id = self.generate_fingerprint(project_data)
        
        # 添加元数据
        project_data['项目ID'] = project_id
        project_data['创建时间'] = datetime.now().isoformat()
        project_data['最后更新时间'] = datetime.now().isoformat()
        project_data['信息来源数量'] = 1
        project_data['信息完整度'] = project_data.get('_completeness', '10%')
        project_data['需要人工确认'] = False
        
        # 为每个字段添加来源标记
        fields_to_track = [
            '项目名称', '近期规模', '远期总规模', '工程总投资',
            '地理位置', '水处理流程', '投资方/总包方', '设计方', '施工方'
        ]
        
        for field in fields_to_track:
            if project_data.get(field):
                project_data[f"{field}_来源"] = source_info.get('数据来源', '未知')
        
        return project_data


if __name__ == "__main__":
    # 测试
    matcher = ProjectMatcher()
    
    existing = {
        '项目ID': 'abc123',
        '项目名称': '嘉兴秀洲地下污水处理厂',
        '地理位置': '浙江·嘉兴',
        '近期规模': 15,
        '工程总投资': 12.5,
        '水处理流程': 'AAO+MBR',
        '信息来源数量': 1
    }
    
    new_data = {
        '项目名称': '嘉兴市秀洲区花园式智慧净水厂',
        '地理位置': '浙江嘉兴',
        '近期规模': 15,
        '工程总投资': 12.8,  # 冲突！
        '设计方': '华东勘测设计研究院',
        '水处理流程': 'AAO+MBR+深度处理'
    }
    
    source = {'数据来源': '中国水网'}
    
    score = matcher.calculate_similarity(existing, new_data)
    print(f"相似度: {score:.2f}")
    
    if score >= 0.8:
        merged, conflicts, updates = matcher.merge_projects(existing, new_data, source)
        print(f"合并后: {merged}")
        print(f"冲突: {conflicts}")
