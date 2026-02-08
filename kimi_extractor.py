"""
Kimi API 提取器 - 从网页内容提取56个字段
"""

import os
import json
import requests
import re
from datetime import datetime


class KimiExtractor:
    """使用 Moonshot Kimi API 提取结构化信息"""
    
    def __init__(self):
        self.api_key = os.environ.get('KIMI_API_KEY')
        if not self.api_key:
            raise ValueError("缺少 KIMI_API_KEY 环境变量")
        
        self.api_url = "https://api.moonshot.cn/v1/chat/completions"
        self.model = "moonshot-v1-8k"  # 或 moonshot-v1-32k 用于长文
    
    def extract(self, url, title="", content=""):
        """
        提取项目信息
        返回: (extracted_data, raw_response)
        """
        # 清理内容，去除多余空白
        content = re.sub(r'\n+', '\n', content)
        content = re.sub(r'\s+', ' ', content)
        
        prompt = self._build_prompt(url, title, content[:6000])  # 限制长度
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "你是专业的环保工程信息提取助手，擅长从新闻、招标公告中提取地下式污水处理厂的详细信息。请严格按照要求的JSON格式返回，没有的信息填null。"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(self.api_url, headers=headers, json=data, timeout=60)
            result = resp.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                raw_content = result['choices'][0]['message']['content']
                extracted = json.loads(raw_content)
                
                # 添加元数据
                extracted['_url'] = url
                extracted['_extract_time'] = datetime.now().isoformat()
                extracted['_api_usage'] = result.get('usage', {})
                
                # 计算完整度
                extracted['_completeness'] = self._calculate_completeness(extracted)
                
                return extracted, raw_content
            else:
                error_msg = result.get('error', {}).get('message', '未知错误')
                print(f"Kimi API 返回错误: {error_msg}")
                return self._fallback(url, title, error_msg), None
                
        except json.JSONDecodeError as e:
            print(f"Kimi 返回非JSON格式: {e}")
            return self._fallback(url, title, "解析失败"), None
        except Exception as e:
            print(f"Kimi API 调用异常: {e}")
            return self._fallback(url, title, str(e)), None
    
    def _build_prompt(self, url, title, content):
        """构建提取提示词"""
        return f"""请从以下地下式污水处理厂相关新闻中提取结构化信息。

【来源信息】
URL: {url}
标题: {title}

【正文内容】
{content}

【提取要求】
请提取以下56个字段，返回标准JSON格式。没有的信息填 null 或空字符串。

【字段列表】

1. 基础信息类：
- 项目名称（标准全称，去掉修饰词）
- 近期规模（万吨/日，纯数字）
- 远期总规模（万吨/日，纯数字）
- 箱体占地面积（平方米，纯数字）
- 厂区占地面积（平方米，纯数字）
- 工程总投资（亿元，纯数字）
- 运行时间（格式：YYYY-MM 或 YYYY-MM-DD）
- 投资方/总包方（公司全称）
- 设计方（公司全称）
- 施工方（公司全称）
- 运营方（公司全称）

2. 水质设计类：
- 执行标准（如：一级A、地表IV类、DB32/1072-2018）
- 设计进水COD（mg/L，数字）
- 设计进水BOD（mg/L，数字）
- 设计进水氨氮（mg/L，数字）
- 设计进水总氮（mg/L，数字）
- 设计进水总磷（mg/L，数字）
- 设计进水SS（mg/L，数字）
- 设计出水COD（mg/L，数字）
- 设计出水BOD（mg/L，数字）
- 设计出水氨氮（mg/L，数字）
- 设计出水总氮（mg/L，数字）
- 设计出水总磷（mg/L，数字）
- 设计出水SS（mg/L，数字）

3. 工艺参数类：
- 水处理流程（如：预处理+AAO+MBR+消毒）
- 生化池HRT（小时，数字）
- 设计污泥浓度（mg/L，数字）
- BOD污泥负荷（kgBOD5/(kgMLSS·d)，数字）
- 生化气水比（如：8:1）
- 是否添加填料（是/否）
- 填料填充比（%，数字）
- 外碳源投加药剂种类（如：乙酸钠、葡萄糖）
- 外碳源投加量（mg/L，数字）
- 二沉池表面负荷（m³/(㎡·h)，数字）
- 高效沉淀池沉淀区表面负荷（m³/(㎡·h)，数字）
- 絮凝剂投加种类（如：PAC、PAM）
- 絮凝剂投加量（mg/L，数字）

4. 除臭通风类：
- 臭气处理工艺（如：生物滤池+活性炭吸附）
- 执行标准_厂界有组织（如：厂界满足《恶臭污染物排放标准》二级标准）
- 收集风管材质（如：玻璃钢、不锈钢）
- 总风量（万m³/h，数字）
- 除臭塔停留时间（秒，数字）
- 生物除臭工艺选择（如：生物滴滤、生物过滤）
- 生物除臭填料选择（如：火山岩、陶粒）
- 通风设计（文字描述）
- 换气次数（次/小时，数字）
- 通风风机总气量（万m³/h，数字）

5. 污泥处理类：
- 污泥处理工艺（如：离心脱水、板框压滤）
- 出厂污泥含水率（%，数字）
- 产泥系数（tDS/万m³，数字）
- 药剂选择（如：PAM、石灰）
- 药剂投加量（kg/tDS，数字）

6. 建设运营类：
- 施工总时长（月，数字）
- 土建时长_至封顶结束（月，数字）
- 安装时长（月，数字）
- 分期模式（如：分两期建设，近期15万吨/日，远期30万吨/日）
- 地面开发模式（如：公园、商业、停车场）
- 绿色能源利用情况（如：光伏发电、余热利用）

【返回格式】
{{
    "项目名称": "嘉兴市秀洲区花园式地下污水处理厂",
    "近期规模": 15,
    "远期总规模": 30,
    "工程总投资": 12.5,
    "地理位置": "浙江嘉兴",
    "水处理流程": "预处理+AAO+MBR+消毒",
    "执行标准": "DB32/1072-2018",
    ...
    "_completeness": "35%",
    "_summary": "该项目位于浙江嘉兴，近期规模15万吨/日，采用AAO+MBR工艺..."
}}

注意：
1. 只返回JSON，不要其他文字
2. 数字字段只返回数字，不要单位
3. 没有的信息明确填 null
4. _completeness 和 _summary 是必填的元数据
"""
    
    def _calculate_completeness(self, data):
        """计算信息完整度"""
        # 核心字段（权重高）
        core_fields = [
            '项目名称', '近期规模', '工程总投资', '地理位置',
            '水处理流程', '投资方/总包方'
        ]
        
        # 所有字段
        all_fields = [
            '项目名称', '近期规模', '远期总规模', '箱体占地面积',
            '厂区占地面积', '工程总投资', '运行时间', '投资方/总包方',
            '设计方', '施工方', '运营方', '执行标准',
            '水处理流程', '地理位置'
        ]
        
        core_filled = sum(1 for f in core_fields if data.get(f))
        all_filled = sum(1 for f in all_fields if data.get(f))
        
        core_pct = (core_filled / len(core_fields)) * 100
        all_pct = (all_filled / len(all_fields)) * 100
        
        # 加权：核心占60%，整体占40%
        final = core_pct * 0.6 + all_pct * 0.4
        
        return f"{final:.0f}%"
    
    def _fallback(self, url, title, error):
        """降级方案"""
        return {
            "项目名称": title or "未识别项目",
            "数据来源": "用户提交",
            "_url": url,
            "_extract_time": datetime.now().isoformat(),
            "_completeness": "5%",
            "_summary": f"Kimi提取失败: {error}，需要人工补充",
            "_error": error,
            "_fallback": True
        }


if __name__ == "__main__":
    # 测试
    extractor = KimiExtractor()
    result, raw = extractor.extract(
        "https://example.com/news",
        "嘉兴秀洲首座花园式地下污水处理厂开工",
        "该项目位于嘉兴市秀洲区，近期规模15万吨/日，远期总规模30万吨/日，工程总投资约12.5亿元，采用AAO+MBR工艺..."
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
