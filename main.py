"""
鱼盆ETF投资量化模型 - 主入口文件
说明:
  本文件是系统唯一入口，负责执行所有策略和处理所有请求
  所有文件放在根目录，简化导入关系

【数据爬取模块说明】
  本文件同时负责从多个数据源获取ETF数据和新股信息
  主数据源：AkShare
  备用数据源：Baostock、新浪财经

【策略详细说明 - 数据获取逻辑】
1. ETF数据获取流程：
   - 主数据源：AkShare（优先尝试，返回最完整数据）
   - 备用数据源1：Baostock（当AkShare失败时尝试）
   - 备用数据源2：新浪财经（当前两个数据源都失败时尝试）
   - 缓存机制：优先检查本地缓存，减少API调用次数

2. 新股信息获取逻辑：
   - 区分两种新股类型：
     * 认购新股（IPO申购）：通过"网上申购日"筛选
     * 上市交易新股：通过"上市日期"筛选
   - 历史数据范围：过去30天（原为7天），确保覆盖最近上市交易的新股
   - 多数据源回退机制：确保即使主数据源失败也能获取数据

3. 数据质量保障：
   - 严格的数据验证：检查DataFrame是否为空
   - 完善的错误处理：记录详细错误日志
   - 数据类型转换：确保数值型字段为正确类型
   - 日期格式标准化：统一使用YYYY-MM-DD格式

4. 性能优化：
   - 缓存机制：减少重复API调用
   - 限流控制：每个API调用间隔1秒
   - 数据过滤：仅获取必要字段

【异常处理机制】
1. 数据源失败：
   - 记录详细错误日志，包含错误类型和消息
   - 自动切换到备用数据源
   - 所有数据源失败时返回空DataFrame

2. 数据质量异常：
   - 空数据处理：返回空DataFrame而非抛出异常
   - 字段缺失处理：使用默认值填充
   - 类型转换错误：记录日志并跳过该条数据

3. 网络问题：
   - 设置15秒超时
   - 重试机制（最多3次）
   - 失败后等待30分钟再重试

【使用说明】
1. 获取ETF数据：get_etf_data(etf_code, data_type='daily')
2. 获取ETF列表：get_all_etf_list()
3. 获取新股信息：get_new_stock_subscriptions()
4. 测试用新股数据获取：get_test_new_stock_subscriptions()
5. 测试用新上市股数据获取：get_test_new_stock_listings()

【数据存储模块说明】
  本文件同时负责数据的存储和清理
  所有文件放在根目录，简化导入关系

【时间处理工具说明】
  本文件同时提供时间相关的工具函数
  所有文件放在根目录，简化导入关系

【企业微信集成说明】
  本文件同时处理企业微信消息推送
  所有文件放在根目录，简化导入关系

【股票池管理模块说明】
  本文件同时负责ETF股票池的更新和管理
  保持10只ETF：5只稳健仓，5只激进仓
  所有文件放在根目录，简化导入关系

【策略计算模块说明】
  本文件负责ETF策略信号的计算和推送
  所有文件放在根目录，简化导入关系

【评分系统核心模块】
  本文件同时包含ETF评分计算功能，为其他模块提供统一的评分接口
  所有文件放在根目录，简化导入关系

【策略详细说明 - 评分体系】
1. 评分维度与权重分配：
   - 流动性评分（20%）：评估ETF交易活跃度，由日均成交额和规模决定
     * 日均成交额（12%）：越高分越高，10亿以上得满分
     * 规模（8%）：越大分越高，500亿以上得满分
     * 目的：确保ETF有足够流动性，避免买卖困难

2. 风险控制评分（25%）：评估ETF风险水平，由波动率和最大回撤决定
   - 年化波动率（15%）：越低分越高，<15%得满分
   - 最大回撤（10%）：越小分越高，<20%得满分
   * 目的：稳健仓特别关注风险控制，避免大幅波动

3. 收益能力评分（25%）：评估ETF历史收益表现
   - 1年收益率（10%）：越高分越高，>10%得满分
   - 3年年化收益率（10%）：越高分越高，>8%得满分
   - 夏普比率（5%）：越高分越高，>1.0得满分
   * 目的：衡量风险调整后收益，避免高风险高收益

4. 溢价率评分（15%）：评估ETF市场价格与净值关系
   - 溢价率（10%）：越接近0%越好，-1%~1%得满分
   - 适度溢价加分（5%）：0.5%~1.5%额外加分
   * 目的：避免在高溢价时买入，防止套利压力

5. 情绪指标评分（15%）：评估市场情绪和成分股质量
   - 龙头股占比（9%）：前5大成分股权重和，越高分越高
   - 行业分散度（6%）：行业数量越多分越高，避免过度集中
   * 目的：捕捉市场情绪和行业轮动机会

【股票池构建逻辑】
1. 筛选过程：
   - 步骤1：获取所有ETF列表（约50只）
   - 步骤2：计算每只ETF的5个维度评分（0-100分）
   - 步骤3：按权重计算总分 = 流动性×0.20 + 风险×0.25 + 收益×0.25 + 溢价×0.15 + 情绪×0.15
   - 步骤4：按总分降序排序，相同分数按风险评分排序（风险越低排名越前）

2. 股票池分配：
   - 稳健仓（5只ETF）：选择风险控制评分 > 75 的前5名
   - 激进仓（5只ETF）：选择风险控制评分 ≤ 75 的前5名

3. 仓位建议：
   - 稳健仓：高评分ETF(85+) 50%，中等评分(70-85) 30%，低评分(<70) 0%
   - 激进仓：高评分ETF(85+) 100%，中等评分(70-85) 50%，低评分(<70) 0%

【评分计算流程】
1. 流动性评分 = (日均成交额评分×0.6 + 规模评分×0.4)
   - 日均成交额评分：min(100, (日均成交额/10)×100)
   - 规模评分：min(100, (规模/500)×100)

2. 风险控制评分 = (波动率评分×0.6 + 最大回撤评分×0.4)
   - 波动率评分：max(0, 100 - 年化波动率×2)
   - 最大回撤评分：max(0, 100 - 最大回撤×2)

3. 收益能力评分 = (1年收益评分×0.3 + 3年收益评分×0.4 + 夏普比率评分×0.3)
   - 1年收益评分：min(100, max(0, 1年收益率×2))
   - 3年收益评分：min(100, max(0, 3年收益率))
   - 夏普比率评分：min(100, max(0, 夏普比率×10))

4. 溢价率评分 = 基础分 + 适度溢价加分
   - 基础分：max(0, 100 - |溢价率|×5)
   - 适度溢价加分：0.5%~1.5%额外+10分，-1%~0%额外+5分

5. 情绪指标评分 = (龙头股占比评分×0.6 + 行业分散度评分×0.4)
   - 龙头股占比评分：min(100, 前5大成分股权重和×150)
   - 行业分散度评分：min(100, 行业数量×10)

【异常处理机制】
1. 数据不足：返回默认中等评分（60分）
2. API调用失败：尝试备用数据源，最多重试3次
3. 无符合标准ETF：使用预设的10只ETF作为备用池
4. 评分异常：设置合理阈值，过滤极端值
"""

import os
import sys
import time
import pandas as pd
import numpy as np
import datetime
import pytz
import shutil
import requests
import json
import akshare as ak
import baostock as bs
from flask import Flask, request, jsonify, has_app_context
from config import Config
from logger import get_logger
from bs4 import BeautifulSoup

app = Flask(__name__)
logger = get_logger(__name__)

# 评分维度权重定义
SCORE_WEIGHTS = {
    'liquidity': 0.20,  # 流动性评分权重
    'risk': 0.25,       # 风险控制评分权重
    'return': 0.25,     # 收益能力评分权重
    'premium': 0.15,    # 溢价率评分权重
    'sentiment': 0.15   # 情绪指标评分权重
}

def get_beijing_time():
    """获取当前北京时间(UTC+8)"""
    beijing_tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(beijing_tz)

def convert_to_beijing_time(dt):
    """
    将datetime对象转换为北京时间(UTC+8)
    参数:
        dt: datetime对象
    返回:
        转换为北京时间的datetime对象
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)  # 若无时区信息，假设为UTC
    beijing_tz = pytz.timezone('Asia/Shanghai')
    return dt.astimezone(beijing_tz)  # 转换为北京时间

def is_trading_day(date=None):
    """
    检查指定日期是否为交易日（中国股市）
    参数:
        date: datetime对象，默认为今天
    返回:
        bool: 是交易日返回True，否则返回False
    """
    if date is None:
        date = get_beijing_time()
    beijing_date = date.date()
    
    # 简单检查：周一至周五且不是已知节假日
    # 实际实现中，应检查节假日日历
    if beijing_date.weekday() < 5:  # 周一=0，周日=6
        return True
    return False

def is_trading_time():
    """
    检查当前时间是否在中国股市交易时段
    返回:
        bool: 是交易时间返回True，否则返回False
        str: 当前交易时段类型
    """
    now = get_beijing_time()
    beijing_time = now.time()
    
    # 集合竞价时段 (9:15-9:25)
    if datetime.time(9, 15) <= beijing_time < datetime.time(9, 25):
        return True, 'pre_market'
    # 早盘交易时段 (9:30-11:30)
    elif datetime.time(9, 30) <= beijing_time < datetime.time(11, 30):
        return True, 'morning'
    # 午盘交易时段 (13:00-15:00)
    elif datetime.time(13, 0) <= beijing_time < datetime.time(15, 0):
        return True, 'afternoon'
    # 收盘后时段
    else:
        return False, 'post_market'

def send_wecom_message(message):
    """
    发送消息到企业微信
    参数:
        message: 消息内容
    返回:
        bool: 是否成功
    """
    # 检查配置
    if not hasattr(Config, 'WECOM_WEBHOOK') or not Config.WECOM_WEBHOOK:
        logger.error("WECOM_WEBHOOK 未设置，无法发送企业微信消息")
        return False
    
    # 在消息结尾添加全局备注
    if hasattr(Config, 'MESSAGE_FOOTER') and Config.MESSAGE_FOOTER:
        message = f"{message}\n\n{Config.MESSAGE_FOOTER}"
    
    try:
        # 构建消息
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        # 发送请求
        response = requests.post(
            Config.WECOM_WEBHOOK,
            json=payload,
            timeout=10
        )
        
        # 检查响应
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                logger.info("企业微信消息发送成功")
                return True
            else:
                logger.error(f"企业微信API返回错误: {result}")
                return False
        else:
            logger.error(f"企业微信请求失败，状态码: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"发送企业微信消息时出错: {str(e)}")
        return False

def calculate_liquidity_score(etf_code, etf_data):
    """
    计算流动性评分（20%权重）
    参数:
        etf_code: ETF代码
        etf_ ETF日线数据
    返回:
        float: 流动性评分（0-100分）
    """
    # 计算最近30天日均成交额（单位：亿元）
    avg_volume = etf_data['volume'].tail(30).mean() / 1000000000
    
    # 规模（假设为5，实际应从ETF基本信息获取）
    scale = 5
    
    # 标准化处理（假设最大成交额为10亿，最大规模为1000亿）
    volume_score = min(100, (avg_volume / 10) * 100)  # 成交额越高分越高
    scale_score = min(100, (scale / 10) * 100)  # 规模越大分越高
    
    # 综合流动性评分
    return (volume_score * 0.6 + scale_score * 0.4)

def calculate_risk_score(etf_code, etf_data):
    """
    计算风险控制评分（25%权重）
    参数:
        etf_code: ETF代码
        etf_ ETF日线数据
    返回:
        float: 风险评分（0-100分）
    """
    # 计算年化波动率（假设日收益率）
    daily_returns = etf_data['close'].pct_change().dropna()
    if len(daily_returns) < 30:
        return 70  # 数据不足，给中等评分
    
    annual_volatility = daily_returns.std() * np.sqrt(252) * 100
    
    # 计算最大回撤
    cum_returns = (1 + daily_returns).cumprod()
    drawdown = 1 - cum_returns / cum_returns.cummax()
    max_drawdown = drawdown.max() * 100
    
    # 波动率评分（波动率越低分越高）
    volatility_score = max(0, 100 - annual_volatility * 2)
    
    # 最大回撤评分（回撤越小分越高）
    drawdown_score = max(0, 100 - max_drawdown * 2)
    
    # 综合风险评分
    return (volatility_score * 0.6 + drawdown_score * 0.4)

def calculate_return_score(etf_code, etf_data):
    """
    计算收益能力评分（25%权重）
    参数:
        etf_code: ETF代码
        etf_ ETF日线数据
    返回:
        float: 收益评分（0-100分）
    """
    # 计算1年收益率
    if len(etf_data) >= 252:
        one_year_return = (etf_data['close'].iloc[-1] / etf_data['close'].iloc[-252] - 1) * 100
    else:
        one_year_return = 0
    
    # 计算3年收益率（简化处理）
    if len(etf_data) >= 756:
        three_year_return = (etf_data['close'].iloc[-1] / etf_data['close'].iloc[-756] - 1) * 100
    else:
        three_year_return = one_year_return * 3 if one_year_return > 0 else one_year_return
    
    # 计算夏普比率
    daily_returns = etf_data['close'].pct_change().dropna()
    if len(daily_returns) > 0:
        sharpe_ratio = daily_returns.mean() / daily_returns.std() * np.sqrt(252)
    else:
        sharpe_ratio = 0
    
    # 收益评分
    one_year_score = min(100, max(0, one_year_return * 2))
    three_year_score = min(100, max(0, three_year_return))
    sharpe_score = min(100, max(0, sharpe_ratio * 10))
    
    # 综合收益评分
    return (one_year_score * 0.3 + three_year_score * 0.4 + sharpe_score * 0.3)

def calculate_premium_score(etf_code):
    """
    计算溢价率评分（15%权重）
    参数:
        etf_code: ETF代码
    返回:
        float: 溢价率评分（0-100分）
    """
    # 获取溢价率
    premium_rate = calculate_premium_rate(etf_code)
    
    # 溢价率评分（0%溢价得100分，每偏离1%扣5分）
    deviation = abs(premium_rate)
    score = max(0, 100 - deviation * 5)
    
    # 适度溢价（0.5%-1.5%）可以额外加分
    if 0.5 <= premium_rate <= 1.5:
        score = min(100, score + 10)
    
    # 折价情况（-1%到0%）也可以接受
    if -1 <= premium_rate < 0:
        score = min(100, score + 5)
    
    return score

def calculate_sentiment_score(etf_code):
    """
    计算情绪指标评分（15%权重）
    参数:
        etf_code: ETF代码
    返回:
        float: 情绪指标评分（0-100分）
    """
    # 获取成分股权重
    weights = calculate_component_weights(etf_code)
    
    if not weights:
        return 60  # 默认中等评分
    
    # 计算龙头股占比（前5大成分股权重和）
    top5_weight = sum(sorted(weights.values(), reverse=True)[:5])
    
    # 行业分散度（简化处理）
    industry_diversity = min(10, len(weights) // 5)  # 每5只股票算一个行业
    
    # 情绪评分
    leader_score = min(100, top5_weight * 150)  # 龙头股占比越高分越高
    diversity_score = min(100, industry_diversity * 10)  # 分散度越高分越高
    
    return (leader_score * 0.6 + diversity_score * 0.4)

def calculate_ETF_score(etf_code):
    """
    计算ETF综合评分（0-100分），用于排名决策
    参数:
        etf_code: ETF代码
    返回:
        dict: 包含各项评分和总分的字典
    """
    try:
        # 获取ETF基础数据
        etf_data = get_etf_data(etf_code, 'daily')
        if etf_data is None or etf_data.empty:
            logger.error(f"获取{etf_code}数据失败，无法计算评分")
            return None
        
        # 计算各项指标
        liquidity_score = calculate_liquidity_score(etf_code, etf_data)
        risk_score = calculate_risk_score(etf_code, etf_data)
        return_score = calculate_return_score(etf_code, etf_data)
        premium_score = calculate_premium_score(etf_code)
        sentiment_score = calculate_sentiment_score(etf_code)
        
        # 计算总评分
        total_score = (
            liquidity_score * SCORE_WEIGHTS['liquidity'] +
            risk_score * SCORE_WEIGHTS['risk'] +
            return_score * SCORE_WEIGHTS['return'] +
            premium_score * SCORE_WEIGHTS['premium'] +
            sentiment_score * SCORE_WEIGHTS['sentiment']
        )
        
        return {
            'code': etf_code,
            'total_score': round(total_score, 1),
            'liquidity_score': round(liquidity_score, 1),
            'risk_score': round(risk_score, 1),
            'return_score': round(return_score, 1),
            'premium_score': round(premium_score, 1),
            'sentiment_score': round(sentiment_score, 1)
        }
    except Exception as e:
        logger.error(f"计算{etf_code}综合评分失败: {str(e)}")
        return None

def generate_stock_pool():
    """
    生成股票池（5只稳健仓 + 5只激进仓）
    返回:
        DataFrame: 股票池
    """
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    
    # 计算每只ETF的评分
    scored_etfs = []
    for _, etf in etf_list.iterrows():
        score = calculate_ETF_score(etf['code'])
        if score:
            scored_etfs.append(score)
    
    if not scored_etfs:
        logger.error("未计算出有效的ETF评分。无法生成股票池。")
        return None
    
    # 转换为DataFrame以便处理
    scores_df = pd.DataFrame(scored_etfs)
    
    # 筛选和选择稳健ETF（风险控制评分 > 75）
    stable_etfs = scores_df[scores_df['risk_score'] > 75]
    
    if not stable_etfs.empty:
        # 按总分降序排序
        stable_etfs = stable_etfs.sort_values('total_score', ascending=False)
        # 选择前5名
        selected_stable = stable_etfs.head(5)
    else:
        logger.warning("没有符合标准的稳健ETF。使用备用ETF。")
        # 备用ETF（宽基）
        fallback_stable = [
            {'code': '510050', 'name': '上证50ETF'},
            {'code': '510300', 'name': '沪深300ETF'},
            {'code': '510500', 'name': '中证500ETF'},
            {'code': '159919', 'name': '300SCIE'},
            {'code': '515790', 'name': '光伏ETF'}
        ]
        # 获取备用ETF的评分
        selected_stable = pd.DataFrame([
            calculate_ETF_score(etf['code'])
            for etf in fallback_stable
        ])
    
    # 筛选和选择激进ETF（风险控制评分 ≤ 75）
    aggressive_etfs = scores_df[scores_df['risk_score'] <= 75]
    
    if not aggressive_etfs.empty:
        # 按总分降序排序
        aggressive_etfs = aggressive_etfs.sort_values('total_score', ascending=False)
        # 选择前5名
        selected_aggressive = aggressive_etfs.head(5)
    else:
        logger.warning("没有符合标准的激进ETF。使用备用ETF。")
        # 备用ETF（行业）
        fallback_aggressive = [
            {'code': '512480', 'name': '半导体ETF'},
            {'code': '512660', 'name': '军工ETF'},
            {'code': '512888', 'name': '消费ETF'},
            {'code': '512980', 'name': '通信ETF'},
            {'code': '159995', 'name': '芯片ETF'}
        ]
        # 获取备用ETF的评分
        selected_aggressive = pd.DataFrame([
            calculate_ETF_score(etf['code'])
            for etf in fallback_aggressive
        ])
    
    # 获取ETF名称（从原始列表）
    etf_names = {row['code']: row['name'] for _, row in etf_list.iterrows()}
    
    # 准备最终股票池DataFrame
    beijing_now = get_beijing_time()
    final_pool = pd.DataFrame({
        'type': ['稳健仓'] * len(selected_stable) + ['激进仓'] * len(selected_aggressive),
        'code': scores_df['code'].tolist(),
        'name': [etf_names.get(code, code) for code in scores_df['code']],
        'total_score': scores_df['total_score'].tolist(),
        'update_time': [beijing_now.strftime('%Y-%m-%d %H:%M')] * len(scores_df),
        'liquidity_score': scores_df['liquidity_score'].tolist(),
        'risk_score': scores_df['risk_score'].tolist(),
        'return_score': scores_df['return_score'].tolist(),
        'premium_score': scores_df['premium_score'].tolist(),
        'sentiment_score': scores_df['sentiment_score'].tolist()
    })
    
    # 保存到数据目录
    os.makedirs(Config.STOCK_POOL_DIR, exist_ok=True)
    filename = f"stock_pool_{beijing_now.strftime('%Y%m%d')}.csv"
    filepath = os.path.join(Config.STOCK_POOL_DIR, filename)
    final_pool.to_csv(filepath, index=False)
    
    logger.info(f"股票池生成成功。保存为: {filename}")
    logger.info(f"选定{len(selected_stable)}只稳健ETF和{len(selected_aggressive)}只激进ETF")
    
    return final_pool

def get_current_stock_pool():
    """
    获取当前有效的股票池
    返回:
        DataFrame: 当前股票池
    """
    # 从文件系统读取最新股票池
    try:
        # 获取股票池目录中最新的CSV文件
        pool_files = [f for f in os.listdir(Config.STOCK_POOL_DIR) if f.startswith('stock_pool_')]
        if not pool_files:
            logger.error("未找到股票池文件")
            return None
        
        # 获取最新文件
        latest_file = max(pool_files)
        return pd.read_csv(os.path.join(Config.STOCK_POOL_DIR, latest_file))
    except Exception as e:
        logger.error(f"获取股票池失败: {str(e)}")
        return None

def get_top_n_etfs(n=10):
    """
    获取评分最高的N只ETF
    参数:
        n: 要返回的ETF数量
    返回:
        DataFrame: 评分最高的N只ETF
    """
    # 获取所有ETF评分
    etf_list = get_all_etf_list()
    
    # 计算每只ETF的评分
    scored_etfs = []
    for _, etf in etf_list.iterrows():
        score = calculate_ETF_score(etf['code'])
        if score:
            scored_etfs.append(score)
    
    if not scored_etfs:
        return None
    
    # 转换为DataFrame并排序
    scores_df = pd.DataFrame(scored_etfs)
    top_etfs = scores_df.sort_values('total_score', ascending=False).head(n)
    
    return top_etfs

def calculate_strategy(code, name, etf_type):
    """
    基于评分系统计算单只ETF的策略信号
    参数:
        code: ETF代码
        name: ETF名称
        etf_type: ETF类型 ('stable'稳健仓, 'aggressive'激进仓)
    返回:
        dict: 策略信号
    """
    # 严格依赖评分系统，不重复计算
    etf_score = calculate_ETF_score(code)
    
    if etf_score is None:
        logger.error(f"ETF {code} 评分获取失败")
        return {
            'etf_code': code,
            'etf_name': name,
            'action': 'hold',
            'position': 0,
            'rationale': '评分数据获取失败',
            'total_score': 0
        }
    
    total_score = etf_score['total_score']
    
    # 根据评分和ETF类型决定操作（严格遵循评分系统说明）
    if etf_type == 'stable':
        # 稳健仓策略：高评分(85+) 50%，中等评分(70-85) 30%，低评分(<70) 0%
        if total_score >= 85:
            action = '买入'
            position = 50
            rationale = f"稳健仓高评分ETF ({total_score})：流动性{etf_score['liquidity_score']}，风险控制{etf_score['risk_score']}"
        elif total_score >= 70:
            action = '持有'
            position = 30
            rationale = f"稳健仓中等评分ETF ({total_score})：持有稳健仓位"
        else:
            action = '卖出'
            position = 0
            rationale = f"稳健仓低评分ETF ({total_score})：风险上升，建议清仓"
    else:  # aggressive
        # 激进仓策略：高评分(85+) 100%，中等评分(70-85) 50%，低评分(<70) 0%
        if total_score >= 85:
            action = '买入'
            position = 100
            rationale = f"激进仓高评分ETF ({total_score})：收益能力{etf_score['return_score']}，风险收益比{etf_score['return_score']-etf_score['risk_score']:.1f}"
        elif total_score >= 70:
            action = '持有'
            position = 50
            rationale = f"激进仓中等评分ETF ({total_score})：持有半仓"
        else:
            action = '卖出'
            position = 0
            rationale = f"激进仓低评分ETF ({total_score})：收益能力不足，建议清仓"
    
    return {
        'etf_code': code,
        'etf_name': name,
        'action': action,
        'position': position,
        'rationale': rationale,
        'total_score': total_score
    }

def push_strategy_results(test=False):
    """
    计算策略信号并推送到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功
    """
    logger.info(f"{'测试' if test else ''}策略计算与推送开始")
    
    # 获取当前股票池
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        logger.error("股票池为空，无法计算策略")
        return False
    
    # 按ETF类型分组
    stable_etfs = stock_pool[stock_pool['type'] == '稳健仓']
    aggressive_etfs = stock_pool[stock_pool['type'] == '激进仓']
    
    # 计算并推送稳健仓策略
    if not stable_etfs.empty:
        logger.info(f"开始推送稳健仓策略（{len(stable_etfs)}只ETF）")
        for _, etf in stable_etfs.iterrows():
            signal = calculate_strategy(etf['code'], etf['name'], 'stable')
            # 格式化消息
            message = _format_strategy_signal(signal, test=test)
            
            # 推送消息
            if not test:
                send_wecom_message(message)
            
            # 记录交易
            if not test:
                log_trade(signal)
            
            # 间隔1分钟
            time.sleep(60)
    
    # 计算并推送激进仓策略
    if not aggressive_etfs.empty:
        logger.info(f"开始推送激进仓策略（{len(aggressive_etfs)}只ETF）")
        for _, etf in aggressive_etfs.iterrows():
            signal = calculate_strategy(etf['code'], etf['name'], 'aggressive')
            # 格式化消息
            message = _format_strategy_signal(signal, test=test)
            
            # 推送消息
            if not test:
                send_wecom_message(message)
            
            # 记录交易
            if not test:
                log_trade(signal)
            
            # 间隔1分钟
            time.sleep(60)
    
    logger.info("策略推送完成")
    return True

def log_trade(signal):
    """
    记录交易流水
    参数:
        signal: 策略信号
    """
    # 确保交易日志目录存在
    os.makedirs(Config.TRADE_LOG_DIR, exist_ok=True)
    
    # 创建日志文件名
    filename = f"trade_log_{get_beijing_time().strftime('%Y%m%d')}.csv"
    filepath = os.path.join(Config.TRADE_LOG_DIR, filename)
    
    # 创建交易记录
    trade_record = {
        '时间': get_beijing_time().strftime('%Y-%m-%d %H:%M'),
        'ETF代码': signal['etf_code'],
        'ETF名称': signal['etf_name'],
        '操作': signal['action'],
        '仓位比例': signal['position'],
        '总评分': signal['total_score'],
        '策略依据': signal['rationale']
    }
    
    # 保存到CSV
    if os.path.exists(filepath):
        # 追加到现有文件
        df = pd.read_csv(filepath)
        df = pd.concat([df, pd.DataFrame([trade_record])], ignore_index=True)
        df.to_csv(filepath, index=False)
    else:
        # 创建新文件
        pd.DataFrame([trade_record]).to_csv(filepath, index=False)
    
    logger.info(f"交易记录已保存: {signal['etf_name']} - {signal['action']}")

def update_stock_pool():
    #本函数应在每周五16:00北京时间运行
    #更新ETF股票池（5只稳健仓 + 5只激进仓）
    # 获取当前股票池
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        logger.error("股票池为空，计算策略必须要有股票池")    
        # 生成新的股票池
        stock_pool = generate_stock_pool()
        if stock_pool is None or stock_pool.empty:
            logger.error("股票池生成失败，无法更新。")
            return None
        
    logger.info("开始周五下午4点自动更新股票池流程")
     
    # 获取当前北京时间
    beijing_now = get_beijing_time()
    
    # 检查今天是否是周五
    if beijing_now.weekday() != 4:  # 周五是4（周一是0）
        logger.info(f"今天是{beijing_now.strftime('%A')}，不是周五。跳过强制更新股票池。")
        return None
    
    # 检查时间是否在16:00之后
    if beijing_now.time() < datetime.time(16, 0):
        logger.info(f"当前时间是{beijing_now.strftime('%H:%M')}，早于16:00。跳过强制更新股票池。")
        return None
    
    # 生成新的股票池
    stock_pool = generate_stock_pool()
    
    if stock_pool is None:
        logger.error("股票池生成失败，无法更新。")
        return None
    
    logger.info(f"股票池强制更新成功。")
    logger.info(f"选定{len(stock_pool[stock_pool['type'] == '稳健仓'])}只稳健ETF和{len(stock_pool[stock_pool['type'] == '激进仓'])}只激进ETF")
    
    return stock_pool

def _is_test_request():
    """判断是否是测试请求（兼容非请求环境）"""
    try:
        # 尝试获取 Flask 请求参数
        return request.args.get('test', 'false').lower() == 'true'
    except RuntimeError:
        # 非请求环境（如 GitHub Actions）返回默认值
        return False
    except:
        # 其他异常情况也返回默认值
        return False

def _format_message(message, test=False):
    """格式化消息，测试请求添加标识"""
    if test:
        return f"【测试消息】\n{message}"
    return message

def _format_stock_pool_message(stock_pool, test=False):
    """格式化股票池消息"""
    if stock_pool is None or stock_pool.empty:
        return "当前股票池为空"
    
    # 获取当前时间
    beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
    
    # 构建消息
    message = f"{'T04: 测试推送' if test else '推送'}当前股票池\nCF系统时间：{beijing_time}\n【ETF股票池】\n"
    message += f"更新时间：{stock_pool['update_time'].iloc[0]}\n\n"
    
    # 稳健仓
    message += "【稳健仓】\n"
    stable_etfs = stock_pool[stock_pool['type'] == '稳健仓']
    for _, etf in stable_etfs.iterrows():
        message += f"{etf['code']} | {etf['name']} | 总分：{etf['total_score']}\n"
        message += f"筛选依据：流动性{etf['liquidity_score']}，风险控制{etf['risk_score']}，收益能力{etf['return_score']}\n\n"
    
    # 激进仓
    message += "【激进仓】\n"
    aggressive_etfs = stock_pool[stock_pool['type'] == '激进仓']
    for _, etf in aggressive_etfs.iterrows():
        message += f"{etf['code']} | {etf['name']} | 总分：{etf['total_score']}\n"
        message += f"筛选依据：收益能力{etf['return_score']}，风险收益比{etf['return_score']-etf['risk_score']:.1f}，情绪指标{etf['sentiment_score']}\n\n"
    
    return message

def _format_strategy_signal(signal, test=False):
    """格式化策略信号消息"""
    if not signal:
        return "无有效策略信号"
    
    # 构建消息
    message = f"{'T05: 测试执行' if test else '执行'}策略并推送结果\n"
    message += f"CF系统时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M')}\n"
    message += f"ETF代码：{signal['etf_code']}\n"
    message += f"名称：{signal['etf_name']}\n"
    message += f"操作建议：{signal['action']}\n"
    message += f"仓位比例：{signal['position']}%\n"
    message += f"策略依据：{signal['rationale']}"
    
    return message

def get_cache_path(etf_code, data_type='daily'):
    """
    生成指定ETF和数据类型的缓存文件路径
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        str: 缓存文件路径
    """
    base_path = os.path.join(Config.RAW_DATA_DIR, 'etf_data')
    os.makedirs(base_path, exist_ok=True)
    
    if data_type == 'daily':
        return os.path.join(base_path, f"{etf_code}_daily.csv")
    else:
        return os.path.join(base_path, f"{etf_code}_intraday_{datetime.datetime.now().strftime('%Y%m%d')}.csv")

def load_from_cache(etf_code, data_type='daily', days=30):
    """
    从缓存加载ETF数据（如果可用）
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
        days: 要加载的天数
    返回:
        DataFrame: 缓存数据或None（如果不可用）
    """
    cache_path = get_cache_path(etf_code, data_type)
    try:
        if os.path.exists(cache_path):
            df = pd.read_csv(cache_path)
            df['date'] = pd.to_datetime(df['date'])
            # 筛选近期数据
            if data_type == 'daily':
                df = df[df['date'] >= (datetime.datetime.now() - datetime.timedelta(days=days))]
            return df
    except Exception as e:
        logger.error(f"缓存加载错误 {etf_code}: {str(e)}")
    return None

def save_to_cache(etf_code, data, data_type='daily'):
    """
    将ETF数据保存到缓存
    参数:
        etf_code: ETF代码
         DataFrame数据
        data_type: 'daily'或'intraday'
    """
    cache_path = get_cache_path(etf_code, data_type)
    
    if os.path.exists(cache_path):
        # 追加到现有文件
        existing_data = pd.read_csv(cache_path)
        combined = pd.concat([existing_data, data]).drop_duplicates(subset=['date'], keep='last')
        combined.to_csv(cache_path, index=False)
    else:
        # 创建新文件
        data.to_csv(cache_path, index=False)

def crawl_akshare(etf_code):
    """
    从AkShare爬取ETF数据（主数据源）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）
    """
    try:
        # 从AkShare获取日线数据
        # 移除无效的period参数
        df = ak.fund_etf_hist_sina(symbol=etf_code)
        df.rename(columns={'日期': 'date', '收盘价': 'close'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        return df
        if df.empty:
            logger.error(f"AkShare返回空数据 {etf_code}")
            return None
        
        # 重命名列为标准格式
        df = df.rename(columns={
            'date': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        # 将日期转换为datetime
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        logger.error(f"AkShare爬取错误 {etf_code}: {str(e)}")
        return None

def crawl_baostock(etf_code):
    """
    从Baostock爬取ETF数据（备用数据源1）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）
    """
    try:
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            return None
        
        # 为Baostock格式化ETF代码（添加sh.或sz.前缀）
        market = 'sh' if etf_code.startswith('5') else 'sz'
        code = f"{market}.{etf_code}"
        
        # 获取历史数据
        rs = bs.query_history_k_data_plus(
            code, "date,open,high,low,close,volume",
            start_date=(datetime.datetime.now() - datetime.timedelta(days=100)).strftime('%Y-%m-%d'),
            end_date=datetime.datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3"
        )
        
        if rs.error_code != '0':
            logger.error(f"Baostock查询失败: {rs.error_msg}")
            bs.logout()
            return None
        
        # 转换为DataFrame
        df = rs.get_data()
        bs.logout()  # 使用后登出
        
        if df.empty:
            return None
        
        # 转换数据类型
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        return df
    except Exception as e:
        logger.error(f"Baostock爬取错误 {etf_code}: {str(e)}")
        try:
            bs.logout()
        except:
            pass
        return None

def crawl_sina_finance(etf_code):
    """
    从新浪财经爬取ETF数据（备用数据源2）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）
    """
    try:
       # 1. 修正交易所前缀处理
        if etf_code.startswith('5'):
            exchange_prefix = 'sh'
        elif etf_code.startswith('1'):
            exchange_prefix = 'sz'
        else:
            exchange_prefix = ''
            
        # 2. 正确构建URL
        full_code = f"{exchange_prefix}{etf_code}"
        sina_url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={full_code}&scale=240&ma=no&datalen=100"
        
        # 3. 添加User-Agent头避免被拒绝
        response = requests.get(sina_url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
      
        # 获取数据
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        
        # 解析JSON响应
        data = response.json()
        if not data or 'data' not in data:
            return None
        kline_data = data['data']
        
        # 转换为DataFrame
        df = pd.DataFrame(data)
        df = df.rename(columns={
            'day': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        # 转换数据类型
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        return df
    except Exception as e:
        logger.error(f"新浪财经爬取错误 {etf_code}: {str(e)}")
        return None

def get_etf_data(etf_code, data_type='daily'):
    """
    从多数据源获取ETF数据（带自动回退机制）
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        DataFrame: ETF数据或None（如果所有数据源都失败）
    """
    # 首先检查缓存
    cached_data = load_from_cache(etf_code, data_type)
    if cached_data is not None and not cached_data.empty:
        logger.info(f"从缓存加载{etf_code}数据")
        return cached_data
    
    # 尝试主数据源(AkShare) - 仅用于日线数据
    if data_type == 'daily':
        data = crawl_akshare(etf_code)
        if data is not None and not data.empty:
            logger.info(f"成功从AkShare爬取{etf_code}日线数据")
            save_to_cache(etf_code, data, data_type)
            return data
    
    # 尝试备用数据源1(Baostock)
    data = crawl_baostock(etf_code)
    if data is not None and not data.empty:
        logger.info(f"成功从Baostock爬取{etf_code}数据")
        save_to_cache(etf_code, data, data_type)
        return data
    
    # 尝试备用数据源2(新浪财经)
    data = crawl_sina_finance(etf_code)
    if data is not None and not data.empty:
        logger.info(f"成功从新浪财经爬取{etf_code}数据")
        save_to_cache(etf_code, data, data_type)
        return data
    
    # 所有数据源均失败
    logger.error(f"无法从所有数据源获取{etf_code}数据")
    return None

def get_all_etf_list():
    """
    从多数据源获取所有ETF列表
    返回:
        DataFrame: ETF列表，包含代码和名称
    """
    try:
        # 从AkShare获取ETF列表（主数据源）- 使用新的接口
        logger.info("尝试从AkShare获取ETF列表...")
        df = ak.fund_etf_hist_sina(symbol="etf")
        df['code'] = df['基金代码'].apply(lambda x: f"sh.{x}" if x.startswith('5') else f"sz.{x}")
        return df[['code', '基金名称']]
        if not df.empty:
            # 筛选仅保留ETF
            etf_list = df.copy()
            etf_list = etf_list[['symbol', 'name']]
            etf_list.columns = ['code', 'name']
            logger.info(f"从AkShare成功获取 {len(etf_list)} 只ETF")
            return etf_list
    except Exception as e:
        logger.error(f"AkShare获取ETF列表失败: {str(e)}")
    
    # 尝试Baostock（备用数据源1）
    try:
        logger.info("尝试从Baostock获取ETF列表...")
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 查询ETF基金
        rs = bs.query_stock_basic(code="sh51")
        etf_list = []
        while (rs.error_code == '0') & rs.next():
            etf_list.append(rs.get_row_data())
        
        rs = bs.query_stock_basic(code="sz159")
        while (rs.error_code == '0') & rs.next():
            etf_list.append(rs.get_row_data())
        
        if etf_list:
            df = pd.DataFrame(etf_list, columns=rs.fields)
            # 筛选ETF
            df = df[df['code_name'].str.contains('ETF')]
            df = df[['code', 'code_name']]
            df.columns = ['code', 'name']
            # 移除交易所前缀
            df['code'] = df['code'].str.replace('sh.', '').str.replace('sz.', '')
            
            logger.info(f"从Baostock成功获取 {len(df)} 只ETF")
            return df
        
        logger.warning("Baostock返回空数据，尝试下一个数据源...")
    except Exception as e:
        logger.error(f"Baostock获取ETF列表失败: {str(e)}")
    finally:
        try:
            bs.logout()
        except:
            pass
    
    # 尝试新浪财经（备用数据源2）
    try:
        logger.info("尝试从新浪财经获取ETF列表...")
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=etf_hk&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data:
            etf_list = pd.DataFrame(data)
            etf_list = etf_list[['symbol', 'name']]
            etf_list.columns = ['code', 'name']
            logger.info(f"从新浪财经成功获取 {len(etf_list)} 只ETF")
            return etf_list
    except Exception as e:
        logger.error(f"新浪财经获取ETF列表失败: {str(e)}")
    
    # 如果所有数据源都失败，返回一个默认列表
    logger.error("所有数据源均无法获取ETF列表，使用默认ETF列表")
    return pd.DataFrame({
        'code': ['510050', '510300', '510500', '159915', '512888', '512480', '512660', '512980', '159825', '159995'],
        'name': ['上证50ETF', '沪深300ETF', '中证500ETF', '创业板ETF', '消费ETF', '半导体ETF', '军工ETF', '通信ETF', '新能源ETF', '医疗ETF']
    })

def calculate_component_weights(etf_code):
    """
    计算ETF成分股权重
    参数:
        etf_code: ETF代码
    返回:
        dict: 成分股权重
    """
    try:
        # 从AkShare获取成分股数据
        component_data = ak.fund_etf_component_sina(symbol=etf_code)
        
        if component_data.empty:
            logger.warning(f"{etf_code}成分股数据为空")
            return {}
        
        # 计算权重
        total_market_cap = component_data['总市值'].sum()
        weights = {}
        for _, row in component_data.iterrows():
            weights[row['股票代码']] = row['总市值'] / total_market_cap
        
        return weights
    except Exception as e:
        logger.error(f"计算{etf_code}成分股权重失败: {str(e)}")
        return {}

def estimate_etf_nav(etf_code, component_prices=None):
    """
    估算ETF净值
    参数:
        etf_code: ETF代码
        component_prices: 可选的成分股价格
    返回:
        float: 估算的净值
    """
    try:
        # 获取成分股权重
        weights = calculate_component_weights(etf_code)
        if not weights:
            return None
        
        # 获取成分股价格
        if component_prices is None:
            component_prices = {}
            for stock_code in weights.keys():
                stock_data = get_etf_data(stock_code, 'intraday')
                if stock_data is not None and not stock_data.empty:
                    component_prices[stock_code] = stock_data['close'].iloc[-1]
        
        # 计算净值
        nav = 0
        for stock_code, weight in weights.items():
            if stock_code in component_prices:
                nav += component_prices[stock_code] * weight
        
        # 考虑管理费等因素
        nav *= 0.995  # 假设年化管理费0.5%
        
        return nav
    except Exception as e:
        logger.error(f"估算{etf_code}净值失败: {str(e)}")
        return None

def calculate_premium_rate(etf_code, etf_price=None):
    """
    计算ETF溢价率
    参数:
        etf_code: ETF代码
        etf_price: 可选的ETF价格
    返回:
        float: 溢价率（百分比）
    """
    # 获取ETF价格（如未提供）
    if etf_price is None:
        data = get_etf_data(etf_code, 'intraday')
        if data is None or data.empty:
            return 0.0
        etf_price = data['close'].iloc[-1]
    
    # 估算净值
    nav = estimate_etf_nav(etf_code)
    if nav is None or nav == 0:
        return 0.0
    
    # 计算溢价率
    premium_rate = (etf_price - nav) / nav * 100
    return premium_rate

def get_new_stock_subscriptions():
    """
    获取当天可申购的新股（IPO）
    使用多数据源回退机制：
    1. 主数据源：AkShare
    2. 备用数据源1：Baostock
    3. 备用数据源2：新浪财经
    
    返回:
        DataFrame: 当天可申购的新股信息
    """
    today = datetime.datetime.now().strftime('%Y-%m-%d')

    # 尝试AkShare（主数据源）
    try:
        logger.info("尝试从AkShare获取新股认购信息...")
        # 调用 stock_xgsglb_em 接口，可根据需要设置 symbol 参数，这里先取全部股票
        df = ak.stock_xgsglb_em(symbol="全部股票")  
        if not df.empty:
            # 假设返回数据里 '申购日期' 字段对应申购日期，需和 today 匹配
            df = df[df['申购日期'] == today]  
            if not df.empty:
                # 按需提取字段，这里根据你之前返回的字段名示例，选取常用字段
                return df[['股票代码', '股票简称', '发行价格', '申购上限', '申购日期']]
            logger.warning("AkShare返回空数据（当天无新股可认购或接口返回无匹配），尝试备用数据源...")
            except Exception as e:
                    logger.error(f"AkShare获取新股认购信息失败: {str(e)}")
    
    # 尝试Baostock（备用数据源1）
    try:
        logger.info("尝试从Baostock获取新股申购信息...")
        login_result = bs.login()
        if login_result.error_code != '0':
            raise Exception("Baostock登录失败")
        
        rs = bs.query_new_stocks()
        df = rs.get_data()
        bs.logout()
        
        if not df.empty:
            df = df[df['ipoDate'] == today]
            if not df.empty:
                return df[['code', 'code_name', 'price', 'max_purchase', 'ipoDate']].rename(columns={
                    'code': 'code',
                    'code_name': 'name',
                    'price': 'issue_price',
                    'max_purchase': 'max_purchase',
                    'ipoDate': 'publish_date'
                })
        logger.warning("Baostock返回空数据，尝试下一个数据源...")
    except Exception as e:
        logger.error(f"Baostock获取新股申购信息失败: {str(e)}")
    
    # 尝试新浪财经（备用数据源2）
    try:
        logger.info("尝试从新浪财经获取新股申购信息...")
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        new_stocks = []
        for item in data:
            code = item.get('申购代码', '')
            name = item.get('股票简称', '')
            issue_price = item.get('发行价格', '')
            max_purchase = item.get('申购上限', '')
            publish_date = item.get('申购日期', '')
            
            if publish_date == today:
                new_stocks.append({
                    'code': code,
                    'name': name,
                    'issue_price': issue_price,
                    'max_purchase': max_purchase,
                    'publish_date': publish_date
                })
        
        if new_stocks:
            return pd.DataFrame(new_stocks)
    except Exception as e:
        logger.error(f"新浪财经获取新股申购信息失败: {str(e)}")
    
    return pd.DataFrame()

def get_new_stock_listings():
    """
    获取当天新上市交易的新股
    使用多数据源回退机制：
    1. 主数据源：AkShare
    2. 备用数据源1：Baostock
    3. 备用数据源2：新浪财经
    
    返回:
        DataFrame: 当天新上市交易的新股信息
    """
    today = get_beijing_time().strftime('%Y-%m-%d')
    
    # 尝试AkShare（主数据源）
    # 尝试AkShare（主数据源，使用 stock_xgsglb_em 接口）
    try:
        logger.info("尝试从AkShare获取新上市交易股票信息...")
        # 调用 stock_xgsglb_em 接口，可根据需要设置 symbol 参数，这里先取全部股票
        df = ak.stock_xgsglb_em(symbol="全部股票")  
        if not df.empty:
            # 假设返回数据里 '上市日期' 字段对应上市日期，需和 today 匹配
            df = df[df['上市日期'] == today]  
            if not df.empty:
                # 按需提取字段，这里根据你之前返回的字段名示例，选取常用字段
                return df[['股票代码', '股票简称', '发行价格',  '上市日期']]
        logger.warning("AkShare返回空数据（当天无新股或接口返回无匹配），尝试备用数据源...")
    except Exception as e:
        logger.error(f"AkShare获取新上市交易股票信息失败: {str(e)}")
    
    # 尝试Baostock（备用数据源1）
    try:
        logger.info("尝试从Baostock获取新上市交易股票信息...")
        login_result = bs.login()
        if login_result.error_code != '0':
            raise Exception("Baostock登录失败")
        
        rs = bs.query_all_stock()
        df = rs.get_data()
        bs.logout()
        
        if not df.empty:
            df = df[df['ipoDate'] == today]
            if not df.empty:
                return df[['code', 'code_name', 'price', 'max_purchase', 'ipoDate']].rename(columns={
                    'code': 'code',
                    'code_name': 'name',
                    'price': 'issue_price',
                    'max_purchase': 'max_purchase',
                    'ipoDate': 'listing_date'
                })
        logger.warning("Baostock返回空数据，尝试下一个数据源...")
    except Exception as e:
        logger.error(f"Baostock获取新上市交易股票信息失败: {str(e)}")
    
    # 尝试新浪财经（备用数据源2）
    try:
        logger.info("尝试从新浪财经获取新上市交易股票信息...")
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        new_listings = []
        for item in data:
            code = item.get('symbol', '')
            name = item.get('name', '')
            issue_price = item.get('price', '')
            max_purchase = item.get('limit', '')
            listing_date = item.get('listing_date', '')
            
            if listing_date == today:
                new_listings.append({
                    'code': code,
                    'name': name,
                    'issue_price': issue_price,
                    'max_purchase': max_purchase,
                    'listing_date': listing_date
                })
        
        if new_listings:
            return pd.DataFrame(new_listings)
    except Exception as e:
        logger.error(f"新浪财经获取新上市交易股票信息失败: {str(e)}")
    
    return pd.DataFrame()

def get_test_new_stock_subscriptions():
    """
    获取测试用的新股申购信息（当天无数据时回溯7天）
    返回:
        DataFrame: 测试数据
    """
    # 尝试获取当天真实数据（用于定时任务）
    today_stocks = get_new_stock_subscriptions()
    if not today_stocks.empty:
        return today_stocks
    
    # 回溯7天
    for i in range(1, 8):
        date_str = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d')
        try:
            # 尝试AkShare（主数据源）
            logger.info(f"尝试从AkShare获取{date_str}的历史新股数据...")
            df = ak.stock_ipo_info()
            if not df.empty:
                # 转换为日期格式
                target_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                df = df[df['publish_date'] == target_date]
                if not df.empty:
                    return df[['申购代码', '股票简称', '发行价格', '申购上限', '申购日期']]
        except:
            pass
        
        # 尝试Baostock（备用数据源1）
        try:
            logger.info(f"尝试从Baostock获取{date_str}的历史新股数据...")
            login_result = bs.login()
            if login_result.error_code != '0':
                raise Exception("Baostock登录失败")
            
            rs = bs.query_new_stocks()
            df = rs.get_data()
            bs.logout()
            
            if not df.empty:
                df = df[df['ipoDate'] == date_str]
                if not df.empty:
                    return df[['申购代码', '股票简称', '发行价格', '申购上限', '申购日期']].rename(columns={
                        'code': 'code',
                        'code_name': 'name',
                        'price': 'issue_price',
                        'max_purchase': 'max_purchase',
                        'ipoDate': 'publish_date'
                    })
        except:
            pass
        
        # 尝试新浪财经（备用数据源2）
        try:
            logger.info(f"尝试从新浪财经获取{date_str}的历史新股数据...")
            sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
            response = requests.get(sina_url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            new_stocks = []
            for item in data:
                code = item.get('申购代码', '')
                name = item.get('股票简称', '')
                issue_price = item.get('发行价格', '')
                max_purchase = item.get('申购上限', '')
                publish_date = item.get('申购上限', '')
                
                if publish_date == f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}":
                    new_stocks.append({
                        'code': code,
                        'name': name,
                        'issue_price': issue_price,
                        'max_purchase': max_purchase,
                        'publish_date': publish_date
                    })
            
            if new_stocks:
                return pd.DataFrame(new_stocks)
        except:
            pass
    
    return pd.DataFrame()

def get_test_new_stock_listings():
    """
    获取测试用的新上市交易股票信息（当天无数据时回溯7天）
    返回:
        DataFrame: 测试数据
    """
    # 尝试获取当天真实数据（用于定时任务）
    today_listings = get_new_stock_listings()
    if not today_listings.empty:
        return today_listings
    
    # 回溯7天
    for i in range(1, 8):
        date_str = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d')
        try:
            # 尝试AkShare（主数据源）
            logger.info(f"尝试从AkShare获取{date_str}的历史新上市数据...")
            df = ak.stock_ipo_info()
            if not df.empty:
                # 转换为日期格式
                target_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                df = df[df['list_date'] == target_date]
                if not df.empty:
                    return df[['股票代码', '股票简称', '发行价格', '申购上限', '上市日期']]
        except:
            pass
        
        # 尝试Baostock（备用数据源1）
        try:
            logger.info(f"尝试从Baostock获取{date_str}的历史新上市数据...")
            login_result = bs.login()
            if login_result.error_code != '0':
                raise Exception("Baostock登录失败")
            
            rs = bs.query_all_stock()
            df = rs.get_data()
            bs.logout()
            
            if not df.empty:
                df = df[df['ipoDate'] == date_str]
                if not df.empty:
                    return df[['股票代码', '股票简称', '发行价格', '申购上限', '上市日期']].rename(columns={
                        'code': 'code',
                        'code_name': 'name',
                        'price': 'issue_price',
                        'max_purchase': 'max_purchase',
                        'ipoDate': 'listing_date'
                    })
        except:
            pass
        
        # 尝试新浪财经（备用数据源2）
        try:
            logger.info(f"尝试从新浪财经获取{date_str}的历史新上市数据...")
            sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
            response = requests.get(sina_url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            new_listings = []
            for item in data:
                code = item.get('symbol', '')
                name = item.get('name', '')
                issue_price = item.get('price', '')
                max_purchase = item.get('limit', '')
                listing_date = item.get('listing_date', '')
                
                if listing_date == f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}":
                    new_listings.append({
                        'code': code,
                        'name': name,
                        'issue_price': issue_price,
                        'max_purchase': max_purchase,
                        'listing_date': listing_date
                    })
            
            if new_listings:
                return pd.DataFrame(new_listings)
        except:
            pass
    
    return pd.DataFrame()

def format_new_stock_subscriptions_message(new_stocks):
    """
    格式化新股申购信息消息
    参数:
        new_stocks: 新股DataFrame
    返回:
        str: 格式化后的消息
    """
    if new_stocks is None or new_stocks.empty:
        return "今天没有新股、新债、新债券可认购"
    
    # 仅包含新股基本信息，不涉及任何ETF评分
    message = "【今日新股申购信息】\n"
    for _, row in new_stocks.iterrows():
        # 确保只使用新股基本信息
        code = row.get('申购代码', '')
        name = row.get('股票简称', '')
        issue_price = row.get('发行价格', '')
        max_purchase = row.get('申购上限', '')
        publish_date = row.get('申购日期', '')
        
        # 格式化消息 - 仅包含新股基本信息
        message += f"\n申购代码：{code}\n"
        message += f"股票简称：{name}\n"
        message += f"发行价格：{issue_price}元\n"
        message += f"申购上限：{max_purchase}股\n"
        message += f"申购日期：{publish_date}\n"
        message += "─" * 20
    
    return message

def format_new_stock_listings_message(new_listings):
    """
    格式化新上市交易股票信息消息
    参数:
        new_listings: 新上市交易股票DataFrame
    返回:
        str: 格式化后的消息
    """
    if new_listings is None or new_listings.empty:
        return "今天没有新上市股票、可转债、债券可供交易"
    
    # 仅包含新上市交易股票基本信息
    message = "【近期新上市交易股票】\n"
    for _, row in new_listings.iterrows():
        # 确保只使用新上市交易股票基本信息
        code = row.get('股票代码', '')
        name = row.get('股票简称', '')
        issue_price = row.get('发行价格', '')
        max_purchase = row.get('申购上限', '')
        listing_date = row.get('上市日期', '')
        
        # 格式化消息 - 仅包含新上市交易股票基本信息
        message += f"\n股票代码：{code}\n"
        message += f"股票简称：{name}\n"
        message += f"发行价格：{issue_price}元\n"
        message += f"申购上限：{max_purchase}股\n"
        message += f"上市日期：{listing_date}\n"
        message += "─" * 20
    
    return message

def is_new_stock_info_pushed(target_date=None):
    """
    检查是否已经推送过新股信息，并返回推送日期
    :param target_date: 需要比较的目标日期（datetime.date对象），默认为None
    :return: 
        - 文件不存在时返回 (None, False) 或 None
        - 存在时返回文件中的日期
        - 当提供target_date时，额外返回日期是否匹配的布尔值
    """
    flag_file = Config.NEW_STOCK_PUSHED_FLAG
    
    if not os.path.exists(flag_file):
        return (None, False) if target_date is not None else None
    
    try:
        # 读取文件内容
        with open(flag_file, 'r') as f:
            content = f.read().strip()
            
        # 尝试解析日期
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                file_date = datetime.strptime(content, fmt).date()
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"无法解析日期: {content}")
            
        # 根据是否提供目标日期返回不同结果
        if target_date is None:
            return file_date
        else:
            # 确保target_date是日期类型
            if isinstance(target_date, datetime):
                target_date = target_date.date()
            return file_date, (file_date == target_date)
            
    except Exception as e:
        print(f"读取推送标记文件错误: {str(e)}")
        return (None, False) if target_date is not None else None

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    with open(Config.NEW_STOCK_PUSHED_FLAG, 'w') as f:
        f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

def clear_new_stock_pushed_flag():
    """清除新股信息推送标记"""
    if os.path.exists(Config.NEW_STOCK_PUSHED_FLAG):
        os.remove(Config.NEW_STOCK_PUSHED_FLAG)

def is_listing_info_pushed(target_date=None):
    """
    检查是否已经推送过新上市交易股票信息，并返回推送日期
    :param target_date: 需要比较的目标日期（datetime.date对象），默认为None
    :return: 
        - 文件不存在时返回 (None, False) 或 None
        - 存在时返回文件中的日期
        - 当提供target_date时，额外返回日期是否匹配的布尔值
    """
    flag_file = Config.LISTING_PUSHED_FLAG
    
    if not os.path.exists(flag_file):
        return (None, False) if target_date is not None else None
    
    try:
        # 读取文件内容
        with open(flag_file, 'r') as f:
            content = f.read().strip()
            
        # 尝试解析日期
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                file_date = datetime.strptime(content, fmt).date()
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"无法解析日期: {content}")
            
        # 根据是否提供目标日期返回不同结果
        if target_date is None:
            return file_date
        else:
            # 确保target_date是日期类型
            if isinstance(target_date, datetime):
                target_date = target_date.date()
            return file_date, (file_date == target_date)
            
    except Exception as e:
        print(f"读取推送标记文件错误: {str(e)}")
        return (None, False) if target_date is not None else None

def mark_listing_info_pushed():
    """标记新上市交易股票信息已推送"""
    with open(Config.LISTING_PUSHED_FLAG, 'w') as f:
        f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

def clear_listing_info_pushed_flag():
    """清除新上市交易股票信息推送标记"""
    if os.path.exists(Config.LISTING_PUSHED_FLAG):
        os.remove(Config.LISTING_PUSHED_FLAG)

# ================== 新增：套利策略相关函数 ==================

def get_arbitrage_status():
    """获取当前套利ETF状态"""
    if not os.path.exists(Config.ARBITRAGE_STATUS_FILE):
        return None
    
    try:
        with open(Config.ARBITRAGE_STATUS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"读取套利状态文件失败: {str(e)}")
        return None

def update_arbitrage_status(etf_code, etf_name, buy_time, buy_price, target_price, stop_loss_price):
    """更新套利ETF状态"""
    status = {
        "etf_code": etf_code,
        "etf_name": etf_name,
        "buy_time": buy_time,
        "buy_price": buy_price,
        "target_price": target_price,
        "stop_loss_price": stop_loss_price
    }
    
    try:
        with open(Config.ARBITRAGE_STATUS_FILE, 'w') as f:
            json.dump(status, f)
        return True
    except Exception as e:
        logger.error(f"更新套利状态文件失败: {str(e)}")
        return False

def clear_arbitrage_status():
    """清除套利ETF状态"""
    try:
        if os.path.exists(Config.ARBITRAGE_STATUS_FILE):
            os.remove(Config.ARBITRAGE_STATUS_FILE)
        return True
    except Exception as e:
        logger.error(f"清除套利状态文件失败: {str(e)}")
        return False

def is_holding_over_one_trading_day(buy_time_str):
    """检查套利ETF持有时间是否超过一个交易日"""
    try:
        # 解析买入时间
        buy_time = datetime.datetime.fromisoformat(buy_time_str)
        
        # 获取当前时间
        current_time = get_beijing_time()
        
        # 计算两个时间之间的交易日数量
        trading_days = 0
        current_date = buy_time.date()
        
        while current_date < current_time.date():
            # 检查是否为交易日（周一至周五）
            if current_date.weekday() < 5:  # 周一=0，周日=6
                trading_days += 1
            current_date += datetime.timedelta(days=1)
        
        # 如果交易日数量 >= 1，则认为持有时间超过一个交易日
        return trading_days >= 1
    except Exception as e:
        logger.error(f"检查持有时间失败: {str(e)}")
        return False

def scan_arbitrage_opportunities():
    """扫描套利机会"""
    logger.info("开始扫描套利机会")
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        logger.error("未获取到ETF列表，跳过套利扫描")
        return None
    
    # 扫描每只ETF
    opportunities = []
    for _, etf in etf_list.iterrows():
        etf_code = etf['code']
        etf_name = etf['name']

        try:
            # 获取ETF溢价率,修正：移除对shares模块的无效调用
            premium_rate = calculate_premium_rate(etf_code)
        
            # 判断是否有套利机会
            # 通常，溢价率过高（如>2%）或过低（如<-2%）可能存在套利机会
            # 这里可以根据实际情况调整阈值
            if abs(premium_rate) >= 2.0:
                # 获取ETF当前价格
                etf_data = get_etf_data(etf_code, 'intraday')
                if etf_data is None or etf_data.empty:
                    continue
                etf_price = etf_data['close'].iloc[-1]
            
                # 估算ETF净值
                nav = estimate_etf_nav(etf_code)
                if nav is None or nav == 0:
                    continue
            
                # 计算止盈目标价格和止损价格
                # 止盈目标：溢价率回归到0%附近
                if premium_rate > 0.1:
                    # 溢价情况：ETF价格高于净值，预期价格下跌
                    target_price = nav * 1.005  # 回归到0.5%溢价
                    stop_loss_price = etf_price * 1.02  # 溢价扩大2%
                else:
                    # 折价情况：ETF价格低于净值，预期价格上涨
                    target_price = nav * 0.995  # 回归到-0.5%折价
                    stop_loss_price = etf_price * 0.98  # 折价扩大2%
            
                opportunities.append({
                    "etf_code": etf_code,
                    "etf_name": etf_name,
                    "premium_rate": premium_rate,
                    "current_price": etf_price,
                    "nav": nav,
                    "target_price": target_price,
                    "stop_loss_price": stop_loss_price
                  })
        except Exception as e:
            logger.error(f"处理ETF {etf_code} 时出错: {str(e)}")
    
    if opportunities:
        logger.info(f"发现 {len(opportunities)} 个套利机会")
        return opportunities
    else:
        logger.info("未发现套利机会")
        return None

# ================== 套利策略相关函数结束 ==================

def retry_push():
    """在交易时间段内每30分钟检查是否需要重试推送"""
    now = datetime.datetime.now()
    current_time = now.time()
    
    # 检查是否在交易时间段内
    if not (datetime.time(9, 30) <= current_time <= datetime.time(15, 0)):
        return False
    
    # 检查新股信息是否已推送
    if not is_new_stock_info_pushed(target_date=now):
        logger.info("新股信息未推送，尝试重试")
        push_new_stock_info()
        return True
    
    # 检查新上市交易股票信息是否已推送
    if not is_listing_info_pushed(target_date=now):
        logger.info("新上市交易股票信息未推送，尝试重试")
        push_listing_info()
        return True
    
    return False

def push_new_stock_info(test=False):
    """
    推送当天可申购的新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功
    """
    new_stocks = get_new_stock_subscriptions()
    
    if new_stocks is None or new_stocks.empty:
        message = "今天没有新股、新债、新债券可认购"
    else:
        message = format_new_stock_subscriptions_message(new_stocks)
    
    if test:
        message = "【测试消息】\n" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        mark_new_stock_info_pushed()
    
    return success

def push_listing_info(test=False):
    """
    推送当天新上市交易的新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功
    """
    new_listings = get_new_stock_listings()
    
    if new_listings is None or new_listings.empty:
        message = "今天没有新上市股票、可转债、债券可供交易"
    else:
        message = format_new_stock_listings_message(new_listings)
    
    if test:
        message = "【测试消息】\n" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        mark_listing_info_pushed()
    
    return success

# ========== 定时任务端点 ==========

@app.route('/health')
def health_check():
    """健康检查"""
    response = {
        "status": "healthy",
        "timestamp": get_beijing_time().isoformat(),
        "environment": "production"
    }
    return jsonify(response) if has_app_context() else response

@app.route('/cron/crawl_daily', methods=['GET', 'POST'])
def cron_crawl_daily():
    """日线数据爬取任务"""
    logger.info("日线数据爬取任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过爬取")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        logger.error("未获取到ETF列表，跳过爬取")
        response = {"status": "skipped", "message": "No ETF list available"}
        return jsonify(response) if has_app_context() else response
    
    # 爬取每只ETF的日线数据
    success = True
    for _, etf in etf_list.iterrows():
        data = get_etf_data(etf['code'], 'daily')
        if data is None or data.empty:
            logger.error(f"爬取{etf['code']}日线数据失败")
            success = False
        time.sleep(1)  # 避免请求过快
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/crawl_intraday', methods=['GET', 'POST'])
def cron_crawl_intraday():
    """盘中数据爬取任务"""
    logger.info("盘中数据爬取任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过爬取")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        logger.error("未获取到ETF列表，跳过爬取")
        response = {"status": "skipped", "message": "No ETF list available"}
        return jsonify(response) if has_app_context() else response
    
    # 爬取每只ETF的盘中数据
    success = True
    for _, etf in etf_list.iterrows():
        data = get_etf_data(etf['code'], 'intraday')
        if data is None or data.empty:
            logger.error(f"爬取{etf['code']}盘中数据失败")
            success = False
        time.sleep(1)  # 避免请求过快
    
    # ================== 新增：套利扫描逻辑 ==================
    # 检查当前是否持有套利ETF
    arbitrage_status = get_arbitrage_status()
    
    if arbitrage_status:
        # 检查持有时间是否超过一个交易日
        if is_holding_over_one_trading_day(arbitrage_status['buy_time']):
            # 推送获利了结消息
            message = f"套利ETF {arbitrage_status['etf_code']}({arbitrage_status['etf_name']}) 在 {arbitrage_status['buy_time']} 买入，建议获利了结。"
            send_wecom_message(message)
            
            # 清除套利状态
            clear_arbitrage_status()
        else:
            # 检查是否达到止盈或止损条件
            etf_code = arbitrage_status['etf_code']
            etf_data = get_etf_data(etf_code, 'intraday')
            if etf_data is not None and not etf_data.empty:
                current_price = etf_data['close'].iloc[-1]
                target_price = arbitrage_status['target_price']
                stop_loss_price = arbitrage_status['stop_loss_price']
                
                # 检查是否达到止盈条件
                if (current_price >= target_price and float(arbitrage_status['buy_price']) < target_price) or \
                   (current_price <= target_price and float(arbitrage_status['buy_price']) > target_price):
                    message = f"套利ETF {etf_code}({arbitrage_status['etf_name']}) 达到止盈目标价格 {target_price}，当前价格 {current_price}，建议获利了结。"
                    send_wecom_message(message)
                    clear_arbitrage_status()
                
                # 检查是否达到止损条件
                elif (current_price <= stop_loss_price and float(arbitrage_status['buy_price']) < stop_loss_price) or \
                     (current_price >= stop_loss_price and float(arbitrage_status['buy_price']) > stop_loss_price):
                    message = f"套利ETF {etf_code}({arbitrage_status['etf_name']}) 达到止损价格 {stop_loss_price}，当前价格 {current_price}，建议止损离场。"
                    send_wecom_message(message)
                    clear_arbitrage_status()
    else:
        # 没有持有套利ETF，扫描新的套利机会
        opportunities = scan_arbitrage_opportunities()
        if opportunities:
            # 选择第一个机会（可以修改为选择最佳机会）
            opportunity = opportunities[0]
            
            # 推送套利机会
            message = f"【套利机会】\n"
            message += f"ETF代码：{opportunity['etf_code']}\n"
            message += f"ETF名称：{opportunity['etf_name']}\n"
            message += f"溢价率：{opportunity['premium_rate']:.2f}%\n"
            message += f"当前价格：{opportunity['current_price']:.4f}\n"
            message += f"净值：{opportunity['nav']:.4f}\n"
            message += f"止盈目标：{opportunity['target_price']:.4f}\n"
            message += f"止损价格：{opportunity['stop_loss_price']:.4f}\n"
            message += "建议：立即买入，目标止盈，严格止损。"
            
            send_wecom_message(message)
            
            # 更新套利状态
            current_time = get_beijing_time().isoformat()
            update_arbitrage_status(
                opportunity['etf_code'],
                opportunity['etf_name'],
                current_time,
                opportunity['current_price'],
                opportunity['target_price'],
                opportunity['stop_loss_price']
            )
    
    # ================== 套利扫描逻辑结束 ==================
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/update_stock_pool', methods=['GET', 'POST'])
def cron_update_stock_pool():
    """股票池更新任务"""
    logger.info("股票池更新任务触发")
    
    # 检查是否为周五
    if datetime.datetime.now().weekday() != 4:  # 周五
        logger.info("今天不是周五，跳过股票池更新")
        response = {"status": "skipped", "message": "Not Friday"}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否在16:00之后
    current_time = datetime.datetime.now().time()
    if current_time < datetime.time(16, 0):
        logger.info("时间未到16:00，跳过股票池更新")
        response = {"status": "skipped", "message": "Before 16:00"}
        return jsonify(response) if has_app_context() else response
    
    # 调用核心股票池更新函数
    result = update_stock_pool()
    
    if result is None:
        response = {"status": "skipped", "message": "No valid ETFs found"}
        return jsonify(response) if has_app_context() else response
    
    response = {"status": "success", "message": "Stock pool updated"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/push_strategy', methods=['GET', 'POST'])
def cron_push_strategy():
    """策略推送任务"""
    logger.info("策略推送任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过策略推送")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 调用核心策略计算函数
    success = push_strategy_results()
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/arbitrage_scan', methods=['GET', 'POST'])
def cron_arbitrage_scan():
    """套利扫描任务"""
    logger.info("套利扫描任务触发")
    
    # 实际实现中会调用套利扫描函数
    opportunities = scan_arbitrage_opportunities()
    
    if opportunities:
        # 选择第一个机会
        opportunity = opportunities[0]
        
        # 推送套利机会
        message = f"【套利机会】\n"
        message += f"ETF代码：{opportunity['etf_code']}\n"
        message += f"ETF名称：{opportunity['etf_name']}\n"
        message += f"溢价率：{opportunity['premium_rate']:.2f}%\n"
        message += f"当前价格：{opportunity['current_price']:.4f}\n"
        message += f"净值：{opportunity['nav']:.4f}\n"
        message += f"止盈目标：{opportunity['target_price']:.4f}\n"
        message += f"止损价格：{opportunity['stop_loss_price']:.4f}\n"
        message += "建议：立即买入，目标止盈，严格止损。"
        
        send_wecom_message(message)
        
        # 更新套利状态
        current_time = get_beijing_time().isoformat()
        update_arbitrage_status(
            opportunity['etf_code'],
            opportunity['etf_name'],
            current_time,
            opportunity['current_price'],
            opportunity['target_price'],
            opportunity['stop_loss_price']
        )
        
        response = {"status": "success", "message": "Arbitrage opportunity found"}
    else:
        response = {"status": "success", "message": "No arbitrage opportunity found"}
    
    return jsonify(response) if has_app_context() else response

@app.route('/cron/cleanup', methods=['GET', 'POST'])
def cron_cleanup():
    """数据清理任务"""
    logger.info("数据清理任务触发")
    
    # 实际实现中会调用数据清理函数
    cleanup_old_data()
    
    response = {"status": "success", "message": "Old data cleaned"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/new-stock-info', methods=['GET', 'POST'])
def cron_new_stock_info():
    """定时推送新股信息（当天可申购的新股）和新上市交易股票信息"""
    logger.info("新股信息与新上市交易股票信息推送任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过新股信息推送")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否已经推送过
    if is_new_stock_info_pushed():
        logger.info("新股信息已推送，跳过")
        response = {"status": "skipped", "message": "Already pushed"}
        return jsonify(response) if has_app_context() else response
    
    # 推送新股申购信息
    success_subscriptions = push_new_stock_info()
    
    # 如果新股申购信息推送成功，等待1分钟再推送新上市交易股票信息
    if success_subscriptions:
        time.sleep(60)  # 等待1分钟
        
        # 推送新上市交易股票信息
        success_listings = push_listing_info()
        
        # 标记已推送
        if success_subscriptions and success_listings:
            mark_new_stock_info_pushed()
            mark_listing_info_pushed()
            response = {"status": "success", "message": "New stock info and listings pushed"}
            return jsonify(response) if has_app_context() else response
        elif success_subscriptions:
            logger.warning("新上市交易股票信息推送失败，但新股申购信息已成功推送")
            mark_new_stock_info_pushed()
            response = {"status": "partial_success", "message": "New stock subscriptions pushed, listings failed"}
            return jsonify(response) if has_app_context() else response
    else:
        logger.error("新股申购信息推送失败")
        response = {"status": "error", "message": "Failed to push new stock subscriptions"}
        return jsonify(response) if has_app_context() else response

@app.route('/cron/retry-push', methods=['GET', 'POST'])
def cron_retry_push():
    """交易时间段内每30分钟检查是否需要重试推送"""
    logger.info("重试推送任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过重试推送")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 执行重试
    retry_push()
    
    response = {"status": "success", "message": "Retry push completed"}
    return jsonify(response) if has_app_context() else response

# ========== 测试端点 ==========

@app.route('/test/message', methods=['GET'])
def test_message():
    """T01: 测试消息推送"""
    # 判断是否是测试请求
    test = _is_test_request()
    
    # 获取当前北京时间
    beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
    
    # 构建测试消息
    message = f"CF系统时间：{beijing_time}\n这是来自鱼盆ETF系统的测试消息。\nT01: 测试消息推送。"
    
    # 如果是测试请求，添加测试标识
    if test:
        message = f"【测试消息】\n{message}"
    
    # 发送消息并记录结果
    logger.info(f"准备发送测试消息: {message[:100]}...")
    success = send_wecom_message(message)
    
    if success:
        logger.info("测试消息发送成功")
        response = {"status": "success", "message": "Test message sent"}
        return jsonify(response) if has_app_context() else response
    else:
        logger.error("测试消息发送失败")
        response = {"status": "error", "message": "Failed to send test message"}
        return jsonify(response) if has_app_context() else response

@app.route('/test/strategy', methods=['GET'])
def test_strategy():
    """T02: 测试策略执行（仅返回结果）"""
    test = _is_test_request()
    
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        response = {"status": "error", "message": "No stock pool available"}
        return jsonify(response) if has_app_context() else response
    
    results = []
    for _, etf in stock_pool.iterrows():
        etf_type = 'stable' if etf['type'] == '稳健仓' else 'aggressive'
        signal = calculate_strategy(etf['code'], etf['name'], etf_type)
        results.append({
            'code': etf['code'],
            'name': etf['name'],
            'action': signal['action'],
            'position': signal['position'],
            'total_score': signal['total_score'],
            'rationale': signal['rationale']
        })
    
    response = {"status": "success", "results": results}
    return jsonify(response) if has_app_context() else response

@app.route('/test/trade-log', methods=['GET'])
def test_trade_log():
    """T03: 打印交易流水"""
    test = _is_test_request()
    
    try:
        # 获取所有交易日志文件
        log_files = sorted([f for f in os.listdir(Config.TRADE_LOG_DIR) if f.startswith('trade_log_')])
        if not log_files:
            response = {"status": "error", "message": "No trade logs found"}
            return jsonify(response) if has_app_context() else response
        
        # 合并所有交易日志
        all_logs = []
        for log_file in log_files:
            log_path = os.path.join(Config.TRADE_LOG_DIR, log_file)
            log_df = pd.read_csv(log_path)
            all_logs.extend(log_df.to_dict(orient='records'))
        
        response = {
            "status": "success", 
            "trade_log": all_logs,
            "total_records": len(all_logs),
            "file_count": len(log_files)
        }
        return jsonify(response) if has_app_context() else response
    except Exception as e:
        logger.error(f"获取交易流水失败: {str(e)}")
        response = {"status": "error", "message": str(e)}
        return jsonify(response) if has_app_context() else response

@app.route('/test/stock-pool', methods=['GET'])
def test_stock_pool():
    """T04: 手动推送当前股票池"""
    test = _is_test_request()
    
    stock_pool = get_current_stock_pool()
    if stock_pool is None:
        response = {"status": "error", "message": "No stock pool available"}
        return jsonify(response) if has_app_context() else response
    
    # 格式化消息
    message = _format_stock_pool_message(stock_pool, test=test)
    
    # 发送消息
    send_wecom_message(_format_message(message, test=test))
    response = {"status": "success", "message": "Stock pool pushed"}
    return jsonify(response) if has_app_context() else response

@app.route('/test/execute', methods=['GET'])
def test_execute():
    """T05: 执行策略并推送结果"""
    test = _is_test_request()
    
    # 调用核心策略计算函数
    success = push_strategy_results(test=test)
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

@app.route('/test/reset', methods=['GET'])
def test_reset():
    """T06: 重置所有仓位（测试用）"""
    test = _is_test_request()
    
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        response = {"status": "error", "message": "No stock pool available"}
        return jsonify(response) if has_app_context() else response
    
    beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
    for _, etf in stock_pool.iterrows():
        # 创建重置信号
        etf_type = 'stable' if etf['type'] == '稳健仓' else 'aggressive'
        signal = {
            'etf_code': etf['code'],
            'etf_name': etf['name'],
            'cf_time': beijing_time,
            'action': 'strong_sell',
            'position': 0,
            'rationale': '测试重置仓位'
        }
        
        # 格式化消息
        message = _format_strategy_signal(signal, test=test)
        
        # 推送消息
        send_wecom_message(_format_message(message, test=test))
        
        # 记录交易
        if not test:
            log_trade(signal)
        
        # 间隔1分钟
        time.sleep(60)
    
    response = {"status": "success", "message": "All positions reset"}
    return jsonify(response) if has_app_context() else response

@app.route('/test/new-stock', methods=['GET'])
def test_new_stock():
    """T07: 测试推送新股信息（只推送当天可申购的新股）"""
    test = _is_test_request()
    
    # 获取测试用的新股申购信息
    new_stocks = get_test_new_stock_subscriptions()
    
    # 检查是否获取到新股数据
    if new_stocks is None or new_stocks.empty:
        message = "近7天没有新股、新债、新债券可认购"
    else:
        # 使用专用测试消息格式
        message = "【测试新股信息】\n"
        message += f"共发现{len(new_stocks)}只新股：\n"
        for _, row in new_stocks.iterrows():
            message += f"股票代码：{row['code']}\n"
            message += f"股票名称：{row['name']}\n"
            message += f"发行价格：{row.get('issue_price', 'N/A')}元\n"
            message += f"申购上限：{row.get('max_purchase', 'N/A')}股\n"
            message += f"申购日期：{row.get('publish_date', 'N/A')}\n"
            message += "─" * 20 + "\n"
    
    # 添加测试标识
    message = "【测试消息】\n" + message
    
    # 发送消息
    success = send_wecom_message(message)
    
    # 不需要等待2分钟推送新上市信息（手动测试只需新股信息）
    
    # 检查推送结果
    if success:
        logger.info("测试新股信息推送成功")
        response = {"status": "success", "message": "Test new stocks sent"}
        return jsonify(response) if has_app_context() else response
    else:
        logger.error("测试新股信息推送失败")
        response = {"status": "error", "message": "Failed to send test new stocks"}
        return jsonify(response) if has_app_context() else response

@app.route('/test/new-stock-listings', methods=['GET'])
def test_new_stock_listings():
    """T08: 测试推送新上市交易股票信息"""
    test = _is_test_request()
    
    # 获取测试数据
    new_listings = get_test_new_stock_listings()
    
    # 推送新上市交易股票信息
    if new_listings is None or new_listings.empty:
        message = "近7天没有新上市股票、可转债、债券"
    else:
        # 使用专用测试消息格式
        message = "【测试新上市交易信息】\n"
        message += f"共发现{len(new_listings)}只新上市股票：\n"
        for _, row in new_listings.iterrows():
            message += f"股票代码：{row['code']}\n"
            message += f"股票名称：{row['name']}\n"
            message += f"发行价格：{row.get('issue_price', 'N/A')}元\n"
            message += f"申购上限：{row.get('max_purchase', 'N/A')}股\n"
            message += f"上市日期：{row.get('listing_date', 'N/A')}\n"
            message += "─" * 20 + "\n"
    
    # 添加测试标识
    message = "【测试消息】\n" + message
    
    # 发送消息
    success = send_wecom_message(message)
    
    if success:
        response = {"status": "success", "message": "Test new stock listings sent"}
    else:
        response = {"status": "error", "message": "Failed to send test new stock listings"}
    return jsonify(response) if has_app_context() else response

# ========== 新增：测试套利扫描 ==========

@app.route('/test/arbitrage-scan', methods=['GET'])
def test_arbitrage_scan():
    """T09: 测试套利扫描"""
    test = _is_test_request()
    
    # 扫描套利机会
    opportunities = scan_arbitrage_opportunities()
    
    if opportunities:
        # 选择第一个机会
        opportunity = opportunities[0]
        
        # 推送套利机会
        message = f"【测试套利机会】\n"
        message += f"ETF代码：{opportunity['etf_code']}\n"
        message += f"ETF名称：{opportunity['etf_name']}\n"
        message += f"溢价率：{opportunity['premium_rate']:.2f}%\n"
        message += f"当前价格：{opportunity['current_price']:.4f}\n"
        message += f"净值：{opportunity['nav']:.4f}\n"
        message += f"止盈目标：{opportunity['target_price']:.4f}\n"
        message += f"止损价格：{opportunity['stop_loss_price']:.4f}\n"
        message += "建议：立即买入，目标止盈，严格止损。"
        
        success = send_wecom_message(_format_message(message, test=test))
        
        # 更新套利状态（仅测试模式）
        if test:
            current_time = get_beijing_time().isoformat()
            update_arbitrage_status(
                opportunity['etf_code'],
                opportunity['etf_name'],
                current_time,
                opportunity['current_price'],
                opportunity['target_price'],
                opportunity['stop_loss_price']
            )
        
        logger.info("测试套利扫描成功")
        return {"status": "success", "message": "Arbitrage opportunity found"}
    else:
        logger.info("未发现套利机会")
        return {"status": "success", "message": "No arbitrage opportunity found"}

# ========== 辅助函数 ==========

def cleanup_directory(directory, days_to_keep=None):
    """
    清理指定目录中的旧文件
    参数:
        directory: 目录路径
        days_to_keep: 保留天数，None表示不清理
    """
    if days_to_keep is None:
        logger.info(f"跳过目录清理: {directory} (永久保存)")
        return
    
    now = datetime.datetime.now()
    
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        
        # 获取文件修改时间
        file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
        
        # 计算文件年龄
        file_age = now - file_time
        
        # 如果文件年龄大于指定天数，删除文件
        if file_age.days > days_to_keep:
            try:
                os.remove(filepath)
                logger.info(f"已删除旧文件: {filepath}")
            except Exception as e:
                logger.error(f"删除文件失败 {filepath}: {str(e)}")

def cleanup_old_data():
    """清理旧数据，交易流水永久保存，其他数据保留指定天数"""
    logger.info("开始清理旧数据...")
    
    # 清理原始数据
    cleanup_directory(Config.RAW_DATA_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 清理股票池
    cleanup_directory(Config.STOCK_POOL_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 交易流水永久保存，不清理
    logger.info("交易流水目录不清理，永久保存")
    
    # 清理套利数据
    cleanup_directory(Config.ARBITRAGE_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 清理错误日志
    cleanup_directory(Config.ERROR_LOG_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 清理新股数据
    cleanup_directory(Config.NEW_STOCK_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    logger.info("数据清理完成")
    return True

# ========== 任务执行入口 ==========

def run_task(task):

    logger.info(f"接收到的任务参数: {task}")
    
    # 添加获取环境变量的日志
    env_task = os.getenv('TASK', '未设置')
    logger.info(f"环境变量TASK的值: {env_task}")
  
    """执行指定任务（用于GitHub Actions）"""
    logger.info(f"开始执行任务: {task}")
    
    try:
        if task == 'test_message':
            # T01: 测试消息推送
            # 手动触发时不需要检查是否为测试请求
            beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
            message = f"【测试消息】\nT01: 测试消息推送\nCF系统时间：{beijing_time}\n这是来自鱼盆ETF系统的测试消息。"
            success = send_wecom_message(message)
            return {"status": "success" if success else "error", "message": "Test message sent"}
        
        elif task == 'test_new_stock':
            # T07: 测试推送新股信息
            logger.info("执行测试新股信息推送任务")
            
            # 获取测试用的新股申购信息
            new_stocks = get_test_new_stock_subscriptions()
            
            # 检查是否获取到新股数据
            if new_stocks is None or new_stocks.empty:
                message = "【测试】近7天没有新股、新债、新债券可认购"
            else:
                # 使用专用测试消息格式
                message = "【测试新股信息】\n"
                message += f"共发现{len(new_stocks)}只新股：\n"
                for _, row in new_stocks.iterrows():
                    message += f"股票代码：{row['code']}\n"
                    message += f"股票名称：{row['name']}\n"
                    message += f"发行价格：{row.get('issue_price', 'N/A')}元\n"
                    message += f"申购上限：{row.get('max_purchase', 'N/A')}股\n"
                    message += f"申购日期：{row.get('publish_date', 'N/A')}\n"
                    message += "─" * 20 + "\n"
            
            # 添加测试标识
            message = "【测试消息】\n" + message
            
            # 发送消息
            success = send_wecom_message(message)
            
            if success:
                logger.info("测试新股信息推送成功")
                return {"status": "success", "message": "Test new stocks sent"}
            else:
                logger.error("测试新股信息推送失败")
                return {"status": "error", "message": "Failed to send test new stocks"}
        
        elif task == 'test_stock_pool':
            # T04: 测试推送当前股票池
            logger.info("执行测试股票池推送任务")
            # 获取当前股票池
            stock_pool = get_current_stock_pool()
            
            if stock_pool is None or stock_pool.empty:
                logger.error("股票池为空，强制更新股票池")
       
                # 先强制更新股票池
                update_stock_pool()
                time.sleep(10)  # 等待更新完成
                
                # 再次获取当前股票池
                stock_pool = get_current_stock_pool()

                if stock_pool is None or stock_pool.empty:
                    logger.error("股票池为空，无法推送")
                    return {"status": "error", "message": "No stock pool available"}
            
            # 格式化消息
            message = "【测试消息】\n" + _format_stock_pool_message(stock_pool, test=True)
            
            # 发送消息
            success = send_wecom_message(message)
            
            if success:
                logger.info("测试股票池推送成功")
                return {"status": "success", "message": "Stock pool pushed"}
            else:
                logger.error("测试股票池推送失败")
                return {"status": "error", "message": "Failed to push stock pool"}
        
        elif task == 'test_execute':
            # T05: 执行策略并推送结果
            logger.info("执行测试策略推送任务")
            # 调用核心策略计算函数
            success = push_strategy_results(test=True)
            
            if success:
                logger.info("测试策略推送成功")
                return {"status": "success", "message": "Strategy executed"}
            else:
                logger.error("测试策略推送失败")
                return {"status": "error", "message": "Failed to execute strategy"}
        
        elif task == 'test_reset':
            # T06: 重置所有仓位
            logger.info("执行测试重置仓位任务")
            # 获取当前股票池
            stock_pool = get_current_stock_pool()
            
            if stock_pool is None or stock_pool.empty:
                logger.error("股票池为空，无法重置仓位")
                return {"status": "error", "message": "No stock pool available"}
            
            # 创建重置信号
            beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
            for _, etf in stock_pool.iterrows():
                etf_type = 'stable' if etf['type'] == '稳健仓' else 'aggressive'
                signal = {
                    'etf_code': etf['code'],
                    'etf_name': etf['name'],
                    'cf_time': beijing_time,
                    'action': 'strong_sell',
                    'position': 0,
                    'rationale': '测试重置仓位'
                }
                
                # 格式化消息
                message = "【测试消息】\n" + _format_strategy_signal(signal, test=True)
                
                # 推送消息
                send_wecom_message(message)
                
                # 记录交易
                log_trade(signal)
                
                # 间隔1分钟
                time.sleep(60)
            
            logger.info("测试重置仓位完成")
            return {"status": "success", "message": "All positions reset"}
        
        elif task == 'test_arbitrage':
            # T09: 测试套利扫描
            logger.info("执行测试套利扫描任务")
            
            # 扫描套利机会
            opportunities = scan_arbitrage_opportunities()
            
            if opportunities:
                # 选择第一个机会
                opportunity = opportunities[0]
                
                # 推送套利机会
                message = f"【测试套利机会】\n"
                message += f"ETF代码：{opportunity['etf_code']}\n"
                message += f"ETF名称：{opportunity['etf_name']}\n"
                message += f"溢价率：{opportunity['premium_rate']:.2f}%\n"
                message += f"当前价格：{opportunity['current_price']:.4f}\n"
                message += f"净值：{opportunity['nav']:.4f}\n"
                message += f"止盈目标：{opportunity['target_price']:.4f}\n"
                message += f"止损价格：{opportunity['stop_loss_price']:.4f}\n"
                message += "建议：立即买入，目标止盈，严格止损。"
                
                success = send_wecom_message(message)
                
                # 更新套利状态
                if success:
                    current_time = get_beijing_time().isoformat()
                    update_arbitrage_status(
                        opportunity['etf_code'],
                        opportunity['etf_name'],
                        current_time,
                        opportunity['current_price'],
                        opportunity['target_price'],
                        opportunity['stop_loss_price']
                    )
                
                logger.info("测试套利扫描成功")
                return {"status": "success", "message": "Arbitrage opportunity found"}
            else:
                logger.info("未发现套利机会")
                return {"status": "success", "message": "No arbitrage opportunity found"}
        
        elif task == 'run_new_stock_info':
            # 每日 9:35 新股信息推送
            return cron_new_stock_info()
        
        elif task == 'push_strategy':
            # 每日 14:50 策略信号推送
            return cron_push_strategy()
        
        elif task == 'update_stock_pool':
            # 每周五 16:00 更新股票池
            if datetime.datetime.now().weekday() == 4:  # 周五
                return cron_update_stock_pool()
            else:
                logger.info("今天不是周五，跳过股票池更新")
                return {"status": "skipped", "message": "Not Friday"}
        
        elif task == 'crawl_daily':
            # 每日 15:30 爬取日线数据
            return cron_crawl_daily()
        
        elif task == 'crawl_intraday':
            # 盘中数据爬取（每30分钟）
            return cron_crawl_intraday()
        
        elif task == 'cleanup':
            # 每天 00:00 清理旧数据
            return cron_cleanup()
        
        elif task == 'retry_push':
            # 交易时间段内每30分钟检查是否需要重试推送
            retry_push()
            return {"status": "success", "message": "Retry push completed"}
        
        else:
            logger.warning(f"未知任务: {task}")
            return {"status": "error", "message": "Unknown task"}
    
    except Exception as e:
        logger.critical(f"任务执行失败: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    # 用于GitHub Actions执行任务
    task = os.getenv('TASK', 'run_new_stock_info')
    
    if task.startswith('test_') or task in ['run_new_stock_info', 'push_strategy', 'update_stock_pool', 'crawl_daily', 'cleanup', 'retry_push', 'crawl_intraday']:
        # 执行任务
        result = run_task(task)
        logger.info(f"任务执行结果: {result}")
    else:
        # 启动Web服务
        app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
