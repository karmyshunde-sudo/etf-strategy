"""
鱼盆ETF投资量化模型 - 股票池管理模块
说明:
  本文件负责ETF股票池的更新和管理
  保持10只ETF：5只稳健仓，5只激进仓
  所有文件放在根目录，简化导入关系
"""

import os
import time
import pandas as pd
from datetime import datetime
from config import Config
from logger import get_logger
from crawler import get_etf_data, get_all_etf_list, calculate_etf_score
from time_utils import convert_to_beijing_time

logger = get_logger(__name__)

def calculate_liquidity_score(etf_data, etf_type='broad'):
    """
    计算流动性评分（30%权重）
    参数:
        etf_ ETF数据
        etf_type: 'broad'为宽基ETF，'sector'为行业ETF
    返回:
        float: 流动性评分（0-10）
    """
    # 计算必要的指标
    daily_volume = etf_data['volume'].mean() / 100000000  # 转换为亿元
    latest_price = etf_data['close'].iloc[-1]
    
    # 日均成交额（10%）
    daily_volume_score = min(10, max(0, (daily_volume - 2) / 0.3))
    
    # 买卖价差（8%）
    # 实际实现中，应从盘口数据获取
    bid_ask_spread = 0.08  # 模拟值，实际应计算
    spread_score = max(0, 8 - (bid_ask_spread - 0.05) * 50)
    
    # 市场冲击成本（7%）
    # 实际实现中，应通过模拟大额交易计算
    impact_cost = 0.15  # 模拟值
    impact_score = max(0, 7 - (impact_cost - 0.1) * 17.5)
    
    # 换手率（5%）
    # 计算过去30天平均换手率
    turnover = etf_data['volume'].mean() / etf_data['close'].mean() * 100
    turnover_score = 5 * (1 - min(1, abs(turnover - 6.5) / 5.5))
    
    # 计算加权流动性评分
    liquidity_score = (
        daily_volume_score * 0.10 +
        spread_score * 0.08 +
        impact_score * 0.07 +
        turnover_score * 0.05
    )
    
    return liquidity_score

def calculate_risk_score(etf_data):
    """
    计算风险控制评分（25%权重）
    参数:
        etf_ ETF数据
    返回:
        float: 风险评分（0-10）
    """
    # 计算日收益率
    etf_data['daily_return'] = etf_data['close'].pct_change()
    
    # 历史波动率（8%）
    volatility = etf_data['daily_return'].std() * 252**0.5 * 100
    volatility_score = 8 * (1 - min(1, abs(volatility - 20) / 15))
    
    # 最大回撤（7%）
    window = 252 if len(etf_data) >= 252 else len(etf_data)
    rolling_max = etf_data['close'].rolling(window, min_periods=1).max()
    daily_drawdown = etf_data['close'] / rolling_max - 1.0
    max_drawdown = abs(daily_drawdown.min() * 100)
    drawdown_score = max(0, 7 - (max_drawdown - 15) * 0.35)
    
    # 夏普比率（5%）
    risk_free_rate = 0.02  # 无风险利率2%
    excess_return = etf_data['daily_return'].mean() * 252 - risk_free_rate
    sharpe_ratio = excess_return / (etf_data['daily_return'].std() * 252**0.5)
    sharpe_score = min(5, sharpe_ratio * 5)
    
    # 与指数相关性（5%）
    # 实际实现中，应计算与跟踪指数的相关系数
    correlation = 0.92  # 模拟值
    correlation_score = max(0, 5 - (0.95 - correlation) * 33.3)
    
    # 计算加权风险评分
    risk_score = (
        volatility_score * 0.08 +
        drawdown_score * 0.07 +
        sharpe_score * 0.05 +
        correlation_score * 0.05
    )
    
    return risk_score

def calculate_return_score(etf_data):
    """
    计算收益能力评分（20%权重）
    参数:
        etf_ ETF数据
    返回:
        float: 收益评分（0-10）
    """
    # 计算各时间周期收益率
    returns = {
        '1w': etf_data['close'].pct_change(5).iloc[-1] * 100,
        '1m': etf_data['close'].pct_change(20).iloc[-1] * 100,
        '3m': etf_data['close'].pct_change(60).iloc[-1] * 100,
        '6m': etf_data['close'].pct_change(120).iloc[-1] * 100
    }
    
    # 近期收益率（8%）
    return_score = min(8, (
        returns['1w'] * 0.1 + 
        returns['1m'] * 0.2 + 
        returns['3m'] * 0.3 + 
        returns['6m'] * 0.4
    ) * 0.8)  # 简化计算
    
    # 收益稳定性（6%）
    # 计算过去12个月正收益月数
    monthly_returns = etf_data['close'].resample('M').last().pct_change()
    positive_months = (monthly_returns > 0).sum()
    stability_score = max(0, 6 - (75 - positive_months * 100 / 12) * 0.24)
    
    # 超额收益（4%）
    # 与跟踪指数比较
    excess_return = returns['6m'] - 5.0  # 模拟超额收益
    excess_score = max(0, min(4, (excess_return + 1) * 2))
    
    # 趋势强度（2%）
    price = etf_data['close'].iloc[-1]
    ma20 = etf_data['close'].rolling(20).mean().iloc[-1]
    trend_score = 2 * min(1, max(0, (price / ma20 - 1) * 10))
    
    # 计算加权收益评分
    return_score = (
        return_score * 0.08 +
        stability_score * 0.06 +
        excess_score * 0.04 +
        trend_score * 0.02
    )
    
    return return_score

def calculate_fundamental_score(etf_data):
    """
    计算基本面评分（15%权重）
    参数:
        etf_ ETF数据
    返回:
        float: 基本面评分（0-10）
    """
    # 规模(AUM)（5%）
    # 实际实现中，应从ETF基本信息获取
    aum = 100  # 模拟值（亿元）
    aum_score = 5 * (1 - min(1, abs(aum - 175) / 155))
    
    # 跟踪误差（4%）
    # 实际实现中，应计算与跟踪指数的跟踪误差
    tracking_error = 0.8  # 模拟值（%）
    tracking_score = max(0, 4 - (tracking_error - 0.5) * 2.67)
    
    # 管理费（3%）
    # 实际实现中，应从ETF基本信息获取
    management_fee = 0.5  # 模拟值（%）
    fee_score = max(0, 3 - (management_fee - 0.2) * 7.5)
    
    # 集中度（3%）
    # 实际实现中，应计算前10大成分股权重之和
    concentration = 60  # 模拟值（%）
    concentration_score = 3 * (1 - min(1, abs(concentration - 60) / 30))
    
    # 计算加权基本面评分
    fundamental_score = (
        aum_score * 0.05 +
        tracking_score * 0.04 +
        fee_score * 0.03 +
        concentration_score * 0.03
    )
    
    return fundamental_score

def calculate_sentiment_score(etf_data):
    """
    计算市场情绪评分（10%权重）
    参数:
        etf_ ETF数据
    返回:
        float: 情绪评分（0-10）
    """
    # 融资余额变化（3%）
    # 实际实现中，应从数据源获取融资余额数据
    margin_change = 5  # 模拟值（%）
    margin_score = 3 * (1 - min(1, abs(margin_change - 5) / 15))
    
    # 北向资金（2.5%）
    # 实际实现中，应计算成分股北向资金流入情况
    north_bound = 0.3  # 模拟值（%）
    north_score = min(2.5, north_bound * 2.5)
    
    # 隐含波动率（2.5%）
    # 实际实现中，应从期权市场获取
    iv = 20  # 模拟值（%）
    iv_score = 2.5 * (1 - min(1, abs(iv - 20) / 20))
    
    # 社交媒体情绪（2%）
    # 实际实现中，应分析财经新闻和社交媒体
    social_sentiment = 0.6  # 模拟值（0-1）
    social_score = min(2, social_sentiment * 2)
    
    # 计算加权情绪评分
    sentiment_score = (
        margin_score * 0.03 +
        north_score * 0.025 +
        iv_score * 0.025 +
        social_score * 0.02
    )
    
    return sentiment_score

def calculate_etf_score(etf_code, etf_name, etf_type):
    """
    计算ETF在五个维度的综合评分
    参数:
        etf_code: ETF代码
        etf_name: ETF名称
        etf_type: 'broad'或'sector'
    返回:
        dict: ETF在各维度的评分
    """
    # 获取ETF数据
    daily_data = get_etf_data(etf_code, 'daily')
    if daily_data is None or daily_data.empty:
        logger.error(f"获取{etf_code}数据失败")
        return None
    
    # 计算各维度评分
    liquidity_score = calculate_liquidity_score(daily_data, etf_type)
    risk_score = calculate_risk_score(daily_data)
    return_score = calculate_return_score(daily_data)
    fundamental_score = calculate_fundamental_score(daily_data)
    sentiment_score = calculate_sentiment_score(daily_data)
    
    # 计算总评分
    total_score = (
        liquidity_score * 0.30 +
        risk_score * 0.25 +
        return_score * 0.20 +
        fundamental_score * 0.15 +
        sentiment_score * 0.10
    )
    
    return {
        'code': etf_code,
        'name': etf_name,
        'type': etf_type,
        'liquidity_score': round(liquidity_score, 1),
        'risk_score': round(risk_score, 1),
        'return_score': round(return_score, 1),
        'fundamental_score': round(fundamental_score, 1),
        'sentiment_score': round(sentiment_score, 1),
        'total_score': round(total_score, 1)
    }

def update_stock_pool():
    """
    更新ETF股票池（5只稳健ETF和5只激进ETF）
    本函数应在每周五16:00北京时间运行
    """
    logger.info("开始股票池更新流程")
    
    # 检查今天是否是周五
    beijing_now = convert_to_beijing_time(datetime.now())
    if beijing_now.weekday() != 4:  # 周五是4（周一是0）
        logger.info(f"今天是{beijing_now.strftime('%A')}，不是周五。跳过股票池更新。")
        return None
    
    # 检查时间是否在16:00之后
    if beijing_now.time() < datetime.time(16, 0):
        logger.info(f"当前时间是{beijing_now.strftime('%H:%M')}，早于16:00。跳过股票池更新。")
        return None
    
    # 获取所有ETF
    etf_list = get_all_etf_list()
    logger.info(f"找到{len(etf_list)}只ETF进行评估")
    
    # 计算所有ETF的评分
    scored_etfs = []
    for _, etf in etf_list.iterrows():
        etf_type = 'sector' if etf['code'] in ['512480', '512660', '512980', '159825', '159995'] else 'broad'
        score = calculate_etf_score(etf['code'], etf['name'], etf_type)
        if score:
            scored_etfs.append(score)
        time.sleep(1)  # 温和对待API
    
    if not scored_etfs:
        logger.error("未计算出有效的ETF评分。无法更新股票池。")
        return None
    
    # 转换为DataFrame以便处理
    scores_df = pd.DataFrame(scored_etfs)
    
    # 筛选和选择稳健ETF（宽基）
    stable_etfs = scores_df[scores_df['type'] == 'broad']
    # 应用最低评分阈值
    stable_etfs = stable_etfs[
        (stable_etfs['liquidity_score'] >= 6.0) &
        (stable_etfs['risk_score'] >= 5.0) &
        (stable_etfs['total_score'] >= 7.0)
    ]
    
    if not stable_etfs.empty:
        # 按风险评分排序（高分优先，稳健ETF风险越低越好）
        stable_etfs = stable_etfs.sort_values('risk_score', ascending=False)
        # 选择前5名
        selected_stable = stable_etfs.head(5)
    else:
        logger.warning("没有符合标准的稳健ETF。使用备用ETF。")
        # 备用ETF（宽基）
        fallback_stable = [
            {'code': '510050', 'name': '上证50ETF', 'type': 'broad'},
            {'code': '510300', 'name': '沪深300ETF', 'type': 'broad'},
            {'code': '510500', 'name': '中证500ETF', 'type': 'broad'},
            {'code': '159919', 'name': '300SCIE', 'type': 'broad'},
            {'code': '515790', 'name': '光伏ETF', 'type': 'broad'}
        ]
        # 获取备用ETF的评分
        selected_stable = pd.DataFrame([
            calculate_etf_score(etf['code'], etf['name'], etf['type'])
            for etf in fallback_stable
        ])
    
    # 筛选和选择激进ETF（行业）
    aggressive_etfs = scores_df[scores_df['type'] == 'sector']
    # 应用最低评分阈值
    aggressive_etfs = aggressive_etfs[
        (aggressive_etfs['liquidity_score'] >= 5.5) &
        (aggressive_etfs['return_score'] - aggressive_etfs['risk_score'] >= 0)
    ]
    
    if not aggressive_etfs.empty:
        # 按(收益-风险)差值排序（高分优先，激进ETF收益风险比越高越好）
        aggressive_etfs['return_risk_diff'] = aggressive_etfs['return_score'] - aggressive_etfs['risk_score']
        aggressive_etfs = aggressive_etfs.sort_values('return_risk_diff', ascending=False)
        # 选择前5名
        selected_aggressive = aggressive_etfs.head(5)
    else:
        logger.warning("没有符合标准的激进ETF。使用备用ETF。")
        # 备用ETF（行业）
        fallback_aggressive = [
            {'code': '512480', 'name': '半导体ETF', 'type': 'sector'},
            {'code': '512660', 'name': '军工ETF', 'type': 'sector'},
            {'code': '512880', 'name': '证券ETF', 'type': 'sector'},
            {'code': '512980', 'name': '通信ETF', 'type': 'sector'},
            {'code': '159995', 'name': '芯片ETF', 'type': 'sector'}
        ]
        # 获取备用ETF的评分
        selected_aggressive = pd.DataFrame([
            calculate_etf_score(etf['code'], etf['name'], etf['type'])
            for etf in fallback_aggressive
        ])
    
    # 合并选定的ETF
    stock_pool = pd.concat([selected_stable, selected_aggressive])
    
    # 准备最终股票池DataFrame
    final_pool = pd.DataFrame({
        'type': ['稳健仓'] * 5 + ['激进仓'] * 5,
        'code': stock_pool['code'].tolist(),
        'name': stock_pool['name'].tolist(),
        'total_score': stock_pool['total_score'].tolist(),
        'update_time': [beijing_now.strftime('%Y-%m-%d %H:%M')] * 10,
        'liquidity_score': stock_pool['liquidity_score'].tolist(),
        'risk_score': stock_pool['risk_score'].tolist(),
        'return_score': stock_pool['return_score'].tolist(),
        'fundamental_score': stock_pool['fundamental_score'].tolist(),
        'sentiment_score': stock_pool['sentiment_score'].tolist()
    })
    
    # 保存到数据目录
    filename = f"stock_pool_{beijing_now.strftime('%Y%m%d')}.csv"
    filepath = os.path.join(Config.STOCK_POOL_DIR, filename)
    final_pool.to_csv(filepath, index=False)
    
    logger.info(f"股票池更新成功。保存为: {filename}")
    logger.info(f"选定{len(selected_stable)}只稳健ETF和{len(selected_aggressive)}只激进ETF")
    
    return final_pool

def get_current_stock_pool():
    """
    获取当前有效的股票池
    返回:
        DataFrame: 当前股票池
    """
    # 获取最新股票池文件
    try:
        pool_files = [f for f in os.listdir(Config.STOCK_POOL_DIR) if f.startswith('stock_pool_')]
        if not pool_files:
            logger.error("未找到股票池文件")
            return None
        
        # 获取最新文件
        latest_file = max(pool_files)
        return pd.read_csv(os.path.join(Config.STOCK_POOL_DIR, latest_file))
    except Exception as e:
        logger.error(f"加载股票池错误: {str(e)}")
        return None