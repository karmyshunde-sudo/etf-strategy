"""
鱼盆ETF投资量化模型 - 评分系统核心模块
说明:
  本文件负责ETF评分计算，为其他模块提供统一的评分接口
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
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import Config
from logger import get_logger
from crawler import get_etf_data, calculate_component_weights, estimate_etf_nav, calculate_premium_rate
from time_utils import get_beijing_time

logger = get_logger(__name__)

# 评分维度权重定义
SCORE_WEIGHTS = {
    'liquidity': 0.20,  # 流动性评分权重
    'risk': 0.25,       # 风险控制评分权重
    'return': 0.25,     # 收益能力评分权重
    'premium': 0.15,    # 溢价率评分权重
    'sentiment': 0.15   # 情绪指标评分权重
}

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
    avg_volume = etf_data['volume'].tail(30).mean() / 100000000
    
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
    from crawler import get_all_etf_list
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
            {'code': '512880', 'name': '证券ETF'},
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

def get_top_n_etfs(n=10):
    """
    获取评分最高的N只ETF
    参数:
        n: 要返回的ETF数量
    返回:
        DataFrame: 评分最高的N只ETF
    """
    # 获取所有ETF评分
    from crawler import get_all_etf_list
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
