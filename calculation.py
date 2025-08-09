"""
鱼盆ETF投资量化模型 - 策略计算模块
说明:
  本文件负责生成交易信号和策略建议
  所有文件放在根目录，简化导入关系
"""

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime
from config import Config
from logger import get_logger
from time_utils import convert_to_beijing_time
from crawler import get_etf_data, calculate_premium_rate
from stock_pool import get_current_stock_pool
from wecom import send_wecom_message

logger = get_logger(__name__)

def analyze_trend(etf_data):
    """
    使用多周期均线分析价格趋势
    参数:
        etf_ ETF历史数据
    返回:
        str: 趋势信号 (strong_buy, buy, hold, sell, strong_sell)
        float: 趋势强度 (1-5)
    """
    if etf_data is None or etf_data.empty:
        return 'hold', 3.0
    
    # 计算移动平均线
    etf_data['ma5'] = etf_data['close'].rolling(window=5).mean()
    etf_data['ma10'] = etf_data['close'].rolling(window=10).mean()
    etf_data['ma20'] = etf_data['close'].rolling(window=20).mean()
    etf_data['ma60'] = etf_data['close'].rolling(window=60).mean()
    
    # 获取最新值
    latest = etf_data.iloc[-1]
    prev = etf_data.iloc[-2] if len(etf_data) > 1 else None
    
    # 检查金叉（短期均线上穿长期均线）
    golden_cross = (
        latest['ma5'] > latest['ma10'] > latest['ma20'] > latest['ma60'] and
        (prev is None or not (prev['ma5'] > prev['ma10'] > prev['ma20'] > prev['ma60']))
    )
    
    # 检查死叉（短期均线下穿长期均线）
    death_cross = (
        latest['ma5'] < latest['ma10'] < latest['ma20'] < latest['ma60'] and
        (prev is None or not (prev['ma5'] < prev['ma10'] < prev['ma20'] < prev['ma60']))
    )
    
    # 计算价格相对于20日均线的位置
    price_ma_ratio = latest['close'] / latest['ma20'] if latest['ma20'] else 1.0
    
    # 计算趋势强度 (1-5)
    trend_strength = 3.0  # 默认中性
    
    if golden_cross:
        trend_strength = 4.5  # 强买入信号
    elif death_cross:
        trend_strength = 1.5  # 强卖出信号
    elif price_ma_ratio > 1.05:
        trend_strength = 4.0  # 上升趋势
    elif price_ma_ratio < 0.95:
        trend_strength = 2.0  # 下降趋势
    
    # 确定趋势信号
    if trend_strength >= 4.5:
        trend_signal = 'strong_buy'
    elif trend_strength >= 3.5:
        trend_signal = 'buy'
    elif trend_strength >= 2.5:
        trend_signal = 'hold'
    elif trend_strength >= 1.5:
        trend_signal = 'sell'
    else:
        trend_signal = 'strong_sell'
    
    return trend_signal, trend_strength

def analyze_volume(etf_data):
    """
    分析成交量确认
    参数:
        etf_ ETF历史数据
    返回:
        str: 成交量确认 (confirmed, neutral, weak)
        float: 量能强度 (1-3)
    """
    if etf_data is None or etf_data.empty or len(etf_data) < 6:
        return 'neutral', 2.0
    
    # 计算平均成交量
    avg_volume = etf_data['volume'].tail(5).mean()
    latest_volume = etf_data['volume'].iloc[-1]
    
    # 计算成交量比率
    volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0
    
    # 确定量能确认
    if volume_ratio > 1.5:
        volume_confirmation = 'confirmed'
        volume_strength = 3.0
    elif volume_ratio > 1.2:
        volume_confirmation = 'neutral'
        volume_strength = 2.0
    else:
        volume_confirmation = 'weak'
        volume_strength = 1.0
    
    return volume_confirmation, volume_strength

def assess_risk(etf_data, etf_type):
    """
    评估风险水平
    参数:
        etf_ ETF历史数据
        etf_type: 'stable'或'aggressive'
    返回:
        str: 风险水平 (safe, warning, danger)
        float: 风险评分 (1-3)
    """
    if etf_data is None or etf_data.empty:
        return 'warning', 2.0
    
    # 计算日收益率
    etf_data['daily_return'] = etf_data['close'].pct_change()
    
    # 计算30天波动率(年化)
    volatility_30d = etf_data['daily_return'].tail(30).std() * np.sqrt(252) * 100 if len(etf_data) >= 30 else 20.0
    
    # 计算最大回撤(1年)
    window = 252 if len(etf_data) >= 252 else len(etf_data)
    rolling_max = etf_data['close'].rolling(window, min_periods=1).max()
    daily_drawdown = etf_data['close'] / rolling_max - 1.0
    max_drawdown = abs(daily_drawdown.min() * 100)
    
    # 评估个体风险
    individual_risk = 'safe'
    risk_score = 3.0
    
    # 对稳健ETF应用更严格规则
    if etf_type == 'stable':
        if volatility_30d > 25.0 or max_drawdown > 25.0:
            individual_risk = 'warning'
            risk_score = 2.0
        if volatility_30d > 30.0 or max_drawdown > 30.0:
            individual_risk = 'danger'
            risk_score = 1.0
    else:  # 激进ETF
        if volatility_30d > 35.0 or max_drawdown > 35.0:
            individual_risk = 'warning'
            risk_score = 2.0
        if volatility_30d > 40.0 or max_drawdown > 40.0:
            individual_risk = 'danger'
            risk_score = 1.0
    
    return individual_risk, risk_score

def assess_valuation(etf_code, etf_data):
    """
    评估估值水平
    参数:
        etf_code: ETF代码
        etf_ ETF历史数据
    返回:
        str: 估值水平 (undervalued, fair, overvalued)
        float: 估值评分 (1-3)
    """
    # 计算当前溢价率
    latest_price = etf_data['close'].iloc[-1] if etf_data is not None and not etf_data.empty else 1.0
    premium_rate = calculate_premium_rate(etf_code, latest_price)
    
    # 确定估值水平
    if premium_rate < -0.8:  # 折价
        valuation = 'undervalued'
        valuation_score = 3.0
    elif premium_rate > 0.8:  # 溢价
        valuation = 'overvalued'
        valuation_score = 1.0
    else:  # 合理估值
        valuation = 'fair'
        valuation_score = 2.0
    
    return valuation, valuation_score

def analyze_sentiment():
    """
    分析市场情绪
    返回:
        str: 情绪水平 (positive, neutral, negative)
        float: 情绪评分 (1-2)
    """
    # 实际实现中，将分析多个情绪指标
    # 这里使用Tushare获取市场情绪数据
    try:
        import tushare as ts
        ts.set_token(Config.TUSHARE_TOKEN)
        
        # 获取市场情绪指标（简化示例）
        # 实际中会获取更多指标
        sentiment_data = ts.pro_api().index_dailybasic(
            trade_date=datetime.now().strftime('%Y%m%d'),
            fields='pe, pb, turnover_rate'
        )
        
        if not sentiment_data.empty:
            # 计算简单情绪评分
            pe = sentiment_data['pe'].mean()
            pb = sentiment_data['pb'].mean()
            
            # 基于PE/PB计算情绪
            if pe < 15 and pb < 1.5:
                return 'positive', 1.8
            elif pe > 25 and pb > 2.5:
                return 'negative', 1.2
            else:
                return 'neutral', 1.5
    
    except Exception as e:
        logger.error(f"获取市场情绪数据失败: {str(e)}")
    
    # 默认返回中性情绪
    return 'neutral', 1.5

def calculate_signal_strength(trend_signal, trend_strength, 
                             volume_confirmation, volume_strength,
                             risk_level, risk_score,
                             valuation, valuation_score,
                             sentiment, sentiment_score):
    """
    计算综合交易信号强度
    参数:
        trend_signal: 趋势信号
        trend_strength: 趋势强度 (1-5)
        volume_confirmation: 量能确认
        volume_strength: 量能强度 (1-3)
        risk_level: 风险水平
        risk_score: 风险评分 (1-3)
        valuation: 估值水平
        valuation_score: 估值评分 (1-3)
        sentiment: 情绪水平
        sentiment_score: 情绪评分 (1-2)
    返回:
        dict: 交易信号详情
    """
    # 计算加权评分 (1-5)
    total_score = (
        trend_strength * 0.35 +
        volume_strength * 0.25 +
        risk_score * 0.20 +
        valuation_score * 0.15 +
        sentiment_score * 0.05
    )
    
    # 基于总评分确定操作
    if total_score >= 4.0:
        action = 'strong_buy'
        position = 70 if valuation == 'undervalued' else 50
    elif total_score >= 3.0:
        action = 'buy' if trend_strength >= 3.0 else 'sell'
        position = 50
    elif total_score >= 2.0:
        action = 'hold'
        position = 25 if risk_level == 'warning' else 50
    else:
        action = 'strong_sell'
        position = 0
    
    # 风险覆盖
    if risk_level == 'danger':
        position = max(0, position - 30)
        if position == 0:
            action = 'strong_sell'
    
    return {
        'action': action,
        'position': position,
        'total_score': round(total_score, 2),
        'trend_score': round(trend_strength, 1),
        'volume_score': round(volume_strength, 1),
        'risk_score': round(risk_score, 1),
        'valuation_score': round(valuation_score, 1),
        'sentiment_score': round(sentiment_score, 1)
    }

def generate_rationale(trend_signal, trend_strength, 
                      volume_confirmation, volume_strength,
                      risk_level, risk_score,
                      valuation, valuation_score,
                      sentiment, sentiment_score):
    """
    生成交易信号的详细依据
    参数:
        同calculate_signal_strength
    返回:
        str: 详细依据
    """
    rationale = []
    
    # 趋势依据
    if trend_signal in ['strong_buy', 'buy']:
        rationale.append(f"趋势向上：{trend_signal.replace('_', ' ')}信号 (强度:{trend_strength}/5.0)")
    elif trend_signal in ['strong_sell', 'sell']:
        rationale.append(f"趋势向下：{trend_signal.replace('_', ' ')}信号 (强度:{trend_strength}/5.0)")
    else:
        rationale.append(f"趋势中性：{trend_signal}信号 (强度:{trend_strength}/5.0)")
    
    # 量能依据
    rationale.append(f"量能{volume_confirmation}：成交量{volume_confirmation.replace('confirmed', '放大').replace('weak', '不足')} (强度:{volume_strength}/3.0)")
    
    # 风险依据
    rationale.append(f"风险{risk_level}：{risk_level}风险水平 (评分:{risk_score}/3.0)")
    
    # 估值依据
    rationale.append(f"估值{valuation}：{valuation}估值水平 (评分:{valuation_score}/3.0)")
    
    # 情绪依据
    rationale.append(f"情绪{sentiment}：{sentiment}市场情绪 (评分:{sentiment_score}/2.0)")
    
    return " | ".join(rationale)

def calculate_strategy(etf_code, etf_name, etf_type):
    """
    计算单只ETF的交易策略
    参数:
        etf_code: ETF代码
        etf_name: ETF名称
        etf_type: 'stable'或'aggressive'
    返回:
        dict: 策略详情
    """
    # 获取ETF数据
    etf_data = get_etf_data(etf_code, 'daily')
    
    # 分析趋势
    trend_signal, trend_strength = analyze_trend(etf_data)
    
    # 分析量能
    volume_confirmation, volume_strength = analyze_volume(etf_data)
    
    # 评估风险
    risk_level, risk_score = assess_risk(etf_data, etf_type)
    
    # 评估估值
    valuation, valuation_score = assess_valuation(etf_code, etf_data)
    
    # 分析情绪
    sentiment, sentiment_score = analyze_sentiment()
    
    # 计算信号强度
    signal = calculate_signal_strength(
        trend_signal, trend_strength,
        volume_confirmation, volume_strength,
        risk_level, risk_score,
        valuation, valuation_score,
        sentiment, sentiment_score
    )
    
    # 生成依据
    rationale = generate_rationale(
        trend_signal, trend_strength,
        volume_confirmation, volume_strength,
        risk_level, risk_score,
        valuation, valuation_score,
        sentiment, sentiment_score
    )
    
    # 获取当前北京时间
    beijing_time = convert_to_beijing_time(datetime.now())
    
    return {
        'etf_code': etf_code,
        'etf_name': etf_name,
        'etf_type': etf_type,
        'action': signal['action'],
        'position': signal['position'],
        'total_score': signal['total_score'],
        'trend_score': signal['trend_score'],
        'volume_score': signal['volume_score'],
        'risk_score': signal['risk_score'],
        'valuation_score': signal['valuation_score'],
        'sentiment_score': signal['sentiment_score'],
        'rationale': rationale,
        'cf_time': beijing_time.strftime('%Y-%m-%d %H:%M')
    }

def log_trade(signal):
    """
    记录交易到流水（永久保存）
    参数:
        signal: 策略信号详情
    """
    try:
        # 准备交易日志
        trade_log = pd.DataFrame([{
            'timestamp': signal['cf_time'],
            'etf_code': signal['etf_code'],
            'etf_name': signal['etf_name'],
            'action': signal['action'],
            'position': signal['position'],
            'rationale': signal['rationale'],
            'total_score': signal['total_score']
        }])
        
        # 保存到交易流水目录
        today = datetime.now().strftime('%Y%m%d')
        filename = f"trade_log_{today}.csv"
        filepath = os.path.join(Config.TRADE_LOG_DIR, filename)
        trade_log.to_csv(filepath, mode='a', header=not os.path.exists(filepath), index=False)
        
        logger.info(f"交易记录成功: {signal['etf_code']} - {signal['action']}")
    except Exception as e:
        logger.error(f"记录交易失败: {str(e)}")

def push_strategy_results():
    """
    推送股票池中所有ETF的策略结果
    本函数应在交易日14:50北京时间运行
    """
    logger.info("开始策略计算和推送流程")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过策略计算")
        return False
    
    # 检查是否在交易时段
    is_trading, _ = is_trading_time()
    if not is_trading:
        logger.info("非交易时段，跳过策略计算")
        return False
    
    # 检查时间是否在14:50之后
    beijing_now = convert_to_beijing_time(datetime.now())
    if beijing_now.time() < datetime.time(14, 50):
        logger.info(f"当前时间是{beijing_now.strftime('%H:%M')}，早于14:50。跳过策略计算。")
        return False
    
    # 获取当前股票池
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        logger.error("无法获取当前股票池")
        return False
    
    logger.info(f"处理股票池中的{len(stock_pool)}只ETF")
    
    # 处理每只ETF
    for idx, etf in stock_pool.iterrows():
        try:
            logger.info(f"计算{etf['code']} - {etf['name']}的策略")
            
            # 确定ETF类型
            etf_type = 'stable' if etf['type'] == '稳健仓' else 'aggressive'
            
            # 计算策略
            signal = calculate_strategy(etf['code'], etf['name'], etf_type)
            
            # 格式化消息
            message = f"CF系统时间：{signal['cf_time']}\n"
            message += f"ETF代码：{signal['etf_code']}\n"
            message += f"名称：{signal['etf_name']}\n"
            message += f"操作建议：{signal['action'].replace('_', ' ')}\n"
            message += f"仓位比例：{signal['position']}%\n"
            message += f"综合评分：{signal['total_score']}/5.0\n"
            message += f"策略依据：{signal['rationale']}"
            
            # 推送消息
            logger.info(f"推送{etf['code']}的消息")
            send_wecom_message(message)
            
            # 记录交易
            log_trade(signal)
            
            # 每条消息间隔1分钟
            if idx < len(stock_pool) - 1:
                logger.info("等待1分钟后再发送下一条消息...")
                time.sleep(60)
            
        except Exception as e:
            logger.error(f"处理{etf['code']}时出错: {str(e)}")
            continue
    
    logger.info("策略计算和推送流程完成")
    return True

def is_trading_day(date=None):
    """检查是否为交易日"""
    from time_utils import is_trading_day as _is_trading_day
    return _is_trading_day(date)

def is_trading_time():
    """检查是否在交易时段"""
    from time_utils import is_trading_time as _is_trading_time
    return _is_trading_time()