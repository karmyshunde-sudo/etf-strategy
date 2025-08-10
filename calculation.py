"""
鱼盆ETF投资量化模型 - 策略计算模块
说明:
  本文件负责ETF策略信号的计算和推送
  所有文件放在根目录，简化导入关系
"""

import os
import time
import pandas as pd
from datetime import datetime
from config import Config
from logger import get_logger
from wecom import send_wecom_message
from scoring import get_current_stock_pool
from time_utils import get_beijing_time

logger = get_logger(__name__)

def calculate_strategy(etf_code, etf_name, etf_type='stable'):
    """
    计算单只ETF的策略信号
    参数:
        etf_code: ETF代码
        etf_name: ETF名称
        etf_type: 仓类型 ('stable'稳健仓, 'aggressive'激进仓)
    返回:
        dict: 策略信号
    """
    # 获取ETF评分
    from scoring import calculate_ETF_score
    etf_score = calculate_ETF_score(etf_code)
    
    # 根据评分决定操作
    if etf_score['total_score'] >= 85:
        action = '买入'
        position = 100 if etf_type == 'aggressive' else 50
        rationale = f"高评分ETF ({etf_score['total_score']})：突破20日均线+成交量放大"
    elif etf_score['total_score'] >= 70:
        action = '持有'
        position = 50 if etf_type == 'aggressive' else 30
        rationale = f"中等评分ETF ({etf_score['total_score']})：持有稳健仓位"
    else:
        action = '卖出'
        position = 0
        rationale = f"低评分ETF ({etf_score['total_score']})：跌破均线+风险上升"
    
    return {
        'code': etf_code,
        'name': etf_name,
        'action': action,
        'position': position,
        'rationale': rationale,
        'total_score': etf_score['total_score']
    }

def push_strategy_results():
    """
    计算策略信号并推送到企业微信
    返回:
        bool: 是否成功
    """
    logger.info("开始策略计算与推送")
    
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
            message = f"CF系统时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M')}\n"
            message += f"ETF代码：{signal['code']}\n"
            message += f"名称：{signal['name']}\n"
            message += f"操作建议：{signal['action']}\n"
            message += f"仓位比例：{signal['position']}%\n"
            message += f"策略依据：{signal['rationale']}"
            
            # 推送消息
            send_wecom_message(message)
            
            # 记录交易
            log_trade(signal)
            
            # 间隔1分钟
            time.sleep(60)
    
    # 计算并推送激进仓策略
    if not aggressive_etfs.empty:
        logger.info(f"开始推送激进仓策略（{len(aggressive_etfs)}只ETF）")
        for _, etf in aggressive_etfs.iterrows():
            signal = calculate_strategy(etf['code'], etf['name'], 'aggressive')
            # 格式化消息
            message = f"CF系统时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M')}\n"
            message += f"ETF代码：{signal['code']}\n"
            message += f"名称：{signal['name']}\n"
            message += f"操作建议：{signal['action']}\n"
            message += f"仓位比例：{signal['position']}%\n"
            message += f"策略依据：{signal['rationale']}"
            
            # 推送消息
            send_wecom_message(message)
            
            # 记录交易
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
        'ETF代码': signal['code'],
        'ETF名称': signal['name'],
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
    
    logger.info(f"交易记录已保存: {signal['name']} - {signal['action']}")
