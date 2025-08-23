"""2025-08-20 Ver3.0 主入口文件
所有说明查看【notes.md】"""

import os
import sys
import time
import pandas as pd
import numpy as np
import datetime
import pytz
import shutil
import requests
import subprocess
import json
from flask import Flask, request, jsonify, has_app_context
from config import Config
from logger import get_logger
from bs4 import BeautifulSoup
from data_fix import (
                     get_beijing_time, is_trading_day, get_all_etf_list,
                     get_new_stock_subscriptions, get_new_stock_listings,
                     get_etf_data, crawl_etf_data, read_new_stock_pushed_flag, 
                     mark_new_stock_info_pushed, read_listing_pushed_flag, 
                     mark_listing_info_pushed, check_data_integrity, send_wecom_message,
                     get_etf_iopv_data, get_market_sentiment)

# 确保所有数据目录存在
Config.init_directories()

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

def calculate_ETF_score(etf_code):
    """计算ETF评分
    参数:
        etf_code: ETF代码
    返回:
        dict: 评分结果或None（如果失败）"""
    try:
        # 获取ETF数据
        data = get_etf_data(etf_code, 'daily')
        if data is None or data.empty:
            error_msg = f"【数据错误】获取{etf_code}数据失败，无法计算评分"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 确保数据按日期排序
        data = data.sort_values('date')
        
        # 1. 流动性评分 (基于成交量)
        avg_volume = data['volume'].mean()
        liquidity_score = min(100, max(0, avg_volume / 1000000 * 10))  # 假设100万成交量为满分
        
        # 2. 风险控制评分 (基于波动率)
        returns = data['close'].pct_change().dropna()
        volatility = returns.std() * np.sqrt(252)  # 年化波动率
        risk_score = 100 - min(100, volatility * 100)  # 假设1%波动率为满分
        
        # 3. 收益能力评分 (基于近期收益率)
        current_price = data['close'].iloc[-1]
        past_price = data['close'].iloc[-30]  # 30天前价格
        return_pct = (current_price - past_price) / past_price
        return_score = min(100, max(0, return_pct * 1000))  # 假设10%收益率为满分
        
        # 4. 溢价率评分 (需要IOPV数据)
        try:
            # 尝试获取IOPV数据
            iopv_data = get_etf_iopv_data(etf_code)
            if iopv_data is not None and not iopv_data.empty:
                latest_iopv = iopv_data['iopv'].iloc[-1]
                premium_rate = (current_price - latest_iopv) / latest_iopv * 100
                premium_score = 100 - min(100, abs(premium_rate) * 5)  # 溢价率越小分越高
            else:
                error_msg = f"【数据错误】获取{etf_code} IOPV数据失败，溢价率评分使用默认值"
                logger.warning(error_msg)
                send_wecom_message(error_msg)
                premium_score = 50
        except Exception as e:
            error_msg = f"【数据错误】获取{etf_code} IOPV数据失败: {str(e)}，溢价率评分使用默认值"
            logger.warning(error_msg)
            send_wecom_message(error_msg)
            premium_score = 50
        
        # 5. 情绪指标评分 (基于市场情绪)
        try:
            # 获取市场情绪数据
            sentiment = get_market_sentiment()
            if sentiment is not None:
                # 根据市场情绪调整评分
                sentiment_score = 50 + sentiment * 50  # 情绪值范围[-1, 1]
                sentiment_score = max(0, min(100, sentiment_score))  # 限制在0-100范围内
            else:
                error_msg = "【数据错误】获取市场情绪数据失败，情绪评分使用默认值"
                logger.warning(error_msg)
                send_wecom_message(error_msg)
                sentiment_score = 50
        except Exception as e:
            error_msg = f"【数据错误】获取市场情绪数据失败: {str(e)}，情绪评分使用默认值"
            logger.warning(error_msg)
            send_wecom_message(error_msg)
            sentiment_score = 50
        
        # 综合评分 (加权平均)
        total_score = (
            liquidity_score * SCORE_WEIGHTS['liquidity'] +
            risk_score * SCORE_WEIGHTS['risk'] +
            return_score * SCORE_WEIGHTS['return'] +
            premium_score * SCORE_WEIGHTS['premium'] +
            sentiment_score * SCORE_WEIGHTS['sentiment']
        )
        
        # 构建结果
        result = {
            'etf_code': etf_code,
            'liquidity_score': liquidity_score,
            'risk_score': risk_score,
            'return_score': return_score,
            'premium_score': premium_score,
            'sentiment_score': sentiment_score,
            'total_score': total_score,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        logger.debug(f"{etf_code}评分结果: {result}")
        return result
    except Exception as e:
        error_msg = f"【系统错误】计算{etf_code}评分失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None

def generate_stock_pool():
    """生成股票池（5只稳健仓 + 5只激进仓）"""
    # 检查数据完整性
    integrity_check = check_data_integrity()
    if integrity_check:
        error_msg = f"【数据错误】{integrity_check}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None
    
    try:
        # 获取ETF列表
        etf_list = get_all_etf_list()
        if etf_list is None or etf_list.empty:
            error_msg = "【数据错误】股票池生成失败：ETF列表为空"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 评分所有ETF
        scored_etfs = []
        for _, etf in etf_list.iterrows():
            try:
                # 确保ETF代码是标准化格式
                etf_code = etf['code']
                score = calculate_ETF_score(etf_code)
                if score:
                    scored_etfs.append(score)
            except Exception as e:
                error_msg = f"【系统错误】计算{etf.get('code', '未知')}评分失败: {str(e)}"
                logger.error(error_msg)
                send_wecom_message(error_msg)
                continue
        
        if not scored_etfs:
            error_msg = "【数据错误】股票池生成失败：无有效评分数据"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 转换为DataFrame
        scores_df = pd.DataFrame(scored_etfs)
        
        # 按综合评分排序
        scores_df = scores_df.sort_values('total_score', ascending=False)
        
        # 选择稳健仓（高评分、低波动）
        # 稳健仓选择标准：综合评分高，风险评分高
        # 先按总评分排序，再按风险评分排序
        selected_stable = scores_df.nlargest(10, 'total_score')
        selected_stable = selected_stable.nlargest(5, 'risk_score')
        
        # 选择激进仓（高收益潜力）
        # 激进仓选择标准：收益评分高，溢价率低
        selected_aggressive = scores_df.nlargest(10, 'return_score')
        selected_aggressive = selected_aggressive.nlargest(5, 'premium_score')
        
        # 检查是否选择到足够的ETF
        if len(selected_stable) < 5:
            error_msg = f"【数据错误】稳健仓ETF数量不足（仅{len(selected_stable)}只），无法生成完整股票池"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
            
        if len(selected_aggressive) < 5:
            error_msg = f"【数据错误】激进仓ETF数量不足（仅{len(selected_aggressive)}只），无法生成完整股票池"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 合并股票池
        final_pool = pd.concat([selected_stable, selected_aggressive])
        
        # 为每只ETF添加类型标识
        final_pool['type'] = ''
        final_pool.loc[final_pool.index.isin(selected_stable.index), 'type'] = '稳健仓'
        final_pool.loc[final_pool.index.isin(selected_aggressive.index), 'type'] = '激进仓'
        
        # 保存到数据目录
        os.makedirs(Config.STOCK_POOL_DIR, exist_ok=True)
        filename = f"stock_pool_{get_beijing_time().strftime('%Y%m%d')}.csv"
        filepath = os.path.join(Config.STOCK_POOL_DIR, filename)
        final_pool.to_csv(filepath, index=False)
        logger.info(f"股票池生成成功。保存为: {filename}")
        logger.info(f"选定{len(selected_stable)}只稳健ETF和{len(selected_aggressive)}只激进ETF")
        
        # 生成消息
        message = "【ETF股票池】\n"
        message += "【稳健仓】\n"
        for _, etf in selected_stable.iterrows():
            message += f"• {etf['etf_code']} - {etf.get('name', etf.get('etf_name', '未知'))} (评分: {etf['total_score']:.2f})\n"
            message += f"  流动性: {etf['liquidity_score']:.1f} | 风险: {etf['risk_score']:.1f} | 收益: {etf['return_score']:.1f}\n"
        
        message += "\n【激进仓】\n"
        for _, etf in selected_aggressive.iterrows():
            message += f"• {etf['etf_code']} - {etf.get('name', etf.get('etf_name', '未知'))} (评分: {etf['total_score']:.2f})\n"
            message += f"  流动性: {etf['liquidity_score']:.1f} | 风险: {etf['risk_score']:.1f} | 收益: {etf['return_score']:.1f}\n"
        
        return message
    except Exception as e:
        error_msg = f"【系统错误】股票池生成失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None

def format_new_stock_subscriptions_message(new_stocks):
    """格式化新股申购信息消息"""
    if new_stocks is None or new_stocks.empty:
        return "今天没有新股、新债、新债券可认购"
    
    message = "【今日新股申购】\n"
    for _, stock in new_stocks.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}\n"
        message += f"  申购上限: {stock.get('申购上限', '未知')}\n"
        message += f"  申购日期: {stock.get('申购日期', '未知')}\n\n"
    
    return message

def format_new_stock_listings_message(new_listings):
    """格式化新上市交易股票信息消息"""
    if new_listings is None or new_listings.empty:
        return "今天没有新上市股票、可转债、债券可供交易"
    
    message = "【今日新上市交易】\n"
    for _, stock in new_listings.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}\n"
        message += f"  上市日期: {stock.get('上市日期', '未知')}\n\n"
      
    return message

def push_new_stock_info(test=False):
    """推送当天新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功"""
    # 检查是否已经推送过
    if not test:
        flag_path, is_pushed = read_new_stock_pushed_flag(get_beijing_time().date())
        if is_pushed:
            logger.info("今天已经推送过新股信息，跳过")
            return True
    
    new_stocks = get_new_stock_subscriptions(test=test)
    if new_stocks is None or new_stocks.empty:
        message = "今天没有新股、新债、新债券可认购"
    else:
        message = format_new_stock_subscriptions_message(new_stocks)
    
    if test:
        message = "【测试消息】" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        mark_new_stock_info_pushed()
    
    return success

def push_listing_info(test=False):
    """推送当天新上市交易的新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功"""
    # 检查是否已经推送过
    if not test:
        flag_path, is_pushed = read_listing_pushed_flag(get_beijing_time().date())
        if is_pushed:
            logger.info("今天已经推送过新上市交易信息，跳过")
            return True
    
    new_listings = get_new_stock_listings(test=test)
    if new_listings is None or new_listings.empty:
        message = "今天没有新上市股票、可转债、债券可供交易"
    else:
        message = format_new_stock_listings_message(new_listings)
    
    if test:
        message = "【测试消息】" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        mark_listing_info_pushed()
    
    return success

def calculate_strategy(code, name, etf_type):
    """基于评分系统计算单只ETF的策略信号
    参数:
        code: ETF代码
        name: ETF名称
        etf_type: ETF类型 ('stable'稳健仓, 'aggressive'激进仓)
    返回:
        dict: 策略信号"""
    
    # 获取ETF评分
    etf_score = calculate_ETF_score(code)
    if etf_score is None:
        error_msg = f"【数据错误】无法获取{code}的评分，无法生成策略信号"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None
    
    # 获取ETF数据
    etf_data = get_etf_data(code, 'daily')
    if etf_data is None or etf_data.empty:
        error_msg = f"【数据错误】无法获取{code}的数据，无法生成策略信号"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None
    
    # 获取最新价格
    current_price = etf_data['close'].iloc[-1]
    
    # 根据ETF类型生成策略信号
    if etf_type == 'stable':
        # 稳健仓策略：注重风险控制和稳定性
        if etf_score['total_score'] >= 80 and etf_score['risk_score'] >= 70:
            action = 'strong_buy'
            position = 20  # 仓位比例20%
            rationale = f"综合评分高({etf_score['total_score']:.1f})，风险控制好({etf_score['risk_score']:.1f})"
        elif etf_score['total_score'] >= 60 and etf_score['risk_score'] >= 50:
            action = 'buy'
            position = 10
            rationale = f"综合评分良好({etf_score['total_score']:.1f})，风险适中({etf_score['risk_score']:.1f})"
        elif etf_score['total_score'] <= 30:
            action = 'strong_sell'
            position = 0
            rationale = f"综合评分低({etf_score['total_score']:.1f})，风险较高({etf_score['risk_score']:.1f})"
        else:
            action = 'hold'
            position = 0
            rationale = f"综合评分中等({etf_score['total_score']:.1f})，等待更明确信号"
    else:
        # 激进仓策略：注重收益潜力和交易机会
        if etf_score['return_score'] >= 80 and etf_score['premium_score'] >= 70:
            action = 'strong_buy'
            position = 10  # 仓位比例10%
            rationale = f"收益潜力大({etf_score['return_score']:.1f})，溢价率低({etf_score['premium_score']:.1f})"
        elif etf_score['return_score'] >= 60 and etf_score['premium_score'] >= 50:
            action = 'buy'
            position = 5
            rationale = f"收益潜力较好({etf_score['return_score']:.1f})，溢价率适中({etf_score['premium_score']:.1f})"
        elif etf_score['return_score'] <= 30:
            action = 'strong_sell'
            position = 0
            rationale = f"收益潜力低({etf_score['return_score']:.1f})，无交易机会"
        else:
            action = 'hold'
            position = 0
            rationale = f"收益潜力中等({etf_score['return_score']:.1f})，等待更明确信号"
    
    # 构建策略信号
    signal = {
        'etf_code': code,
        'etf_name': name,
        'cf_time': get_beijing_time().strftime('%Y-%m-%d %H:%M'),
        'action': action,
        'position': position,
        'rationale': rationale,
        'total_score': etf_score['total_score'],
        'risk_score': etf_score['risk_score'],
        'return_score': etf_score['return_score'],
        'current_price': current_price
    }
    
    return signal

def push_strategy_results(test=False):
    """计算策略信号并推送到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功"""
    logger.info(f"{'测试' if test else ''}策略计算与推送开始")
    
    # 获取当前股票池
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        error_msg = "【数据错误】股票池为空，无法计算策略"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return False
    
    # 按ETF类型分组
    stable_etfs = stock_pool[stock_pool['type'] == '稳健仓']
    aggressive_etfs = stock_pool[stock_pool['type'] == '激进仓']
    
    # 计算并推送稳健仓策略
    if not stable_etfs.empty:
        logger.info(f"开始推送稳健仓策略（{len(stable_etfs)}只ETF）")
        for _, etf in stable_etfs.iterrows():
            # 确保ETF代码是标准化格式
            etf_code = etf['etf_code']
            etf_name = etf.get('name', etf.get('etf_name', '未知'))
            signal = calculate_strategy(etf_code, etf_name, 'stable')
            
            if signal is None:
                continue
                
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
            # 确保ETF代码是标准化格式
            etf_code = etf['etf_code']
            etf_name = etf.get('name', etf.get('etf_name', '未知'))
            signal = calculate_strategy(etf_code, etf_name, 'aggressive')
            
            if signal is None:
                continue
                
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
    """记录交易流水
    参数:
        signal: 策略信号"""
    # 确保交易日志目录存在
    os.makedirs(Config.TRADE_LOG_DIR, exist_ok=True)
    
    # 创建日志文件名
    filename = f"trade_log_{get_beijing_time().strftime('%Y%m%d')}.csv"
    filepath = os.path.join(Config.TRADE_LOG_DIR, filename)
    
    # 创建交易记录
    trade_record = {
        '时间': signal['cf_time'],
        'ETF代码': signal['etf_code'],
        'ETF名称': signal['etf_name'],
        '操作': signal['action'],
        '仓位比例': signal['position'],
        '总评分': signal['total_score'],
        '风险评分': signal['risk_score'],
        '收益评分': signal['return_score'],
        '当前价格': signal['current_price'],
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

def _format_strategy_signal(signal, test=False):
    """格式化策略信号消息"""
    if not signal:
        return "无有效策略信号"
    
    # 构建消息
    message = f"{'【测试策略信号】' if test else '【策略信号】'}\n"
    message += f"CF系统时间：{signal['cf_time']}\n"
    message += f"ETF代码：{signal['etf_code']}\n"
    message += f"名称：{signal['etf_name']}\n"
    message += f"操作建议：{signal['action']}\n"
    message += f"仓位比例：{signal['position']}%\n"
    message += f"综合评分：{signal['total_score']:.1f}\n"
    message += f"风险评分：{signal['risk_score']:.1f}\n"
    message += f"收益评分：{signal['return_score']:.1f}\n"
    message += f"当前价格：{signal['current_price']:.4f}\n"
    message += f"策略依据：{signal['rationale']}"
    
    return message

def get_current_stock_pool():
    """获取当前股票池"""
    try:
        # 获取最新股票池文件
        pool_files = [f for f in os.listdir(Config.STOCK_POOL_DIR) if f.startswith('stock_pool_')]
        if not pool_files:
            error_msg = "【数据错误】未找到股票池文件"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 按文件名排序，获取最新文件
        latest_file = max(pool_files)
        filepath = os.path.join(Config.STOCK_POOL_DIR, latest_file)
        
        # 读取股票池数据
        df = pd.read_csv(filepath)
        
        # 重命名列以匹配策略函数期望
        # 检查列是否存在，如果不存在则尝试其他可能的列名
        if 'code' not in df.columns and 'etf_code' in df.columns:
            df = df.rename(columns={'etf_code': 'code'})
        if 'name' not in df.columns and 'etf_name' in df.columns:
            df = df.rename(columns={'etf_name': 'name'})
        if 'name' not in df.columns and '基金简称' in df.columns:
            df = df.rename(columns={'基金简称': 'name'})
        
        return df
    except Exception as e:
        error_msg = f"【系统错误】获取股票池失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None

def check_arbitrage_opportunity():
    """检查套利机会"""
    try:
        # 检查数据完整性
        integrity_check = check_data_integrity()
        if integrity_check:
            error_msg = f"【数据错误】{integrity_check}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return False
        
        # 获取ETF列表
        etf_list = get_all_etf_list()
        if etf_list is None or etf_list.empty:
            error_msg = "【数据错误】套利检查失败：ETF列表为空"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return False
        
        # 检查每只ETF
        opportunities = []
        for _, etf in etf_list.iterrows():
            etf_code = etf['code']
            etf_name = etf.get('name', '未知')
            
            try:
                # 获取ETF数据
                etf_data = get_etf_data(etf_code, 'daily')
                if etf_data is None or etf_data.empty:
                    continue
                
                # 获取IOPV数据
                iopv_data = get_etf_iopv_data(etf_code)
                if iopv_data is None or iopv_data.empty:
                    continue
                
                # 检查溢价率
                latest = etf_data.iloc[-1]
                latest_iopv = iopv_data['iopv'].iloc[-1]
                premium_rate = (latest['close'] - latest_iopv) / latest_iopv * 100
                
                # 如果溢价率超过阈值，视为套利机会
                # 正溢价：ETF价格 > IOPV，适合申购套利
                # 负溢价：ETF价格 < IOPV，适合赎回套利
                if abs(premium_rate) >= 0.5:  # 0.5%阈值
                    # 计算目标价格和止损价格
                    current_price = latest['close']
                    target_price = current_price * (1 + 0.01 * (1 if premium_rate > 0 else -1))
                    stop_loss_price = current_price * (1 - 0.01 * (1 if premium_rate > 0 else -1))
                    
                    opportunities.append({
                        'etf_code': etf_code,
                        'etf_name': etf_name,
                        'premium_rate': premium_rate,
                        'current_price': current_price,
                        'iopv': latest_iopv,
                        'target_price': target_price,
                        'stop_loss_price': stop_loss_price
                    })
            except Exception as e:
                error_msg = f"【系统错误】处理ETF {etf_code} 时出错: {str(e)}"
                logger.error(error_msg)
                send_wecom_message(error_msg)
        
        if opportunities:
            logger.info(f"发现 {len(opportunities)} 个套利机会")
            
            # 推送所有套利机会
            for opportunity in opportunities:
                # 生成消息
                message = "【ETF套利机会】\n"
                message += f"• {opportunity['etf_name']} ({opportunity['etf_code']})\n"
                message += f"  当前价格: {opportunity['current_price']:.4f}\n"
                message += f"  IOPV: {opportunity['iopv']:.4f}\n"
                message += f"  溢价率: {opportunity['premium_rate']:.2f}%\n"
                message += f"  止盈目标：{opportunity['target_price']:.4f}\n"
                message += f"  止损价格：{opportunity['stop_loss_price']:.4f}\n"
                
                if opportunity['premium_rate'] > 0:
                    message += "  建议：溢价套利，申购ETF份额，等待溢价消失\n"
                else:
                    message += "  建议：折价套利，赎回ETF份额，等待折价消失\n"
                
                # 发送消息
                send_wecom_message(message)
                
                # 记录套利机会
                record_arbitrage_opportunity(opportunity)
            
            return True
        else:
            logger.info("未发现套利机会")
            return False
    except Exception as e:
        error_msg = f"【系统错误】套利检查失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return False

def record_arbitrage_opportunity(opportunity):
    """记录套利机会"""
    # 确保套利目录存在
    os.makedirs(Config.ARBITRAGE_DIR, exist_ok=True)
    
    # 创建日志文件名
    filename = f"arbitrage_{get_beijing_time().strftime('%Y%m%d')}.csv"
    filepath = os.path.join(Config.ARBITRAGE_DIR, filename)
    
    # 创建交易记录
    arbitrage_record = {
        '时间': get_beijing_time().strftime('%Y-%m-%d %H:%M'),
        'ETF代码': opportunity['etf_code'],
        'ETF名称': opportunity['etf_name'],
        '溢价率': opportunity['premium_rate'],
        '当前价格': opportunity['current_price'],
        'IOPV': opportunity['iopv'],
        '止盈目标': opportunity['target_price'],
        '止损价格': opportunity['stop_loss_price']
    }
    
    # 保存到CSV
    if os.path.exists(filepath):
        # 追加到现有文件
        df = pd.read_csv(filepath)
        df = pd.concat([df, pd.DataFrame([arbitrage_record])], ignore_index=True)
        df.to_csv(filepath, index=False)
    else:
        # 创建新文件
        pd.DataFrame([arbitrage_record]).to_csv(filepath, index=False)
    logger.info(f"套利机会已记录: {opportunity['etf_name']} - 溢价率 {opportunity['premium_rate']:.2f}%")

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
    flag_path, is_pushed = read_new_stock_pushed_flag(get_beijing_time().date())
    if is_pushed:
        logger.info("今天已经推送过新股信息，跳过")
        response = {
            "status": "skipped",
            "message": "New stock info already pushed today"
        }
        return jsonify(response) if has_app_context() else response
    
    # 推送新股申购信息
    success_new_stock = push_new_stock_info()
    
    # 推送新上市交易股票信息
    success_listing = push_listing_info()
    
    response = {
        "status": "success" if success_new_stock and success_listing else "partial_success",
        "new_stock": "success" if success_new_stock else "failed",
        "listing": "success" if success_listing else "failed"
    }
    return jsonify(response) if has_app_context() else response

@app.route('/cron/push-strategy', methods=['GET', 'POST'])
def cron_push_strategy():
    """计算策略信号并推送到企业微信"""
    logger.info("策略信号推送任务触发")
    
    # 检查数据完整性
    integrity_check = check_data_integrity()
    if integrity_check:
        error_msg = f"【数据错误】{integrity_check}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        response = {"status": "error", "message": error_msg}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过策略信号推送")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否已经推送过
    today = get_beijing_time().date()
    stock_pool_file = f"stock_pool_{today.strftime('%Y%m%d')}.csv"
    stock_pool_path = os.path.join(Config.STOCK_POOL_DIR, stock_pool_file)
    
    if not os.path.exists(stock_pool_path):
        error_msg = "【数据错误】股票池未生成，无法推送策略"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        response = {"status": "error", "message": "Stock pool not generated"}
        return jsonify(response) if has_app_context() else response
    
    # 推送策略
    success = push_strategy_results()
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/update-stock-pool', methods=['GET', 'POST'])
def cron_update_stock_pool():
    """每周五16:00北京时间更新ETF股票池（5只稳健仓 + 5只激进仓）"""
    logger.info("股票池更新任务触发")
    
    # 检查数据完整性
    integrity_check = check_data_integrity()
    if integrity_check:
        error_msg = f"【数据错误】{integrity_check}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        response = {"status": "error", "message": error_msg}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过股票池更新")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否为周五
    if datetime.datetime.now().weekday() != 4:  # 4 = Friday
        logger.info("今天不是周五，跳过股票池更新")
        response = {"status": "skipped", "message": "Not Friday"}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否已经更新过
    today = get_beijing_time().date()
    stock_pool_file = f"stock_pool_{today.strftime('%Y%m%d')}.csv"
    stock_pool_path = os.path.join(Config.STOCK_POOL_DIR, stock_pool_file)
    
    if os.path.exists(stock_pool_path):
        logger.info("今天已经更新过股票池，跳过")
        response = {"status": "skipped", "message": "Stock pool already updated today"}
        return jsonify(response) if has_app_context() else response
    
    # 生成股票池
    stock_pool_message = generate_stock_pool()
    
    response = {
        "status": "success" if stock_pool_message else "error",
        "message": stock_pool_message if stock_pool_message else "Failed to generate stock pool"
    }
    return jsonify(response) if has_app_context() else response

@app.route('/cron/arbitrage-scan', methods=['GET', 'POST'])
def cron_arbitrage_scan():
    """套利扫描任务"""
    logger.info("套利扫描任务触发")
    
    # 检查数据完整性
    integrity_check = check_data_integrity()
    if integrity_check:
        error_msg = f"【数据错误】{integrity_check}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        response = {"status": "error", "message": error_msg}
        return jsonify(response) if has_app_context() else response
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过套利扫描")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 执行套利扫描
    success = check_arbitrage_opportunity()
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/resume_crawl', methods=['GET', 'POST'])
def cron_resume_crawl():
    """断点续爬任务（支持非交易日执行）"""
    logger.info("断点续爬任务触发")
    
    # 移除了交易日检查 - 这是关键修复
    # 现在可以在非交易日（如周末）执行续爬
    
    # 检查状态文件
    status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
    if not os.path.exists(status_file):
        logger.info("无待续爬任务，启动全新爬取")
        return cron_crawl_daily()
    
    # 加载状态
    try:
        with open(status_file, 'r') as f:
            crawl_status = json.load(f)
    except Exception as e:
        error_msg = f"【系统错误】加载爬取状态失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return {"status": "error", "message": "Failed to load status"}
    
    # 筛选未完成的ETF
    pending_etfs = [
        code for code, status in crawl_status.items()
        if status.get('status') in ['in_progress', 'failed']
    ]
    
    # 如果没有待续爬任务，启动全新爬取
    if not pending_etfs:
        logger.info("无待续爬任务，启动全新爬取")
        return cron_crawl_daily()
    
    # 执行续爬
    success_count = 0
    failed_count = 0
    
    for etf_code in pending_etfs:
        try:
            # 标记开始
            update_crawl_status(etf_code, 'in_progress')
            logger.info(f"【任务开始】开始续爬 {etf_code}")
            
            # 获取起始日期（从缓存中获取最后日期）- 关键增量爬取逻辑
            cached_data = load_from_cache(etf_code, 'daily')
            start_date = None
            if cached_data is not None and not cached_data.empty:
                last_date = cached_data['date'].max()
                start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"ETF {etf_code} 已有数据到 {last_date.strftime('%Y-%m-%d')}，从 {start_date} 开始获取新数据")
            
            # 尝试主数据源(AkShare)
            data = get_etf_data(etf_code, 'daily')
            
            # 检查结果
            if data is not None and not data.empty:
                # 保存数据（已在get_etf_data内部完成）
                update_crawl_status(etf_code, 'success')
                success_count += 1
                logger.info(f"成功续爬 {etf_code}，共 {len(data)} 条新记录")
            else:
                error_msg = f"【数据错误】续爬 {etf_code} 失败：返回空数据"
                logger.warning(error_msg)
                send_wecom_message(error_msg)
                update_crawl_status(etf_code, 'failed', 'Empty data')
                failed_count += 1
        except Exception as e:
            error_msg = f"【系统错误】续爬 {etf_code} 时出错: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            update_crawl_status(etf_code, 'failed', error_msg)
            failed_count += 1
        
        # 避免请求过快
        time.sleep(1)
    
    # 检查是否全部完成
    remaining = [code for code, status in get_crawl_status().items()
                 if status.get('status') in ['in_progress', 'failed']]
    if not remaining:
        try:
            os.remove(status_file)
            logger.info("所有ETF爬取成功，已清理状态文件")
        except Exception as e:
            logger.warning(f"清理状态文件失败: {str(e)}")
    
    return {
        "status": "success" if failed_count == 0 else "partial_success",
        "success_count": success_count,
        "failed_count": failed_count,
        "remaining": len(remaining)
    }


def main():
    """主函数"""
    # 从环境变量获取任务类型
    task = os.getenv('TASK', 'test_message')
    
    logger.info(f"执行任务: {task}")
    
    # 根据任务类型执行不同操作
    if task == 'test_message':
        # T01: 测试消息推送
        beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
        message = f"【测试消息】T01: 测试消息推送CF系统时间：{beijing_time}这是来自鱼盆ETF系统的测试消息。"
        success = send_wecom_message(message)
        response = {"status": "success" if success else "error", "message": "Test message sent"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_new_stock':
        # T07: 测试推送新股信息
        logger.info("执行测试新股信息推送任务")
        # 获取测试用的新股申购信息
        new_stocks = get_new_stock_subscriptions(test=True)
        # 检查是否获取到新股数据
        if new_stocks is None or new_stocks.empty:
            message = "【测试】近7天没有新股、新债、新债券可认购"
        else:
            # 使用专用测试消息格式
            message = "【测试新股信息】"
            message += f"共发现{len(new_stocks)}只新股："
            for _, row in new_stocks.iterrows():
                message += f"申购代码：{row.get('股票代码', '')}\n"
                message += f"股票简称：{row.get('股票简称', '')}\n"
                message += f"发行价格：{row.get('发行价格', '')}元\n"
                message += f"申购上限：{row.get('申购上限', '')}股\n"
                message += f"申购日期：{row.get('申购日期', '')}\n"
                message += "─" * 20 + "\n"
        
        message = "【测试消息】" + message
        success = send_wecom_message(message)
        
        if success:
            logger.info("测试新股信息推送成功")
            return {"status": "success", "message": "Test new stocks sent"}
        else:
            logger.error("测试新股信息推送失败")
            return {"status": "error", "message": "Failed to send test new stocks"}
    
    elif task == 'test_new_stock_listings':
        # T08: 测试新上市交易股票信息推送
        logger.info("执行测试新上市交易信息推送任务")
        # 获取测试用的新上市交易股票信息
        new_listings = get_new_stock_listings(test=True)
        # 检查是否获取到新上市交易股票数据
        if new_listings is None or new_listings.empty:
            message = "【测试】近7天没有新上市股票、可转债、债券可供交易"
        else:
            # 使用专用测试消息格式
            message = "【测试新上市交易信息】"
            message += f"共发现{len(new_listings)}只新上市交易股票："
            for _, row in new_listings.iterrows():
                message += f"股票代码：{row.get('股票代码', '')}\n"
                message += f"股票简称：{row.get('股票简称', '')}\n"
                message += f"发行价格：{row.get('发行价格', '')}元\n"
                message += f"上市日期：{row.get('上市日期', '')}\n"
                message += "─" * 20 + "\n"
        
        message = "【测试消息】" + message
        success = send_wecom_message(message)
        
        if success:
            logger.info("测试新上市交易信息推送成功")
            return {"status": "success", "message": "Test new listings sent"}
        else:
            logger.error("测试新上市交易信息推送失败")
            return {"status": "error", "message": "Failed to send test new listings"}
    
    elif task == 'test_stock_pool':
        # T04: 测试股票池推送
        stock_pool_message = generate_stock_pool()
        if stock_pool_message:
            success = send_wecom_message(stock_pool_message)
            response = {"status": "success" if success else "error", "message": "Stock pool test sent"}
        else:
            response = {"status": "error", "message": "Failed to generate stock pool"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_execute':
        # T05: 测试执行策略并推送结果
        success = push_strategy_results(test=True)
        response = {"status": "success" if success else "error", "message": "Strategy execution test completed"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_reset':
        # T06: 测试重置所有仓位（测试用）
        logger.info("重置所有仓位（测试用）")
        # 获取当前股票池
        stock_pool = get_current_stock_pool()
        if stock_pool is None or stock_pool.empty:
            error_msg = "【数据错误】股票池为空，无法重置仓位"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            response = {"status": "error", "message": "No stock pool available"}
            print(json.dumps(response, indent=2))
            return response
        
        # 创建重置信号
        beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
        for _, etf in stock_pool.iterrows():
            etf_type = 'stable' if etf['type'] == '稳健仓' else 'aggressive'
            etf_code = etf['etf_code']
            etf_name = etf.get('name', etf.get('etf_name', '未知'))
            signal = {
                'etf_code': etf_code,
                'etf_name': etf_name,
                'cf_time': beijing_time,
                'action': 'strong_sell',
                'position': 0,
                'rationale': '测试重置仓位'
            }
            
            # 格式化消息
            message = _format_strategy_signal(signal, test=True)
            
            # 推送消息
            send_wecom_message(message)
            
            # 记录交易
            log_trade(signal)
            
            # 间隔1分钟
            time.sleep(60)
        
        logger.info("测试重置仓位完成")
        response = {"status": "success", "message": "Positions reset"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_arbitrage':
        # T09: 测试套利扫描
        success = check_arbitrage_opportunity()
        response = {"status": "success" if success else "error", "message": "Arbitrage test completed"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'crawl_new_stock':
        # 9:31 AM：爬取当天新股申购、新上市股票信息
        success_new_stock = push_new_stock_info()
        success_listing = push_listing_info()
        response = {
            "status": "success" if success_new_stock and success_listing else "partial_success",
            "new_stock": "success" if success_new_stock else "failed",
            "listing": "success" if success_listing else "failed"
        }
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'push_new_stock':
        # 9:45 AM：推送新股申购和新上市股票信息
        success_new_stock = push_new_stock_info()
        success_listing = push_listing_info()
        response = {
            "status": "success" if success_new_stock and success_listing else "partial_success",
            "new_stock": "success" if success_new_stock else "failed",
            "listing": "success" if success_listing else "failed"
        }
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'crawl_intraday':
        # 9:55 AM：爬取当天ETF交易数据
        success = crawl_etf_data(data_type='intraday')
        response = {"status": "success" if success else "error", "message": "Intraday data crawl completed"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'arbitrage-scan':
        # 13:00 PM：套利扫描
        success = check_arbitrage_opportunity()
        response = {"status": "success" if success else "error", "message": "Arbitrage scan completed"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'push_strategy':
        # 13:00 PM：推送策略信号
        success = push_strategy_results()
        response = {"status": "success" if success else "error", "message": "Strategy push completed"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'crawl_daily':
        # 4:00 PM：爬取日线数据
        result = crawl_etf_data(data_type='daily')
        print(json.dumps(result, indent=2))
        return result
    
    elif task == 'cleanup':
        # 00:00 AM：清理旧数据
        from data_fix import cron_cleanup
        result = cron_cleanup()
        print(json.dumps(result, indent=2))
        return result

    elif task == 'resume_crawl':
        # 断点续爬任务（支持非交易日执行）
        result = cron_resume_crawl()
        print(json.dumps(result, indent=2))
        return result

    else:
        error_msg = f"【系统错误】未知任务类型: {task}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        response = {"status": "error", "message": "Unknown task type"}
        print(json.dumps(response, indent=2))
        return response

if __name__ == '__main__':
    # 如果作为Flask应用运行
    if len(sys.argv) > 1 and sys.argv[1] == 'flask':
        app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
    else:
        # 作为命令行任务运行
        main()
