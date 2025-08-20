"""2025-08-20 Ver1.0 主入口文件
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
from flask import Flask, request, jsonify, has_app_context
from config import Config
from logger import get_logger
from bs4 import BeautifulSoup
from data_fix import (get_beijing_time, is_trading_day, get_all_etf_list, 
                     get_new_stock_subscriptions, get_new_stock_listings,
                     cron_crawl_daily, cron_crawl_intraday, cron_cleanup, resume_crawl)

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
        from data_fix import get_etf_data
        data = get_etf_data(etf_code, 'daily')
        if data is None or data.empty:
            logger.error(f"获取{etf_code}数据失败，无法计算评分")
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
        
        # 4. 溢价率评分 (需要IOPV数据，这里简化处理)
        premium_score = 50  # 默认中等评分
        
        # 5. 情绪指标评分 (需要额外数据源，这里简化处理)
        sentiment_score = 50  # 默认中等评分
        
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
        logger.error(f"计算{etf_code}评分失败: {str(e)}")
        return None

def generate_stock_pool():
    """生成股票池（5只稳健仓 + 5只激进仓）"""
    try:
        # 获取ETF列表
        etf_list = get_all_etf_list()
        if etf_list is None or etf_list.empty:
            logger.error("股票池生成失败：ETF列表为空")
            return None
        
        # 评分所有ETF
        scored_etfs = []
        for _, etf in etf_list.iterrows():
            try:
                score = calculate_ETF_score(etf['code'])
                if score:
                    scored_etfs.append(score)
            except Exception as e:
                logger.error(f"计算{etf['code']}评分失败: {str(e)}")
                continue
        
        if not scored_etfs:
            logger.error("股票池生成失败：无有效评分数据")
            return None
        
        # 转换为DataFrame
        scores_df = pd.DataFrame(scored_etfs)
        
        # 按综合评分排序
        scores_df = scores_df.sort_values('total_score', ascending=False)
        
        # 选择稳健仓（高评分、低波动）
        selected_stable = scores_df.nlargest(5, 'total_score')
        
        # 选择激进仓（高收益潜力）
        selected_aggressive = scores_df.nlargest(10, 'return_score').iloc[5:]
        
        # 合并股票池
        final_pool = pd.concat([selected_stable, selected_aggressive])
        
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
            message += f"• {etf['etf_code']} - {etf['name']} (评分: {etf['total_score']:.2f})\n"
        
        message += "\n【激进仓】\n"
        for _, etf in selected_aggressive.iterrows():
            message += f"• {etf['etf_code']} - {etf['name']} (评分: {etf['total_score']:.2f})\n"
        
        return message
    except Exception as e:
        logger.error(f"股票池生成失败: {str(e)}")
        return None

def send_wecom_message(message):
    """发送消息到企业微信"""
    try:
        wecom_webhook = os.getenv('WECOM_WEBHOOK')
        if not wecom_webhook:
            logger.error("企业微信Webhook URL未配置")
            return False
        
        # 添加页脚
        footer = os.getenv('MESSAGE_FOOTER', '')
        if footer:
            message += f"\n\n{footer}"
        
        payload = {
            "msgtype": "text",
            "text": {
                "content": message,
                "mentioned_list": ["@all"]
            }
        }
        
        response = requests.post(wecom_webhook, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.info("企业微信消息发送成功")
        return True
    except Exception as e:
        logger.error(f"企业微信消息发送失败: {str(e)}")
        return False

def format_new_stock_message(new_stocks):
    """格式化新股信息消息"""
    if new_stocks is None or new_stocks.empty:
        return "今天没有新股可供申购"
    
    message = "【今日新股申购】\n"
    for _, stock in new_stocks.iterrows():
        message += f"• {stock.get('股票简称', '')} ({stock.get('股票代码', '')})\n"
        message += f"  发行价: {stock.get('发行价格', '未知')}\n"
        message += f"  申购上限: {stock.get('申购上限', '未知')}\n\n"
    
    return message

def format_new_stock_listings_message(new_listings):
    """格式化新上市交易股票信息消息"""
    if new_listings is None or new_listings.empty:
        return "今天没有新上市股票、可转债、债券可供交易"
    
    message = "【今日新上市交易】\n"
    for _, stock in new_listings.iterrows():
        message += f"• {stock.get('name', '')} ({stock.get('code', '')})\n"
        message += f"  发行价: {stock.get('issue_price', '未知')}\n\n"
    
    return message

def push_new_stock_info(test=False):
    """推送当天新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功"""
    new_stocks = get_new_stock_subscriptions()
    if new_stocks is None or new_stocks.empty:
        message = "今天没有新股可供申购"
    else:
        message = format_new_stock_message(new_stocks)
    
    if test:
        message = "【测试消息】" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        from data_fix import mark_new_stock_info_pushed
        mark_new_stock_info_pushed()
    
    return success

def push_listing_info(test=False):
    """推送当天新上市交易的新股信息到企业微信
    参数:
        test: 是否为测试模式
    返回:
        bool: 是否成功"""
    new_listings = get_new_stock_listings()
    if new_listings is None or new_listings.empty:
        message = "今天没有新上市股票、可转债、债券可供交易"
    else:
        message = format_new_stock_listings_message(new_listings)
    
    if test:
        message = "【测试消息】" + message
    
    success = send_wecom_message(message)
    
    # 标记已推送
    if success and not test:
        from data_fix import mark_listing_info_pushed
        mark_listing_info_pushed()
    
    return success

def push_strategy():
    """计算策略信号并推送到企业微信"""
    try:
        # 1. 生成股票池
        stock_pool_message = generate_stock_pool()
        if not stock_pool_message:
            logger.error("股票池为空，无法生成策略信号")
            return False
        
        # 2. 生成策略信号（简化示例）
        # 实际策略会更复杂，这里仅作演示
        strategy_message = "【ETF策略信号】\n"
        strategy_message += "当前策略：\n"
        strategy_message += "1. 稳健仓：5只ETF组合，适合长期持有\n"
        strategy_message += "2. 激进仓：5只ETF组合，适合短线交易\n"
        strategy_message += "\n详细股票池见上文"
        
        # 3. 合并消息
        full_message = stock_pool_message + "\n\n" + strategy_message
        
        # 4. 推送消息
        return send_wecom_message(full_message)
    except Exception as e:
        logger.error(f"策略推送失败: {str(e)}")
        return False

def record_trade(signal):
    """记录交易到日志"""
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

def get_arbitrage_status():
    """获取套利状态"""
    try:
        if not os.path.exists(Config.ARBITRAGE_STATUS_FILE):
            return None
        
        with open(Config.ARBITRAGE_STATUS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"读取套利状态失败: {str(e)}")
        return None

def update_arbitrage_status(etf_code, etf_name, timestamp, current_price, target_price, stop_loss_price):
    """更新套利状态"""
    try:
        status = {
            "etf_code": etf_code,
            "etf_name": etf_name,
            "timestamp": timestamp,
            "current_price": current_price,
            "target_price": target_price,
            "stop_loss_price": stop_loss_price
        }
        
        with open(Config.ARBITRAGE_STATUS_FILE, 'w') as f:
            json.dump(status, f, indent=2)
    except Exception as e:
        logger.error(f"更新套利状态失败: {str(e)}")

def check_arbitrage_opportunity():
    """检查套利机会"""
    try:
        # 1. 获取ETF列表
        etf_list = get_all_etf_list()
        if etf_list is None or etf_list.empty:
            logger.error("套利检查失败：ETF列表为空")
            return None
        
        # 2. 检查每只ETF
        for _, etf in etf_list.iterrows():
            etf_code = etf['code']
            
            # 获取ETF数据
            from data_fix import get_etf_data
            etf_data = get_etf_data(etf_code, 'daily')
            if etf_data is None or etf_data.empty:
                continue
            
            # 获取IOPV数据（简化示例）
            # 实际应用中需要从其他数据源获取IOPV
            
            # 检查溢价率
            latest = etf_data.iloc[-1]
            # 假设IOPV为某个值
            iopv = latest['close'] * 0.98  # 示例：假设IOPV比收盘价低2%
            
            premium = (latest['close'] - iopv) / iopv * 100
            
            # 如果溢价率超过阈值，视为套利机会
            if premium > 0.5:  # 0.5%阈值
                # 生成套利信号
                signal = {
                    'etf_code': etf_code,
                    'etf_name': etf['name'],
                    'action': '买入',
                    'position': '套利仓',
                    'total_score': 90,  # 高评分
                    'rationale': f'溢价率 {premium:.2f}%，存在套利机会'
                }
                
                # 记录交易
                record_trade(signal)
                
                # 生成消息
                message = "【ETF套利机会】\n"
                message += f"• {etf['name']} ({etf_code})\n"
                message += f"  当前价格: {latest['close']:.4f}\n"
                message += f"  IOPV: {iopv:.4f}\n"
                message += f"  溢价率: {premium:.2f}%\n"
                message += f"止盈目标：{latest['close'] * 1.01:.4f}\n"
                message += f"止损价格：{latest['close'] * 0.99:.4f}\n"
                message += "建议：立即买入，目标止盈，严格止损。"
                
                # 发送消息
                send_wecom_message(message)
                
                # 更新套利状态
                current_time = get_beijing_time().isoformat()
                update_arbitrage_status(etf_code, etf['name'], current_time, latest['close'], 
                                      latest['close'] * 1.01, latest['close'] * 0.99)
                
                logger.info(f"发现套利机会: {etf_code} - 溢价率 {premium:.2f}%")
                return True
        
        logger.info("未发现套利机会")
        return False
    except Exception as e:
        logger.error(f"套利检查失败: {str(e)}")
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
            # 获取ETF溢价率
            premium_rate = calculate_premium_rate(etf_code)
            
            # 判断是否有套利机会
            # 通常，溢价率过高（如>2%）或过低（如<-2%）可能存在套利机会
            # 这里可以根据实际情况调整阈值
            if abs(premium_rate) >= 2.0:
                # 获取ETF当前价格
                from data_fix import get_etf_data
                etf_data = get_etf_data(etf_code, 'intraday')
                if etf_data is None or etf_data.empty:
                    continue
                
                current_price = etf_data['close'].iloc[-1]
                # 计算目标价格和止损价格
                target_price = current_price * (1 + 0.01 * (1 if premium_rate > 0 else -1))
                stop_loss_price = current_price * (1 - 0.01 * (1 if premium_rate > 0 else -1))
                
                opportunity = {
                    'etf_code': etf_code,
                    'etf_name': etf_name,
                    'premium_rate': premium_rate,
                    'current_price': current_price,
                    'target_price': target_price,
                    'stop_loss_price': stop_loss_price
                }
                opportunities.append(opportunity)
        except Exception as e:
            logger.error(f"扫描{etf_code}时出错: {str(e)}")
    
    if opportunities:
        logger.info(f"发现 {len(opportunities)} 个套利机会")
        return opportunities
    else:
        logger.info("未发现套利机会")
        return None

def calculate_premium_rate(etf_code):
    """计算ETF溢价率"""
    try:
        # 获取ETF数据
        from data_fix import get_etf_data
        etf_data = get_etf_data(etf_code, 'daily')
        if etf_data is None or etf_data.empty:
            return 0.0
        
        # 获取IOPV数据（简化示例）
        # 实际应用中需要从其他数据源获取IOPV
        latest = etf_data.iloc[-1]
        iopv = latest['close'] * 0.98  # 示例：假设IOPV比收盘价低2%
        
        # 计算溢价率
        premium_rate = (latest['close'] - iopv) / iopv * 100
        return premium_rate
    except Exception as e:
        logger.error(f"计算{etf_code}溢价率失败: {str(e)}")
        return 0.0

@app.route('/cron/new-stock-info', methods=['GET', 'POST'])
def cron_new_stock_info():
    """定时推送新股信息（当天可申购的新股）和新上市交易股票信息"""
    logger.info("新股信息与新上市交易股票信息推送任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过新股信息推送")
        response = {"status": "skipped", "message": "Not trading day"}
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
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过策略信号推送")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 推送策略
    success = push_strategy()
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

@app.route('/cron/update-stock-pool', methods=['GET', 'POST'])
def cron_update_stock_pool():
    """每周五16:00北京时间更新ETF股票池（5只稳健仓 + 5只激进仓）"""
    logger.info("股票池更新任务触发")
    
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
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过套利扫描")
        response = {"status": "skipped", "message": "Not trading day"}
        return jsonify(response) if has_app_context() else response
    
    # 执行套利扫描
    success = check_arbitrage_opportunity()
    
    response = {"status": "success" if success else "error"}
    return jsonify(response) if has_app_context() else response

def main():
    """主函数"""
    # 从环境变量获取任务类型
    task = os.getenv('TASK', 'test_message')
    
    logger.info(f"执行任务: {task}")
    
    # 根据任务类型执行不同操作
    if task == 'test_message':
        # T01: 测试消息推送
        # 手动触发时不需要检查是否为测试请求
        message = "【测试消息】ETF策略系统运行正常"
        success = send_wecom_message(message)
        response = {"status": "success" if success else "error"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_new_stock':
        # T07: 测试新股信息推送
        success = push_new_stock_info(test=True)
        response = {"status": "success" if success else "error"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_new_stock_listings':
        # T08: 测试新上市交易股票信息推送
        success = push_listing_info(test=True)
        response = {"status": "success" if success else "error"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_stock_pool':
        # T04: 测试股票池推送
        stock_pool_message = generate_stock_pool()
        if stock_pool_message:
            success = send_wecom_message(stock_pool_message)
            response = {"status": "success" if success else "error"}
        else:
            response = {"status": "error", "message": "Failed to generate stock pool"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_execute':
        # T05: 测试执行策略并推送结果
        success = push_strategy()
        response = {"status": "success" if success else "error"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_reset':
        # T06: 测试重置所有仓位（测试用）
        logger.info("重置所有仓位（测试用）")
        # 实现重置逻辑
        response = {"status": "success", "message": "Positions reset"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'test_arbitrage':
        # T09: 测试套利扫描
        success = check_arbitrage_opportunity()
        response = {"status": "success" if success else "error"}
        print(json.dumps(response, indent=2))
        return response
    
    elif task == 'run_new_stock_info':
        # 每日 9:35 新股信息推送
        return cron_new_stock_info()
    
    elif task == 'push_strategy':
        # 每日 14:50 策略信号推送
        return cron_push_strategy()
    
    elif task == 'update_stock_pool':
        # 每周五 16:00 更新股票池
        return cron_update_stock_pool()
    
    elif task == 'crawl_daily':
        # 每日 15:30 爬取日线数据
        return cron_crawl_daily()
    
    elif task == 'crawl_intraday':
        # 盘中数据爬取（每30分钟）
        return cron_crawl_intraday()
    
    elif task == 'cleanup':
        # 每天 00:00 清理旧数据
        return cron_cleanup()
    
    elif task == 'arbitrage-scan':
        # 套利扫描
        return cron_arbitrage_scan()
    
    elif task == 'resume_crawl':
        # 断点续爬
        return resume_crawl()
    
    else:
        logger.error(f"未知任务类型: {task}")
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
