"""
鱼盆ETF投资量化模型 - API端点定义
说明:
  本文件定义所有API端点
  所有文件放在根目录，简化导入关系
"""
from flask import request, jsonify
from config import Config
from scoring import generate_stock_pool, get_current_stock_pool, get_top_n_etfs
from calculation import push_strategy_results, calculate_etf_strategy
from wecom import send_wecom_message
from logger import get_logger
from time_utils import get_beijing_time, convert_to_beijing_time, is_trading_day, is_trading_time
import pandas as pd
import time
import os

logger = get_logger(__name__)

def register_api(app):

    """注册所有API端点"""
    # 定时任务端点
    app.add_url_rule('/cron/crawl_daily', 'crawl_daily', cron_crawl_daily, methods=['POST'])
    app.add_url_rule('/cron/crawl_intraday', 'crawl_intraday', cron_crawl_intraday, methods=['POST'])
    app.add_url_rule('/cron/update_stock_pool', 'update_stock_pool', cron_update_stock_pool, methods=['POST'])
    app.add_url_rule('/cron/push_strategy', 'push_strategy', cron_push_strategy, methods=['POST'])
    app.add_url_rule('/cron/arbitrage_scan', 'arbitrage_scan', cron_arbitrage_scan, methods=['POST'])
    app.add_url_rule('/cron/cleanup', 'cleanup', cron_cleanup, methods=['POST'])
    app.add_url_rule('/cron/new-stock-info', 'new_stock_info', cron_new_stock_info, methods=['POST'])
    
    # 测试端点
    app.add_url_rule('/test/message', 'test_message', test_message, methods=['GET'])
    app.add_url_rule('/test/strategy', 'test_strategy', test_strategy, methods=['GET'])
    app.add_url_rule('/test/trade-log', 'test_trade_log', test_trade_log, methods=['GET'])
    app.add_url_rule('/test/stock-pool', 'test_stock_pool', test_stock_pool, methods=['GET'])
    app.add_url_rule('/test/execute', 'test_execute', test_execute, methods=['GET'])
    app.add_url_rule('/test/reset', 'test_reset', test_reset, methods=['GET'])
    app.add_url_rule('/test/new-stock', 'test_new_stock', test_new_stock, methods=['GET'])
    app.add_url_rule('/test/new-stock-info', 'test_new_stock_info', test_new_stock_info, methods=['GET'])
    
    # 健康检查
    app.add_url_rule('/health', 'health_check', health_check, methods=['GET'])

def cron_crawl_daily():
    """日线数据爬取任务"""
    if request.args.get('secret') != Config.CRON_SECRET:
        return jsonify({"error": "Invalid secret"}), 401
    
    # 实际实现中会调用数据爬取函数
    logger.info("日线数据爬取任务触发")
    return jsonify({"status": "success", "message": "Daily data crawl triggered"})

def cron_crawl_intraday():
    """盘中数据爬取任务"""
    if request.args.get('secret') != Config.CRON_SECRET:
        return jsonify({"error": "Invalid secret"}), 401
    
    # 实际实现中会调用数据爬取函数
    logger.info("盘中数据爬取任务触发")
    return jsonify({"status": "success", "message": "Intraday data crawl triggered"})

def cron_update_stock_pool():
    """股票池更新任务"""
    if request.args.get('secret') != Config.CRON_SECRET:
        return jsonify({"error": "Invalid secret"}), 401
    
    from stock_pool import update_stock_pool
    result = update_stock_pool()
    if result is None:
        return jsonify({"status": "skipped", "message": "Not Friday or before 16:00"})
    
    return jsonify({"status": "success", "message": "Stock pool updated"})

def cron_push_strategy():
    """策略推送任务"""
    if request.args.get('secret') != Config.CRON_SECRET:
        return jsonify({"error": "Invalid secret"}), 401
    
    success = push_strategy_results()
    return jsonify({"status": "success" if success else "skipped"})

def push_strategy():
    """
    执行策略计算并推送结果（独立函数，供main.py调用）
    返回:
        dict: 执行结果
    """
    try:
        success = push_strategy_results()
        if success:
            logger.info("策略执行成功")
            return {"status": "success", "message": "Strategy pushed"}
        else:
            logger.warning("策略执行被跳过")
            return {"status": "skipped", "message": "Strategy skipped"}
    except Exception as e:
        logger.error(f"策略执行失败: {str(e)}")
        return {"status": "error", "message": str(e)}

def cron_arbitrage_scan():
    """套利扫描任务"""
    if request.args.get('secret') != Config.CRON_SECRET:
        return jsonify({"error": "Invalid secret"}), 401
    
    # 实际实现中会调用套利扫描函数
    logger.info("套利扫描任务触发")
    return jsonify({"status": "success", "message": "Arbitrage scan triggered"})

def cron_cleanup():
    """数据清理任务"""
    if request.args.get('secret') != Config.CRON_SECRET:
        return jsonify({"error": "Invalid secret"}), 401
    
    from storage import cleanup_old_data
    cleanup_old_data()
    return jsonify({"status": "success", "message": "Old data cleaned"})

def cron_new_stock_info():
    """定时推送新股信息（当天可申购的新股）"""
    if request.args.get('secret') != Config.CRON_SECRET:
        return jsonify({"error": "Invalid secret"}), 401
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过新股信息推送")
        return jsonify({"status": "skipped", "message": "Not trading day"})
    
    # 检查是否已经推送过
    from crawler import is_new_stock_info_pushed
    if is_new_stock_info_pushed():
        logger.info("新股信息已推送，跳过")
        return jsonify({"status": "skipped", "message": "Already pushed"})
    
    # 检查是否在重试时间前
    from crawler import get_new_stock_retry_time
    retry_time = get_new_stock_retry_time()
    if retry_time and retry_time > get_beijing_time():
        logger.info(f"仍在重试等待期，下次尝试时间: {retry_time}")
        return jsonify({"status": "skipped", "message": f"Retry after {retry_time}"})
    
    # 尝试推送
    from crawler import push_new_stock_info
    success = push_new_stock_info()
    
    # 如果失败，设置30分钟后重试
    if not success:
        logger.info("新股信息推送失败，30分钟后重试")
        from crawler import set_new_stock_retry
        set_new_stock_retry()
        return jsonify({"status": "retry", "message": "Will retry in 30 minutes"})
    
    return jsonify({"status": "success", "message": "New stock info pushed"})

def test_message():
    """T01: 测试消息推送"""
    beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
    message = f"CF系统时间：{beijing_time}\n【测试消息】\n这是来自鱼盆ETF系统的测试消息。\nT01: 测试消息推送。"
    
    # 创建临时应用上下文
    from flask import current_app
    send_wecom_message(message)
    
    return jsonify({"status": "success", "message": "Test message sent"})
  
def test_strategy():
    """T02: 测试策略执行（仅返回结果）"""
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        return jsonify({"status": "error", "message": "No stock pool available"})
    
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
    
    return jsonify({"status": "success", "results": results})

def test_trade_log():
    """T03: 打印交易流水"""
    try:
        # 获取所有交易日志文件
        log_files = sorted([f for f in os.listdir(Config.TRADE_LOG_DIR) if f.startswith('trade_log_')])
        if not log_files:
            return jsonify({"status": "error", "message": "No trade logs found"})
        
        # 合并所有交易日志
        all_logs = []
        for log_file in log_files:
            log_path = os.path.join(Config.TRADE_LOG_DIR, log_file)
            log_df = pd.read_csv(log_path)
            all_logs.extend(log_df.to_dict(orient='records'))
        
        return jsonify({
            "status": "success", 
            "trade_log": all_logs,
            "total_records": len(all_logs),
            "file_count": len(log_files)
        })
    except Exception as e:
        logger.error(f"获取交易流水失败: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

def test_stock_pool():
    """T04: 手动推送当前股票池"""
    stock_pool = get_current_stock_pool()
    if stock_pool is None:
        return jsonify({"status": "error", "message": "No stock pool available"})
    
    # 格式化消息
    beijing_time = get_beijing_time().strftime('%Y-%m-%d %H:%M')
    message = f"T04: 手动推送当前股票池\nCF系统时间：{beijing_time}\n【ETF股票池】\n"
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
    
    # 发送消息
    send_wecom_message(message)
    return jsonify({"status": "success", "message": "Stock pool pushed"})

def test_execute():
    """T05: 执行策略并推送结果"""
    success = push_strategy_results()
    return jsonify({"status": "success" if success else "error"})

def test_reset():
    """T06: 重置所有仓位（测试用）"""
    stock_pool = get_current_stock_pool()
    if stock_pool is None or stock_pool.empty:
        return jsonify({"status": "error", "message": "No stock pool available"})
    
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
        message = f"T05: 执行策略并推送结果\n"
        message += f"CF系统时间：{signal['cf_time']}\n"
        message += f"ETF代码：{signal['etf_code']}\n"
        message += f"名称：{signal['etf_name']}\n"
        message += f"操作建议：仓位重置\n"
        message += f"仓位比例：{signal['position']}%\n"
        message += f"策略依据：{signal['rationale']}"
        
        # 推送消息
        send_wecom_message(message)
        
        # 记录交易
        from calculation import log_trade
        log_trade(signal)
        
        # 间隔1分钟
        time.sleep(60)
    
    return jsonify({"status": "success", "message": "All positions reset"})

def test_new_stock():
    """T07: 测试推送新股信息（只推送当天可申购的新股）"""
    # 获取测试用的新股信息
    from crawler import get_test_new_stock_subscriptions, format_new_stock_subscriptions_message
    new_stocks = get_test_new_stock_subscriptions()
    
    # 检查是否获取到新股数据
    if new_stocks.empty:
        logger.error("测试错误：未获取到任何新股信息")
        return {"status": "error", "message": "No test new stocks available"}
    
    # 格式化消息 - 关键修复：添加测试标识前缀
    message = "【测试消息】\nT07: 测试推送新股信息（只推送当天可申购的新股）\n" + format_new_stock_subscriptions_message(new_stocks)
    
    # 发送消息
    success = send_wecom_message(message)
    
    # 检查推送结果
    if success:
        logger.info("测试消息推送成功")
        return {"status": "success", "message": "Test new stocks sent"}
    else:
        logger.error("测试消息推送失败")
        return {"status": "error", "message": "Failed to send test new stocks"}

def test_new_stock_info():
    """T08: 测试推送所有新股申购信息"""
    # 获取测试数据
    from crawler import get_test_new_stock_subscriptions, format_new_stock_subscriptions_message
    new_stocks = get_test_new_stock_subscriptions()
    
    # 推送新股信息
    if not new_stocks.empty:
        message = "【测试消息】T08: 测试推送所有新股申购信息\n" + format_new_stock_subscriptions_message(new_stocks)
        send_wecom_message(message)
    
    return jsonify({"status": "success", "message": "Test new stock info sent"})

def health_check():
    """健康检查"""
    return jsonify({
        "status": "healthy",
        "timestamp": get_beijing_time().isoformat(),
        "environment": "production"
    })

def calculate_strategy(code, name, etf_type):
    """
    计算单个ETF的策略信号
    参数:
        code: ETF代码
        name: ETF名称
        etf_type: ETF类型 ('stable'或'aggressive')
    返回:
        dict: 策略信号
    """
    try:
        # 严格调用实际策略计算函数，无任何示例逻辑
        return calculate_etf_strategy(code, name, etf_type)
    except Exception as e:
        logger.error(f"计算ETF策略失败 {code}: {str(e)}")
        # 返回安全的默认值，但不包含任何示例逻辑
        return {
            'action': 'hold',
            'position': 0,
            'total_score': 0,
            'rationale': f'策略计算失败: {str(e)}'
        }
