"""
鱼盆ETF投资量化模型 - 主入口文件
说明:
  本文件是系统入口，负责执行策略和处理定时任务
  所有文件放在根目录，简化导入关系

【策略执行流程】
1. 初始化配置：
   - 加载配置参数
   - 初始化日志系统
   - 创建必要目录

2. 执行策略任务：
   - 测试消息推送 (T01)
   - 测试新股申购信息 (T07)
   - 测试股票池推送 (T04)
   - 测试策略推送 (T05)
   - 重置所有仓位 (T06)
   - 运行新股信息 (T07)
   - 每日 9:30 新股信息推送
   - 每日 14:50 策略信号推送
   - 每周五 16:00 更新股票池
   - 每日 15:30 爬取日线数据
   - 盘中数据爬取
   - 每天 00:00 清理旧数据

3. 异常处理机制：
   - 捕获并记录所有异常
   - 确保任务失败后能继续执行
   - 重试机制（针对关键任务）

【修复说明】
1. 移除了循环导入问题：
   - 将Config导入移到函数内部
   - 避免模块级导入导致的循环依赖

2. 移除了Tushare相关代码：
   - 仅保留AkShare、Baostock、新浪财经三个数据源
   - 确保系统兼容最新数据源配置

3. 增强了错误处理：
   - 详细记录错误信息
   - 提供明确的错误提示
   - 确保单个任务失败不影响其他任务
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta

def main():
    """主函数：执行策略任务"""
    # 动态导入Config，避免循环导入问题
    from config import Config
    
    # 动态导入其他模块，避免循环导入
    from logger import get_logger
    from time_utils import is_trading_day, get_beijing_time
    from api import register_api, test_message, test_new_stock, test_stock_pool
    from api import test_execute, test_reset, cron_new_stock_info, push_strategy
    from api import update_stock_pool, crawl_daily, cleanup
    
    # 初始化日志
    logger = get_logger(__name__)
    
    # 检查必要配置
    if not Config.WECOM_WEBHOOK:
        logger.critical("企业微信webhook未配置，请在config.py中设置WECOM_WEBHOOK")
        sys.exit(1)
    
    # 显示系统启动信息
    logger.info("=" * 50)
    logger.info(f"CF系统时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("鱼盆ETF投资量化系统启动")
    logger.info("=" * 50)
    
    # 获取任务名称
    task = os.getenv('TASK', 'run_new_stock_info')
    logger.info(f"执行任务: {task}")
    
    try:
        # 执行不同任务
        if task == 'test_message':
            # T01: 测试消息推送
            logger.info("开始执行: T01 测试消息推送")
            result = test_message()
            logger.info(f"任务执行结果: {result}")
            
        elif task == 'test_new_stock':
            # T07: 测试推送新股信息
            logger.info("开始执行: T07 测试推送新股信息")
            result = test_new_stock()
            logger.info(f"任务执行结果: {result}")
            
        elif task == 'test_stock_pool':
            # T04: 测试推送当前股票池
            logger.info("开始执行: T04 测试推送股票池")
            result = test_stock_pool()
            logger.info(f"任务执行结果: {result}")
            
        elif task == 'test_execute':
            # T05: 执行策略并推送结果
            logger.info("开始执行: T05 执行策略并推送结果")
            result = test_execute()
            logger.info(f"任务执行结果: {result}")
            
        elif task == 'test_reset':
            # T06: 重置所有仓位
            logger.info("开始执行: T06 重置所有仓位")
            result = test_reset()
            logger.info(f"任务执行结果: {result}")
            
        elif task == 'run_new_stock_info':
            # 每日 9:30 新股信息推送
            logger.info("开始执行: 每日新股信息推送")
            result = cron_new_stock_info()
            logger.info(f"任务执行结果: {result}")
            
        elif task == 'push_strategy':
            # 每日 14:50 策略信号推送
            logger.info("开始执行: 每日策略信号推送")
            result = push_strategy()
            logger.info(f"任务执行结果: {result}")
            
        elif task == 'update_stock_pool':
            # 每周五 16:00 更新股票池
            logger.info("开始执行: 每周五更新股票池")
            if datetime.now().weekday() == 4:  # 周五
                result = update_stock_pool()
                logger.info(f"任务执行结果: {result}")
            else:
                logger.info("今天不是周五，跳过股票池更新")
                
        elif task == 'crawl_daily':
            # 每日 15:30 爬取日线数据
            logger.info("开始执行: 每日爬取日线数据")
            from crawler import get_all_etf_list
            etf_list = get_all_etf_list()
            logger.info(f"获取到 {len(etf_list)} 只ETF，开始爬取日线数据...")
            
            for _, etf in etf_list.iterrows():
                from crawler import get_etf_data
                data = get_etf_data(etf['code'])
                if data is not None:
                    logger.info(f"成功获取 {etf['code']} {etf['name']} 日线数据")
                time.sleep(1)  # 避免请求过快
                
        elif task == 'cleanup':
            # 每天 00:00 清理旧数据
            logger.info("开始执行: 清理旧数据")
            from storage import cleanup_old_data
            cleanup_old_data()
            logger.info("旧数据清理完成")
            
        else:
            logger.warning(f"未知任务: {task}")
            logger.info("可用任务: test_message, test_new_stock, test_stock_pool, test_execute, test_reset, "
                       "run_new_stock_info, push_strategy, update_stock_pool, crawl_daily, cleanup")
            
    except Exception as e:
        logger.critical(f"程序执行失败: {str(e)}", exc_info=True)
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("任务执行完成")
    logger.info("=" * 50)

if __name__ == "__main__":
    # 防止循环导入：在main函数内部导入
    main()
