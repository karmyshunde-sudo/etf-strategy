"""
鱼盆ETF投资量化模型 - 主入口
说明:
  本文件是应用入口，不包含具体业务逻辑
  所有功能已拆分为独立模块
  所有文件放在根目录（单层结构），简化路径配置
"""

import os
import sys
from config import Config
from api import register_api
from logger import get_logger
import time

# 初始化数据目录
Config.init_directories()

# 创建日志记录器
logger = get_logger(__name__)

try:
    # 验证关键配置
    if not Config.WECOM_WEBHOOK:
        raise ValueError("WECOM_WEBHOOK 未设置！请在 GitHub Secrets 中添加 WECOM_WEBHOOK")
    if not Config.TUSHARE_TOKEN:
        logger.warning("TUSHARE_TOKEN 未设置，部分数据源可能不可用")
except ValueError as e:
    logger.critical(f"配置验证失败: {str(e)}")
    raise

def main():
    """主执行函数"""
    logger.info("启动鱼盆ETF投资量化模型...")
    
    # 从环境变量获取要执行的任务
    task = os.getenv('TASK', 'all')
    logger.info(f"执行任务: {task}")
    
    try:
        if task == 'new_stock_info' or task == 'all':
            from api import push_new_stock_info
            logger.info("开始新股信息推送...")
            success = push_new_stock_info()
            if success:
                logger.info("新股信息推送完成")
            else:
                logger.warning("新股信息推送可能未完成")
        
        if task == 'test_message' or task == 'all':
            from api import test_message
            logger.info("开始测试消息推送...")
            test_message()
            logger.info("测试消息推送完成")
        
        if task == 'test_new_stock' or task == 'all':
            from api import test_new_stock
            logger.info("开始测试新股信息推送...")
            test_new_stock()
            logger.info("测试新股信息推送完成")
        
        if task == 'update_stock_pool' or task == 'all':
            from stock_pool import update_stock_pool
            logger.info("开始更新股票池...")
            update_stock_pool()
            logger.info("股票池更新完成")
        
        if task == 'push_strategy' or task == 'all':
            from calculation import push_strategy_results
            logger.info("开始策略推送...")
            push_strategy_results()
            logger.info("策略推送完成")
        
        # 添加其他任务...
        
    except Exception as e:
        logger.error(f"执行过程中出错: {str(e)}")
        raise

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger = get_logger(__name__)
        logger.critical(f"程序执行失败: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # 保持连接几秒确保日志记录
        time.sleep(5)
