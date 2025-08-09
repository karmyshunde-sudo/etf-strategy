"""
鱼盆ETF投资量化模型 - 数据存储模块
说明:
  本文件负责数据的存储和清理
  所有文件放在根目录，简化导入关系
"""

import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
from config import Config
from logger import get_logger

logger = get_logger(__name__)

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
    
    now = datetime.now()
    
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        
        # 获取文件修改时间
        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        
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