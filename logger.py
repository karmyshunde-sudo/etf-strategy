"""
鱼盆ETF投资量化模型 - 日志处理工具
说明:
  本文件提供日志记录功能
  所有文件放在根目录，简化导入关系
"""

import logging
import os
from datetime import datetime
from config import Config

def get_logger(name):
    """
    获取配置好的日志记录器
    参数:
        name: 日志记录器名称
    返回:
        logger: 配置好的日志记录器
    """
    # 确保错误日志目录存在
    os.makedirs(Config.ERROR_LOG_DIR, exist_ok=True)
    
    # 配置日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 创建文件处理器
    log_file = os.path.join(Config.ERROR_LOG_DIR, f"error_{datetime.now().strftime('%Y%m%d')}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.ERROR)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger