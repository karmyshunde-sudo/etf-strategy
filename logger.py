"""
日志记录
"""

import logging
import os
from datetime import datetime

def get_logger(name):
    """
    获取配置好的日志记录器
    参数:
        name: 日志记录器名称
    返回:
        logger: 配置好的日志记录器
    """
    # 延迟导入Config，解决循环导入问题 - 必须放在使用Config之前
    from config import Config
    
    # 确保错误日志目录存在
    os.makedirs(Config.ERROR_LOG_DIR, exist_ok=True)
    
    # 配置日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 创建日志记录器
    logger = logging.getLogger(name)
    
    # 使用环境变量优先，否则使用配置文件，最后使用默认值
    log_level_str = os.getenv('LOG_LEVEL', Config.LOG_LEVEL).upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)
    
    # 创建文件处理器（仅记录错误）
    log_file = os.path.join(Config.ERROR_LOG_DIR, f"error_{datetime.now().strftime('%Y%m%d')}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.ERROR)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
