"""
鱼盆ETF投资量化模型 - 配置管理
说明:
  本文件管理所有配置参数和环境变量
  所有文件放在根目录，简化路径配置
"""

import os
from datetime import timedelta

class Config:
    """基础配置类"""
    # 企业微信配置
    WECOM_WEBHOOK = os.getenv('WECOM_WEBHOOK', '')
    CRON_SECRET = os.getenv('CRON_SECRET', '')
    
    # 数据保留策略
    OTHER_DATA_RETENTION_DAYS = 3650  # 10年
    TRADE_LOG_RETENTION_DAYS = None  # 永久保存（None表示不清理）
    
    # 数据目录（所有数据目录在ETF_data根目录下）
    BASE_DATA_DIR = '/content/drive/MyDrive/ETF_data'
    RAW_DATA_DIR = os.path.join(BASE_DATA_DIR, 'raw')
    STOCK_POOL_DIR = os.path.join(BASE_DATA_DIR, 'stock_pool')
    TRADE_LOG_DIR = os.path.join(BASE_DATA_DIR, 'trade_log')
    ARBITRAGE_DIR = os.path.join(BASE_DATA_DIR, 'arbitrage')
    ERROR_LOG_DIR = os.path.join(BASE_DATA_DIR, 'error_log')
    NEW_STOCK_DIR = os.path.join(BASE_DATA_DIR, 'new_stock')  # 新增新股数据目录
    
    # Tushare配置
    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', '')
    
    # 端口配置
    PORT = os.getenv('PORT', '5000')
    
    # 日志配置
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # 新股信息推送配置
    NEW_STOCK_INFO_PUSHED_FLAG = os.path.join(BASE_DATA_DIR, 'new_stock_pushed.flag')
    NEW_STOCK_RETRY_FLAG = os.path.join(BASE_DATA_DIR, 'new_stock_retry.flag')
    
    # 确保所有目录存在
    @classmethod
    def init_directories(cls):
        """初始化所有数据目录"""
        for directory in [
            cls.RAW_DATA_DIR,
            cls.STOCK_POOL_DIR,
            cls.TRADE_LOG_DIR,
            cls.ARBITRAGE_DIR,
            cls.ERROR_LOG_DIR,
            cls.NEW_STOCK_DIR
        ]:
            os.makedirs(directory, exist_ok=True)
