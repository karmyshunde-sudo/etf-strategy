"""版本250820-Ver1.0"""
import os

class Config:
    """全局配置类，所有配置参数通过类属性访问"""
    
    # 获取项目基础目录
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # 原始数据存储目录
    RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
    
    # 股票池存储目录
    STOCK_POOL_DIR = os.path.join(BASE_DIR, 'data', 'stock_pool')
    
    # 交易流水存储目录
    TRADE_LOG_DIR = os.path.join(BASE_DIR, 'data', 'trade_log')
    
    # 错误日志存储目录
    ERROR_LOG_DIR = os.path.join(BASE_DIR, 'data', 'error_log')
    
    # 新股数据存储目录
    NEW_STOCK_DIR = os.path.join(BASE_DIR, 'data', 'new_stock')
    
    # 套利数据存储目录
    ARBITRAGE_DIR = os.path.join(BASE_DIR, 'data', 'arbitrage')
    
    # 状态文件存储目录
    STATUS_DIR = os.path.join(BASE_DIR, 'data', 'status')
    
    # 新股信息推送状态文件
    NEW_STOCK_PUSHED_FLAG = os.path.join(STATUS_DIR, 'new_stock_pushed.flag')
    
    # 新上市交易股票信息推送状态文件
    LISTING_PUSHED_FLAG = os.path.join(STATUS_DIR, 'listing_pushed.flag')
    
    # 套利ETF状态文件
    ARBITRAGE_STATUS_FILE = os.path.join(STATUS_DIR, 'arbitrage_status.json')
    
    # 数据保留天数
    OTHER_DATA_RETENTION_DAYS = 365
    
    # 企业微信webhook地址（从环境变量获取）
    WECOM_WEBHOOK = os.getenv('WECOM_WEBHOOK', '')
    
    # 消息底部附加信息
    MESSAGE_FOOTER = "【鱼盆ETF投资量化系统】全自动决策| 无需人工干预| 版本号250820.10.02"
    
    # 数据完整性检查配置
    MIN_DATA_DAYS = 30  # 最小数据天数
    MAX_DATA_AGE = 1    # 最大数据年龄（天）
    
    # 日志级别 - 关键修复：必须定义此项
    LOG_LEVEL = 'INFO'
    
    @classmethod
    def init_directories(cls):
        """初始化所有数据目录"""
        for directory in [cls.RAW_DATA_DIR, cls.STOCK_POOL_DIR, cls.TRADE_LOG_DIR, 
                         cls.ERROR_LOG_DIR, cls.NEW_STOCK_DIR, cls.ARBITRAGE_DIR, cls.STATUS_DIR]:
            os.makedirs(directory, exist_ok=True)
