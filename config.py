"""
版本250820-Ver1.0

"""

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
    
    # 新股数据存储目录（修正：与main.py保持一致，移除"DATA"）
    NEW_STOCK_DIR = os.path.join(BASE_DIR, 'data', 'new_stock')
    
    # 套利数据存储目录
    ARBITRAGE_DIR = os.path.join(BASE_DIR, 'data', 'arbitrage')
    
    # 新股信息推送状态文件
    NEW_STOCK_PUSHED_FLAG = os.path.join(BASE_DIR, 'data', 'new_stock_pushed.flag')
    
    # 新上市交易股票信息推送状态文件
    LISTING_PUSHED_FLAG = os.path.join(BASE_DIR, 'data', 'listing_pushed.flag')
    
    # 套利ETF状态文件
    ARBITRAGE_STATUS_FILE = os.path.join(BASE_DIR, 'data', 'arbitrage_status.json')
    
    # 数据保留天数
    OTHER_DATA_RETENTION_DAYS = 365
    
    # 确保所有目录存在
    for directory in [RAW_DATA_DIR, STOCK_POOL_DIR, TRADE_LOG_DIR, 
                     ERROR_LOG_DIR, NEW_STOCK_DIR, ARBITRAGE_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    # 企业微信webhook地址（从环境变量获取）
    WECOM_WEBHOOK = os.getenv('WECOM_WEBHOOK', '')
    
    # 消息底部附加信息
    MESSAGE_FOOTER = "【鱼盆ETF投资量化系统】全自动决策 | 无需人工干预 | 版本号250815.10.02"
    
    # 定时任务验证密钥（从环境变量获取）
    CRON_SECRET = os.getenv('CRON_SECRET', 'default-secret')
    
    # 最大重试次数
    MAX_RETRIES = 3
    
    # 重试间隔(秒)
    RETRY_DELAY = 5
    
    # AkShare API令牌
    AKSHARE_TOKEN = None
    
    # Baostock用户名
    BAOSTOCK_USER = ''
    
    # Baostock密码
    BAOSTOCK_PWD = ''
    
    # 新浪财经API基础URL
    SINA_FINANCE_URL = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php'
    
    # 日志级别
    LOG_LEVEL = 'INFO'
    
    # 日志文件路径
    LOG_FILE = os.path.join(BASE_DIR, 'etf_strategy.log')
    
    @classmethod
    def init_directories(cls):
        """初始化所有数据目录"""
        # 遍历所有目录配置
        for directory in [cls.RAW_DATA_DIR, cls.STOCK_POOL_DIR, cls.TRADE_LOG_DIR, 
                         cls.ERROR_LOG_DIR, cls.NEW_STOCK_DIR, cls.ARBITRAGE_DIR]:
            # 创建目录（如果不存在）
            os.makedirs(directory, exist_ok=True)
