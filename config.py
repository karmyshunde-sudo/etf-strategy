"""版本250820-Ver2.1"""
import os

class Config:
    """全局配置类，所有配置参数通过类属性访问"""
    
    # 获取项目基础目录
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # 原始数据存储目录（确保目录结构正确）
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
    LISTING_PUSHED_FLAG = os.path.join(BASE_DIR, 'data', 'listing_pushed.flag')
    ARBITRAGE_STATUS_FILE = os.path.join(BASE_DIR, 'data', 'arbitrage_status.json')
    
    # 数据保留策略（天数）
    OTHER_DATA_RETENTION_DAYS = 30  # 其他数据保留30天
    # 交易流水永久保存，不设置保留天数
    
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
    
    # 聚宽配置
    JOINQUANT_USERNAME = os.getenv('JOINQUANT_USERNAME', '13929178188')
    JOINQUANT_PASSWORD = os.getenv('JOINQUANT_PASSWORD', 'aA22280090')
    
    # 日志级别
    LOG_LEVEL = 'INFO'
    
    # 日志文件路径
    LOG_FILE = os.path.join(BASE_DIR, 'etf_strategy.log')
    
    @classmethod
    def init_directories(cls):
        """初始化所有数据目录（确保目录存在）"""
        directories = [
            cls.RAW_DATA_DIR,
            os.path.join(cls.RAW_DATA_DIR, 'etf_data'),
            cls.STOCK_POOL_DIR,
            cls.TRADE_LOG_DIR,
            cls.ERROR_LOG_DIR,
            cls.NEW_STOCK_DIR,
            cls.ARBITRAGE_DIR
        ]
        
        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
                # 创建.gitkeep文件确保目录被Git跟踪
                gitkeep_path = os.path.join(directory, '.gitkeep')
                if not os.path.exists(gitkeep_path):
                    with open(gitkeep_path, 'w') as f:
                        f.write(f"# Git keep file for {directory}\n")
                    print(f"Created .gitkeep in {directory}")
            except Exception as e:
                print(f"创建目录 {directory} 失败: {str(e)}")
