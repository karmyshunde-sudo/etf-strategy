"""版本250826-Ver1148"""
import os

class Config:
    """全局配置类，所有配置参数通过类属性访问"""
    # 关键修复：使用 GITHUB_WORKSPACE 环境变量确保正确获取仓库根目录
    GITHUB_WORKSPACE = os.environ.get('GITHUB_WORKSPACE', 
                                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # 获取项目基础目录
    BASE_DIR = os.path.abspath(GITHUB_WORKSPACE)
    
    # BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    #BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
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
    MESSAGE_FOOTER = "【etf-strategy】20250823-Ver3.0"
    
    # 数据完整性检查配置
    MIN_DATA_DAYS = 30  # 最小数据天数
    MAX_DATA_AGE = 2    # 最大数据年龄（天）
    
    # 日志级别
    LOG_LEVEL = 'INFO'
    
    # 日志文件路径
    LOG_FILE = os.path.join(BASE_DIR, 'etf_strategy.log')
    
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
    
    @classmethod
    def init_directories(cls):
        """初始化所有数据目录（确保目录存在）"""
        directories = [
            cls.RAW_DATA_DIR,
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
