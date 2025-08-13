"""
鱼盆ETF投资量化模型 - 配置模块
说明:
  本文件负责存储所有全局配置参数
  所有文件放在根目录，简化导入关系

【策略详细说明 - 配置参数】
1. 数据存储路径：
   - BASE_DIR: 项目基础目录
   - RAW_DATA_DIR: 原始数据存储目录
   - STOCK_POOL_DIR: 股票池存储目录
   - TRADE_LOG_DIR: 交易流水存储目录
   - ERROR_LOG_DIR: 错误日志存储目录
   - NEW_STOCK_DATA_DIR: 新股数据存储目录

2. 新股信息标记文件：
   - NEW_STOCK_PUSHED_FLAG: 标记新股信息是否已推送
   - LISTING_PUSHED_FLAG: 标记新上市交易股票信息是否已推送

3. 企业微信配置：
   - WECOM_WEBHOOK: 企业微信机器人webhook地址
   - MESSAGE_FOOTER: 消息底部附加信息

4. 系统参数：
   - CRON_SECRET: 定时任务验证密钥
   - MAX_RETRIES: 最大重试次数
   - RETRY_DELAY: 重试间隔时间(秒)

5. 数据源配置：
   - AKSHARE_TOKEN: AkShare API令牌
   - BAOSTOCK_USER: Baostock用户名
   - BAOSTOCK_PWD: Baostock密码
   - SINA_FINANCE_URL: 新浪财经API基础URL

6. 日志配置：
   - LOG_LEVEL: 日志级别
   - LOG_FILE: 日志文件路径

【修复说明】
1. 移除了循环导入问题：
   - 删除了错误的 "from config import Config" 语句
   - 保持原有配置逻辑不变
   - 仅修复导入错误，不改变任何业务逻辑

2. 修复方式：
   - 其他文件中使用Config的地方改为函数内部动态导入
   - 例如：在需要Config的函数中添加 "from config import Config"
   - 避免模块级导入导致的循环依赖

【使用说明】
1. 访问配置参数：Config.RAW_DATA_DIR
2. 初始化目录：Config.init_directories()
3. 获取环境变量：os.getenv('VARIABLE_NAME', 'default')
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
    
    # 新股数据存储目录
    NEW_STOCK_DATA_DIR = os.path.join(BASE_DIR, 'data', 'new_stock')
    
    # 新股信息推送状态文件
    NEW_STOCK_PUSHED_FLAG = os.path.join(BASE_DIR, 'data', 'new_stock_pushed.flag')
    
    # 新上市交易股票信息推送状态文件
    LISTING_PUSHED_FLAG = os.path.join(BASE_DIR, 'data', 'listing_pushed.flag')
    
    # 确保所有目录存在
    for directory in [RAW_DATA_DIR, STOCK_POOL_DIR, TRADE_LOG_DIR, 
                     ERROR_LOG_DIR, NEW_STOCK_DATA_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    # 企业微信webhook地址（从环境变量获取）
    WECOM_WEBHOOK = os.getenv('WECOM_WEBHOOK', '')
    
    # 消息底部附加信息
    MESSAGE_FOOTER = "【鱼盆ETF投资量化系统】全自动决策 | 无需人工干预 | 风险提示：市场有风险，投资需谨慎"
    
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
                         cls.ERROR_LOG_DIR, cls.NEW_STOCK_DATA_DIR]:
            # 创建目录（如果不存在）
            os.makedirs(directory, exist_ok=True)
