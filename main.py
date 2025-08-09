"""
鱼盆ETF投资量化模型
版本: 6.1
说明:
  本文件仅作为应用入口，不包含具体业务逻辑。
  所有功能已拆分为独立模块，所有文件放在根目录（单层结构）。
  交易流水永久保存（满足收益率统计需求），其他数据保留10年。
  严格区分新股申购和新上市股票，只推送当天可申购的新股。
"""

from flask import Flask
from config import Config
from api import register_api
from logger import get_logger

# 创建日志记录器
logger = get_logger(__name__)

# 创建Flask应用
app = Flask(__name__)
app.config.from_object(Config)

# 注册API端点
register_api(app)

if __name__ == '__main__':
    logger.info("启动鱼盆ETF投资量化模型...")
    logger.info(f"应用运行在端口: {app.config['PORT']}")
    app.run(host='0.0.0.0', port=int(app.config['PORT']), debug=False)