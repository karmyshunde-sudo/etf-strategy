"""
鱼盆ETF投资量化模型 - 时间处理工具
说明:
  本文件提供时间相关的工具函数
  所有文件放在根目录，简化导入关系
"""

import datetime
import pytz
from config import Config
from logger import get_logger

logger = get_logger(__name__)

def get_beijing_time():
    """获取当前北京时间(UTC+8)"""
    beijing_tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(beijing_tz)

def convert_to_beijing_time(dt):
    """
    将datetime对象转换为北京时间(UTC+8)
    参数:
        dt: datetime对象
    返回:
        转换为北京时间的datetime对象
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)  # 若无时区信息，假设为UTC
    beijing_tz = pytz.timezone('Asia/Shanghai')
    return dt.astimezone(beijing_tz)  # 转换为北京时间

def is_trading_day(date=None):
    """
    检查指定日期是否为交易日（中国股市）
    参数:
        date: datetime对象，默认为今天
    返回:
        bool: 是交易日返回True，否则返回False
    """
    if date is None:
        date = get_beijing_time()
    beijing_date = date.date()
    
    # 简单检查：周一至周五且不是已知节假日
    # 实际实现中，应检查节假日日历
    if beijing_date.weekday() < 5:  # 周一=0，周日=6
        return True
    return False

def is_trading_time():
    """
    检查当前时间是否在中国股市交易时段
    返回:
        bool: 是交易时间返回True，否则返回False
        str: 当前交易时段类型
    """
    now = get_beijing_time()
    beijing_time = now.time()
    
    # 集合竞价时段 (9:15-9:25)
    if datetime.time(9, 15) <= beijing_time < datetime.time(9, 25):
        return True, 'pre_market'
    # 早盘交易时段 (9:30-11:30)
    elif datetime.time(9, 30) <= beijing_time < datetime.time(11, 30):
        return True, 'morning'
    # 午盘交易时段 (13:00-15:00)
    elif datetime.time(13, 0) <= beijing_time < datetime.time(15, 0):
        return True, 'afternoon'
    # 收盘后时段
    else:
        return False, 'post_market'
