"""
鱼盆ETF投资量化模型 - 股票池管理模块
说明:
  本文件负责ETF股票池的更新和管理
  保持10只ETF：5只稳健仓，5只激进仓
  所有文件放在根目录，简化导入关系
"""

import os
import time
import pandas as pd
from datetime import datetime
from config import Config
from logger import get_logger
from scoring import generate_stock_pool, get_current_stock_pool
from time_utils import convert_to_beijing_time

logger = get_logger(__name__)

def update_stock_pool():
    """
    更新ETF股票池（5只稳健仓 + 5只激进仓）
    本函数应在每周五16:00北京时间运行
    """
    logger.info("开始股票池更新流程")
    
    # 获取当前北京时间
    beijing_now = convert_to_beijing_time(datetime.now())
    
    # 检查今天是否是周五
    if beijing_now.weekday() != 4:  # 周五是4（周一是0）
        logger.info(f"今天是{beijing_now.strftime('%A')}，不是周五。跳过股票池更新。")
        return None
    
    # 检查时间是否在16:00之后
    if beijing_now.time() < datetime.time(16, 0):
        logger.info(f"当前时间是{beijing_now.strftime('%H:%M')}，早于16:00。跳过股票池更新。")
        return None
    
    # 生成新的股票池
    stock_pool = generate_stock_pool()
    
    if stock_pool is None:
        logger.error("股票池生成失败，无法更新。")
        return None
    
    logger.info(f"股票池更新成功。")
    logger.info(f"选定{len(stock_pool[stock_pool['type'] == '稳健仓'])}只稳健ETF和{len(stock_pool[stock_pool['type'] == '激进仓'])}只激进ETF")
    
    return stock_pool

def get_current_stock_pool():
    """
    获取当前有效的股票池
    返回:
        DataFrame: 当前股票池
    """
    return get_current_stock_pool()
