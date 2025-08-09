"""
鱼盆ETF投资量化模型 - 企业微信消息模块
说明:
  本文件负责向企业微信推送消息
  所有文件放在根目录，简化导入关系
"""

import requests
from config import Config
from logger import get_logger

logger = get_logger(__name__)

def send_wecom_message(message):
    """
    通过企业微信webhook发送消息
    参数:
        message: 消息内容
    返回:
        bool: 发送成功返回True，否则返回False
    """
    if not Config.WECOM_WEBHOOK:
        logger.error("未配置WECOM_WEBHOOK")
        return False
    
    try:
        # 为企业微信格式化消息负载
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        # 向企业微信webhook发送POST请求
        response = requests.post(Config.WECOM_WEBHOOK, json=payload)
        response.raise_for_status()
        
        # 记录成功日志
        logger.info("消息成功发送至企业微信")
        return True
    except Exception as e:
        logger.error(f"发送消息至企业微信失败: {str(e)}")
        return False