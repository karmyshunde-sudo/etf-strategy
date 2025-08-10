"""
鱼盆ETF投资量化模型 - 企业微信集成
说明:
  本文件处理企业微信消息推送
  所有文件放在根目录，简化导入关系
"""

import requests
from config import Config
from logger import get_logger

logger = get_logger(__name__)

def send_wecom_message(message):
    """
    发送消息到企业微信
    参数:
        message: 消息内容
    返回:
        bool: 是否成功
    """
    # 检查配置
    if not Config.WECOM_WEBHOOK:
        logger.error("WECOM_WEBHOOK 未设置，无法发送企业微信消息")
        return False
    
    # 在消息结尾添加全局备注
    if Config.MESSAGE_FOOTER:
        message = f"{message}\n\n{Config.MESSAGE_FOOTER}"
    
    try:
        # 构建消息
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        # 发送请求
        response = requests.post(
            Config.WECOM_WEBHOOK,
            json=payload,
            timeout=10
        )
        
        # 检查响应
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                logger.info("企业微信消息发送成功")
                return True
            else:
                logger.error(f"企业微信API返回错误: {result}")
                return False
        else:
            logger.error(f"企业微信请求失败，状态码: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"发送企业微信消息时出错: {str(e)}")
        return False
