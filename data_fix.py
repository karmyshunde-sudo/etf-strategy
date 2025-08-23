"""数据源处理模块 - 仅包含数据爬取、缓存和清理相关功能"""
import os
import time
import random
import pandas as pd
import numpy as np
import datetime
import pytz
import requests
import json
import akshare as ak
import baostock as bs
import subprocess
from config import Config
from logger import get_logger
from retrying import retry

# 确保所有数据目录存在（关键修复）
Config.init_directories()

logger = get_logger(__name__)

def get_beijing_time():
    """获取北京时间"""
    beijing_tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(beijing_tz)

def is_trading_day():
    """检查今天是否为交易日"""
    # 获取北京时间
    beijing_time = get_beijing_time()
    
    # 检查是否为周末
    if beijing_time.weekday() >= 5:  # 5=周六, 6=周日
        return False
    
    # 检查是否为节假日（这里简化处理，实际应查询中国节假日）
    # 可以添加具体节假日检查逻辑
    
    return True

def check_data_completeness(df, required_columns=None, min_records=5):
    """
    检查数据完整性
    
    参数:
        df: DataFrame 数据
        required_columns: 必需的列名列表
        min_records: 最小记录数
        
    返回:
        bool: 数据是否完整
    """
    if df is None or df.empty:
        return False
    
    # 检查必需的列
    if required_columns is None:
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"数据缺少必要列: {missing_columns}")
        return False
    
    # 检查数据量
    if len(df) < min_records:
        logger.warning(f"数据量不足，仅 {len(df)} 条记录（需要至少 {min_records} 条）")
        return False
    
    # 检查关键字段是否为空
    for col in required_columns:
        if df[col].isnull().all():
            logger.warning(f"数据中{col}字段全为空")
            return False
    
    return True

def check_new_stock_completeness(df):
    """检查新股数据完整性"""
    required_columns = ['股票代码', '股票简称', '发行价格', '申购上限', '申购日期']
    return check_data_completeness(df, required_columns, min_records=1)

def check_new_listing_completeness(df):
    """检查新上市交易数据完整性"""
    required_columns = ['股票代码', '股票简称', '发行价格', '上市日期']
    return check_data_completeness(df, required_columns, min_records=1)

def check_etf_list_completeness(df):
    """检查ETF列表数据完整性"""
    if df is None or df.empty:
        return False
    
    # 检查是否有足够的ETF（假设至少有50只ETF）
    if len(df) < 50:
        logger.warning(f"ETF列表数据量不足，仅 {len(df)} 只")
        return False
    
    # 检查是否有必要的列
    if '基金代码' in df.columns and '基金简称' in df.columns:
        return True
    elif 'code' in df.columns and 'name' in df.columns:
        return True
    
    logger.warning("ETF列表数据缺少必要列")
    return False

def get_new_stock_subscriptions(test=False):
    """获取新股申购信息
    参数:
        test: 是否为测试模式（测试模式下若当天无数据则回溯21天）
    """
    try:
        today = get_beijing_time().strftime('%Y-%m-%d')
        logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {today} 的新股申购信息...")
        
        # 如果是测试模式，准备回溯21天
        if test:
            dates_to_try = [
                (datetime.datetime.now().date() - datetime.timedelta(days=i))
                for i in range(0, 22)
            ]
        else:
            dates_to_try = [datetime.datetime.now().date()]
        
        for date_obj in dates_to_try:
            date_str = date_obj.strftime('%Y-%m-%d')
            logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {date_str} 的新股申购数据")
            
            # 尝试AkShare（主数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取新股申购信息...")
                # 使用带超时的请求和重试机制
                df = akshare_retry(ak.stock_xgsglb_em, timeout=20)
                
                if not df.empty:
                    # 动态匹配日期列
                    date_col = next((col for col in df.columns if '申购日期' in col), None)
                    if date_col and date_col in df.columns:
                        # 确保日期列是正确格式
                        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                            try:
                                df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        # 筛选目标日期数据
                        df = df[df[date_col] == date_str]
                        if not df.empty:
                            # 检查数据完整性
                            if check_new_stock_completeness(df):
                                logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(df)} 条新股申购信息")
                                return df[['股票代码', '股票简称', '发行价格', '申购上限', '申购日期']]
                            else:
                                logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的新股数据不完整，将尝试备用数据源...")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取新股信息失败: {str(e)}")
            
            # 尝试Baostock（备用数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从Baostock获取新股申购信息...")
                # 使用正确的接口
                lg = bs.login()
                if lg.error_code != '0':
                    logger.warning(f"Baostock登录失败: {lg.error_msg}")
                    raise Exception("Baostock登录失败")
                
                # 使用正确的接口获取新股数据
                rs = bs.query_zh_a_xgsglb()
                if rs.error_code != '0':
                    logger.error(f"Baostock查询失败: {rs.error_msg}")
                    raise Exception("Baostock查询失败")
                
                # 转换为DataFrame
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())
                df = pd.DataFrame(data_list, columns=rs.fields)
                
                if not df.empty:
                    # 标准化日期格式
                    df['ipoDate'] = pd.to_datetime(df['ipoDate']).dt.strftime('%Y-%m-%d')
                    df = df[df['ipoDate'] == date_str]
                    if not df.empty and check_new_stock_completeness(df):
                        logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新股申购信息")
                        return df[['code', 'code_name', 'price', 'max_purchase', 'ipoDate']].rename(columns={
                            'code': '股票代码',
                            'code_name': '股票简称',
                            'price': '发行价格',
                            'max_purchase': '申购上限',
                            'ipoDate': '申购日期'
                        })
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: Baostock获取新股信息失败: {str(e)}")
            finally:
                try:
                    bs.logout()
                except:
                    pass
        
        logger.info(f"{'测试模式' if test else '正常模式'}: 未找到新股数据")
        return pd.DataFrame()
        
    except Exception as e:
        error_msg = f"{'测试模式' if test else '正常模式'}: 【数据错误】获取新股申购信息失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return pd.DataFrame()

def get_new_stock_listings(test=False):
    """获取新上市交易的新股信息
    参数:
        test: 是否为测试模式（测试模式下若当天无数据则回溯21天）
    """
    try:
        today = get_beijing_time().strftime('%Y-%m-%d')
        logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {today} 的新上市交易信息...")
        
        # 如果是测试模式，准备回溯21天
        if test:
            dates_to_try = [
                (datetime.datetime.now().date() - datetime.timedelta(days=i))
                for i in range(0, 22)
            ]
        else:
            dates_to_try = [datetime.datetime.now().date()]
        
        for date_obj in dates_to_try:
            date_str = date_obj.strftime('%Y-%m-%d')
            logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {date_str} 的新上市交易数据")
            
            # 尝试AkShare（主数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取新上市交易信息...")
                df = ak.stock_xgsglb_em()
                if not df.empty:
                    # 动态匹配上市日期列
                    listing_date_col = next((col for col in df.columns if '上市日期' in col), None)
                    if listing_date_col and listing_date_col in df.columns:
                        # 确保日期列是正确格式
                        if not pd.api.types.is_datetime64_any_dtype(df[listing_date_col]):
                            try:
                                df[listing_date_col] = pd.to_datetime(df[listing_date_col]).dt.strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        # 筛选目标日期数据
                        df = df[df[listing_date_col] == date_str]
                        if not df.empty:
                            # 检查数据完整性
                            if check_new_listing_completeness(df):
                                logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(df)} 条新上市交易信息")
                                return df[['股票代码', '股票简称', '发行价格', '上市日期']]
                            else:
                                logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的新上市交易数据不完整，将尝试备用数据源...")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取新上市交易信息失败: {str(e)}")
            
            # 尝试Baostock（备用数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从Baostock获取新上市交易信息...")
                lg = bs.login()
                if lg.error_code != '0':
                    logger.warning(f"Baostock登录失败: {lg.error_msg}")
                    raise Exception("Baostock登录失败")
                
                # 获取所有股票信息
                rs = bs.query_stock_basic()
                if rs.error_code != '0':
                    logger.error(f"Baostock查询失败: {rs.error_msg}")
                    raise Exception("Baostock查询失败")
                
                # 转换为DataFrame
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())
                df = pd.DataFrame(data_list, columns=rs.fields)
                
                if not df.empty:
                    # 标准化日期格式
                    df['list_date'] = pd.to_datetime(df['list_date']).dt.strftime('%Y-%m-%d')
                    df = df[df['list_date'] == date_str]
                    if not df.empty and check_new_listing_completeness(df):
                        logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新上市交易信息")
                        return df[['code', 'code_name', 'issue_price', 'list_date']].rename(columns={
                            'code': '股票代码',
                            'code_name': '股票简称',
                            'issue_price': '发行价格',
                            'list_date': '上市日期'
                        })
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: Baostock获取新上市交易信息失败: {str(e)}")
            finally:
                try:
                    bs.logout()
                except:
                    pass
        
        logger.info(f"{'测试模式' if test else '正常模式'}: 未找到新上市交易数据")
        return pd.DataFrame()
        
    except Exception as e:
        error_msg = f"{'测试模式' if test else '正常模式'}: 【数据错误】获取新上市交易信息失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return pd.DataFrame()

def get_all_etf_list():
    """从多数据源获取所有ETF列表
    返回:
        DataFrame: ETF列表，包含代码和名称
    """
    # 尝试AkShare（主数据源）
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        df = ak.fund_etf_hist_sina(symbol="etf")
        if not df.empty:
            # 检查数据完整性
            if check_etf_list_completeness(df):
                logger.info(f"从AkShare成功获取 {len(df)} 只ETF")
                # 格式化ETF代码
                df['code'] = df['基金代码'].apply(lambda x: f"sh.{x}" if x.startswith('5') else f"sz.{x}")
                return df[['code', '基金简称']].rename(columns={'基金简称': 'name'})
            else:
                logger.warning("AkShare返回的ETF列表不完整，将尝试备用数据源...")
        else:
            logger.warning("AkShare返回空ETF列表")
    
    except Exception as e:
        logger.error(f"AkShare获取ETF列表失败: {str(e)}")
    
    # 尝试Baostock（备用数据源）
    try:
        logger.info("尝试从Baostock获取ETF列表...")
        lg = bs.login()
        if lg.error_code != '0':
            logger.warning(f"Baostock登录失败: {lg.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 获取ETF列表
        rs = bs.query_etf_basic()
        if rs.error_code != '0':
            logger.error(f"Baostock查询ETF列表失败: {rs.error_msg}")
            raise Exception("Baostock查询失败")
        
        # 转换为DataFrame
        etf_list = []
        while (rs.error_code == '0') & rs.next():
            etf_list.append(rs.get_row_data())
        
        if etf_list:
            df = pd.DataFrame(etf_list, columns=rs.fields)
            # 格式化ETF代码
            df['code'] = df['code'].apply(lambda x: f"sh.{x}" if x.startswith('5') else f"sz.{x}")
            logger.info(f"从Baostock成功获取 {len(df)} 只ETF")
            return df[['code', 'code_name']].rename(columns={'code_name': 'name'})
    
    except Exception as e:
        logger.error(f"Baostock获取ETF列表失败: {str(e)}")
    
    error_msg = "【数据错误】无法从所有数据源获取ETF列表"
    logger.error(error_msg)
    send_wecom_message(error_msg)
    return pd.DataFrame()

def get_etf_data(etf_code, data_type='daily'):
    """从多数据源获取ETF数据（增量获取）
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        DataFrame: ETF数据或None（如果所有数据源都失败）
    """
    # 首先检查缓存
    cached_data = load_from_cache(etf_code, data_type)
    if cached_data is not None and not cached_data.empty:
        logger.info(f"使用缓存数据 {etf_code} ({len(cached_data)}条记录)")
        
        # 获取起始日期（从缓存中获取最后日期）
        last_date = cached_data['date'].max()
        start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"ETF {etf_code} 已有
