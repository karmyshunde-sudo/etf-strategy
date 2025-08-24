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

# 获取AkShare版本（关键修复：确保总是定义）
try:
    akshare_version = ak.__version__
except Exception as e:
    # 无法使用logger，因为logger可能还未初始化
    print(f"警告: 无法获取AkShare版本信息: {str(e)}")
    akshare_version = "unknown"


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

def standardize_code(code):
    """标准化股票代码格式为 sh.510300 或 sz.159915"""
    if code.startswith(('sh.', 'sz.')):
        return code
    elif code.startswith(('5', '119')):
        return f"sh.{code}"
    else:
        return f"sz.{code}"

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
        logger.warning("数据为空")
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
    """检查新股数据完整性（只严格检查关键字段）"""
    if df is None or df.empty:
        logger.warning("新股数据为空")
        return False
    
    # 只严格检查关键字段
    critical_columns = ['股票代码', '股票简称', '申购日期']
    missing_critical = [col for col in critical_columns if col not in df.columns]
    if missing_critical:
        logger.warning(f"新股数据缺少关键字段: {missing_critical}")
        return False
    
    return True

def check_new_listing_completeness(df):
    """检查新上市交易数据完整性（只严格检查关键字段）"""
    if df is None or df.empty:
        logger.warning("新上市交易数据为空")
        return False
    
    # 只严格检查关键字段
    critical_columns = ['股票代码', '股票简称', '上市日期']
    missing_critical = [col for col in critical_columns if col not in df.columns]
    if missing_critical:
        logger.warning(f"新上市交易数据缺少关键字段: {missing_critical}")
        return False
    
    return True

def check_convertible_bond_completeness(df):
    """检查可转债数据完整性"""
    required_columns = ['债券代码', '债券简称', '转股价格', '申购上限', '申购日期']
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
            ak_df = None
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取新股申购信息...")
                logger.info(f"AkShare接口: ak.stock_xgsglb_em()")
                ak_df = akshare_retry(ak.stock_xgsglb_em)
                
                if not ak_df.empty:
                    # 仅记录列名信息
                    logger.info(f"AkShare返回新股数据列数: {len(ak_df.columns)}")
                    logger.info(f"AkShare返回新股数据记录数: {len(ak_df)}")
                    logger.info(f"AkShare返回新股数据列名: {ak_df.columns.tolist()}")
                    
                    # 动态匹配日期列
                    date_col = next((col for col in ak_df.columns 
                                   if any(kw in col.lower() for kw in ['申购日期', 'ipo_date', 'issue_date'])), None)
                    
                    if date_col and date_col in ak_df.columns:
                        # 确保日期列是正确格式
                        if not pd.api.types.is_datetime64_any_dtype(ak_df[date_col]):
                            try:
                                ak_df[date_col] = pd.to_datetime(ak_df[date_col]).dt.strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        # 筛选目标日期数据
                        ak_df = ak_df[ak_df[date_col] == date_str]
                        if not ak_df.empty:
                            # 只严格检查关键字段
                            if check_new_stock_completeness(ak_df):
                                logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(ak_df)} 条新股申购信息")
                                # 确保列名标准化
                                if '申购日期' not in ak_df.columns:
                                    ak_df['申购日期'] = date_str
                                # 添加类型标识
                                ak_df['类型'] = '股票'
                                return ak_df  # 返回完整数据，不丢弃任何字段
                            else:
                                logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的新股数据缺少关键字段，将尝试备用数据源...")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取新股信息失败: {str(e)}", exc_info=True)
            
            # 尝试获取可转债数据
            cb_df = None
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取可转债申购信息...")
                logger.info(f"尝试AkShare接口: ak.bond_cb_issue_em()")
                
                # 检查AkShare版本是否支持该接口
                if hasattr(ak, 'bond_cb_issue_em'):
                    cb_df = ak.bond_cb_issue_em()
                    logger.info("成功调用ak.bond_cb_issue_em()接口")
                else:
                    logger.warning(f"AkShare版本 {akshare_version} 不支持 bond_cb_issue_em 接口")
                    # 根据版本提供替代方案
                    if akshare_version.startswith('1.'):
                        logger.info("尝试替代接口: ak.bond_cb_em()")
                        if hasattr(ak, 'bond_cb_em'):
                            cb_df = ak.bond_cb_em()
                            logger.info("成功调用ak.bond_cb_em()接口")
                        else:
                            logger.error("AkShare版本过旧，不支持可转债数据接口")
                    elif akshare_version.startswith('0.'):
                        logger.info("尝试替代接口: ak.bond_cb_all()")
                        if hasattr(ak, 'bond_cb_all'):
                            cb_df = ak.bond_cb_all()
                            logger.info("成功调用ak.bond_cb_all()接口")
                        else:
                            logger.error("AkShare版本过旧，不支持可转债数据接口")
                
                if cb_df is not None and not cb_df.empty:
                    # 仅记录列名信息
                    logger.info(f"AkShare返回可转债数据列数: {len(cb_df.columns)}")
                    logger.info(f"AkShare返回可转债数据记录数: {len(cb_df)}")
                    logger.info(f"AkShare返回可转债数据列名: {cb_df.columns.tolist()}")
                    
                    # 动态匹配日期列
                    date_col = next((col for col in cb_df.columns 
                                   if any(kw in col.lower() for kw in ['申购日期', 'subscribe_date'])), None)
                    
                    if date_col and date_col in cb_df.columns:
                        # 确保日期列是正确格式
                        if not pd.api.types.is_datetime64_any_dtype(cb_df[date_col]):
                            try:
                                cb_df[date_col] = pd.to_datetime(cb_df[date_col]).dt.strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        # 筛选目标日期数据
                        cb_df = cb_df[cb_df[date_col] == date_str]
                        if not cb_df.empty:
                            # 只严格检查关键字段
                            if '债券代码' in cb_df.columns and '债券简称' in cb_df.columns:
                                logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(cb_df)} 条可转债申购信息")
                                # 重命名列以匹配股票格式
                                cb_df = cb_df.rename(columns={
                                    '债券代码': '股票代码',
                                    '债券简称': '股票简称',
                                    '转股价格': '发行价格'
                                })
                                # 添加类型标识
                                cb_df['类型'] = '可转债'
                                return cb_df  # 返回完整数据，不丢弃任何字段
                            else:
                                logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的可转债数据缺少关键字段")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取可转债信息失败: {str(e)}", exc_info=True)
            
            # 尝试Baostock（备用数据源）
            bs_df = None
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从Baostock获取新股申购信息...")
                logger.info("Baostock接口: bs.query_stock_new()")
                lg = bs.login()
                if lg.error_code != '0':
                    logger.warning(f"Baostock登录失败: {lg.error_msg}")
                    raise Exception("Baostock登录失败")
                
                # 尝试多种Baostock接口
                try:
                    # 尝试方法1: query_stock_new
                    rs = bs.query_stock_new()
                    if rs.error_code == '0':
                        data_list = []
                        while (rs.error_code == '0') & rs.next():
                            data_list.append(rs.get_row_data())
                        bs_df = pd.DataFrame(data_list, columns=rs.fields)
                        if not bs_df.empty:
                            # 仅记录列名信息
                            logger.info(f"Baostock返回新股数据列数: {len(bs_df.columns)}")
                            logger.info(f"Baostock返回新股数据记录数: {len(bs_df)}")
                            logger.info(f"Baostock返回新股数据列名: {bs_df.columns.tolist()}")
                            
                            # 标准化日期格式
                            bs_df['ipoDate'] = pd.to_datetime(bs_df['ipoDate']).dt.strftime('%Y-%m-%d')
                            bs_df = bs_df[bs_df['ipoDate'] == date_str]
                            if not bs_df.empty:
                                # 创建新的DataFrame，但保留所有字段
                                result_df = pd.DataFrame()
                                result_df['股票代码'] = bs_df['code']
                                result_df['股票简称'] = bs_df['code_name']
                                result_df['申购日期'] = bs_df['ipoDate']
                                
                                # 复制其他所有字段
                                for col in bs_df.columns:
                                    if col not in ['code', 'code_name', 'ipoDate'] and col not in result_df.columns:
                                        result_df[col] = bs_df[col]
                                
                                # 添加类型标识
                                result_df['类型'] = '股票'
                                
                                # 只严格检查关键字段
                                if check_new_stock_completeness(result_df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(result_df)} 条新股申购信息")
                                    return result_df  # 返回完整数据，不丢弃任何字段
                                else:
                                    logger.warning(f"{'测试模式' if test else '正常模式'}: Baostock返回的数据缺少关键字段，跳过此数据源")
                except AttributeError:
                    pass
                
                try:
                    # 尝试方法2: query_stock_basic
                    logger.info("Baostock备用接口: bs.query_stock_basic()")
                    rs = bs.query_stock_basic()
                    if rs.error_code == '0':
                        data_list = []
                        while (rs.error_code == '0') & rs.next():
                            data_list.append(rs.get_row_data())
                        bs_df = pd.DataFrame(data_list, columns=rs.fields)
                        if not bs_df.empty:
                            # 仅记录列名信息
                            logger.info(f"Baostock返回新股数据列数: {len(bs_df.columns)}")
                            logger.info(f"Baostock返回新股数据记录数: {len(bs_df)}")
                            logger.info(f"Baostock返回新股数据列名: {bs_df.columns.tolist()}")
                            
                            # 标准化日期格式
                            bs_df['ipoDate'] = pd.to_datetime(bs_df['ipoDate']).dt.strftime('%Y-%m-%d')
                            bs_df = bs_df[bs_df['ipoDate'] == date_str]
                            if not bs_df.empty:
                                # 创建新的DataFrame，但保留所有字段
                                result_df = pd.DataFrame()
                                result_df['股票代码'] = bs_df['code']
                                result_df['股票简称'] = bs_df['code_name']
                                result_df['申购日期'] = bs_df['ipoDate']
                                
                                # 复制其他所有字段
                                for col in bs_df.columns:
                                    if col not in ['code', 'code_name', 'ipoDate'] and col not in result_df.columns:
                                        result_df[col] = bs_df[col]
                                
                                # 添加类型标识
                                result_df['类型'] = '股票'
                                
                                # 只严格检查关键字段
                                if check_new_stock_completeness(result_df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(result_df)} 条新股申购信息")
                                    return result_df  # 返回完整数据，不丢弃任何字段
                                else:
                                    logger.warning(f"{'测试模式' if test else '正常模式'}: Baostock返回的数据缺少关键字段，跳过此数据源")
                except AttributeError:
                    pass
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: Baostock获取新股信息失败: {str(e)}", exc_info=True)
            finally:
                try:
                    bs.logout()
                except:
                    pass
            
            # 尝试新浪财经（备用数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从新浪财经获取新股申购信息...")
                sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
                response = requests.get(sina_url, timeout=15)
                response.raise_for_status()
                data = response.json()
                new_stocks = []
                for item in data:
                    code = item.get('申购代码', '')
                    name = item.get('股票简称', '')
                    issue_price = item.get('发行价格', '')
                    max_purchase = item.get('申购上限', '')
                    publish_date = item.get('申购日期', '')
                    
                    if publish_date == date_str:
                        new_stocks.append({
                            '股票代码': code,
                            '股票简称': name,
                            '发行价格': issue_price,
                            '申购上限': max_purchase,
                            '申购日期': publish_date
                        })
                
                if new_stocks:
                    logger.info(f"{'测试模式' if test else '正常模式'}: 从新浪财经成功获取 {len(new_stocks)} 条新股申购信息")
                    sina_df = pd.DataFrame(new_stocks)
                    sina_df['类型'] = '股票'
                    return sina_df
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: 新浪财经获取新股信息失败: {str(e)}", exc_info=True)
        
        logger.info(f"{'测试模式' if test else '正常模式'}: 未找到新股数据")
        return pd.DataFrame()
        
    except Exception as e:
        error_msg = f"{'测试模式' if test else '正常模式'}: 【数据错误】获取新股申购信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wecom_message(error_msg)
        return pd.DataFrame()

def get_new_stock_listings(test=False):
    """获取新上市交易的新股信息（只严格检查关键字段，保留所有已有字段）"""
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
            ak_df = None
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取新上市交易信息...")
                logger.info(f"AkShare接口: ak.stock_xgsglb_em()")
                ak_df = ak.stock_xgsglb_em()
                if not ak_df.empty:
                    # 仅记录列名信息
                    logger.info(f"AkShare返回新上市交易数据列数: {len(ak_df.columns)}")
                    logger.info(f"AkShare返回新上市交易数据记录数: {len(ak_df)}")
                    logger.info(f"AkShare返回新上市交易数据列名: {ak_df.columns.tolist()}")
                    
                    # 动态匹配上市日期列
                    listing_date_col = next((col for col in ak_df.columns 
                                           if any(kw in col.lower() for kw in ['上市日期', 'listing_date'])), None)
                    
                    if listing_date_col and listing_date_col in ak_df.columns:
                        # 确保日期列是正确格式
                        if not pd.api.types.is_datetime64_any_dtype(ak_df[listing_date_col]):
                            try:
                                ak_df[listing_date_col] = pd.to_datetime(ak_df[listing_date_col]).dt.strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        # 筛选目标日期数据
                        ak_df = ak_df[ak_df[listing_date_col] == date_str]
                        if not ak_df.empty:
                            # 只严格检查关键字段
                            if check_new_listing_completeness(ak_df):
                                logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(ak_df)} 条新上市交易信息")
                                # 确保列名标准化
                                if '上市日期' not in ak_df.columns:
                                    ak_df['上市日期'] = date_str
                                # 添加类型标识
                                ak_df['类型'] = '股票'
                                return ak_df  # 返回完整数据，不丢弃任何字段
                            else:
                                logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的新上市交易数据不完整，将尝试备用数据源...")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取新上市交易信息失败: {str(e)}", exc_info=True)
            
            # 尝试获取可转债上市数据
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取可转债上市信息...")
                logger.info(f"尝试AkShare接口: ak.bond_cb_list()")
                
                # 检查AkShare版本是否支持该接口
                if hasattr(ak, 'bond_cb_list'):
                    cb_df = ak.bond_cb_list()
                    logger.info("成功调用ak.bond_cb_list()接口")
                else:
                    logger.warning(f"AkShare版本 {akshare_version} 不支持 bond_cb_list 接口")
                    # 根据版本提供替代方案
                    if akshare_version.startswith('1.'):
                        logger.info("尝试替代接口: ak.bond_cb_em()")
                        if hasattr(ak, 'bond_cb_em'):
                            cb_df = ak.bond_cb_em()
                            logger.info("成功调用ak.bond_cb_em()接口")
                        else:
                            logger.error("AkShare版本过旧，不支持可转债数据接口")
                            continue
                    elif akshare_version.startswith('0.'):
                        logger.info("尝试替代接口: ak.bond_cb_all()")
                        if hasattr(ak, 'bond_cb_all'):
                            cb_df = ak.bond_cb_all()
                            logger.info("成功调用ak.bond_cb_all()接口")
                        else:
                            logger.error("AkShare版本过旧，不支持可转债数据接口")
                            continue
                
                if not cb_df.empty:
                    # 仅记录列名信息
                    logger.info(f"AkShare返回可转债上市数据列数: {len(cb_df.columns)}")
                    logger.info(f"AkShare返回可转债上市数据记录数: {len(cb_df)}")
                    logger.info(f"AkShare返回可转债上市数据列名: {cb_df.columns.tolist()}")
                    
                    # 动态匹配上市日期列
                    listing_date_col = next((col for col in cb_df.columns 
                                           if any(kw in col.lower() for kw in ['上市日期', 'listing_date'])), None)
                    
                    if listing_date_col and listing_date_col in cb_df.columns:
                        # 确保日期列是正确格式
                        if not pd.api.types.is_datetime64_any_dtype(cb_df[listing_date_col]):
                            try:
                                cb_df[listing_date_col] = pd.to_datetime(cb_df[listing_date_col]).dt.strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        # 筛选目标日期数据
                        cb_df = cb_df[cb_df[listing_date_col] == date_str]
                        if not cb_df.empty:
                            # 只严格检查关键字段
                            if '债券代码' in cb_df.columns and '债券简称' in cb_df.columns:
                                logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(cb_df)} 条可转债上市信息")
                                # 重命名列以匹配股票格式
                                cb_df = cb_df.rename(columns={
                                    '债券代码': '股票代码',
                                    '债券简称': '股票简称'
                                })
                                # 添加类型标识
                                cb_df['类型'] = '可转债'
                                return cb_df  # 返回完整数据，不丢弃任何字段
                            else:
                                logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的可转债上市数据不完整")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取可转债上市信息失败: {str(e)}", exc_info=True)
            
            # 尝试Baostock（备用数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从Baostock获取新上市交易信息...")
                logger.info("Baostock接口: bs.query_stock_basic()")
                lg = bs.login()
                if lg.error_code != '0':
                    logger.warning(f"Baostock登录失败: {lg.error_msg}")
                    raise Exception("Baostock登录失败")
                
                # 尝试多种Baostock接口
                try:
                    # 尝试方法1: query_stock_basic
                    rs = bs.query_stock_basic()
                    if rs.error_code == '0':
                        data_list = []
                        while (rs.error_code == '0') & rs.next():
                            data_list.append(rs.get_row_data())
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        if not df.empty:
                            # 仅记录列名信息
                            logger.info(f"Baostock返回新上市交易数据列数: {len(df.columns)}")
                            logger.info(f"Baostock返回新上市交易数据记录数: {len(df)}")
                            logger.info(f"Baostock返回新上市交易数据列名: {df.columns.tolist()}")
                            
                            # 标准化日期格式
                            df['list_date'] = pd.to_datetime(df['list_date']).dt.strftime('%Y-%m-%d')
                            df = df[df['list_date'] == date_str]
                            if not df.empty:
                                # 创建新的DataFrame，但保留所有字段
                                result_df = pd.DataFrame()
                                result_df['股票代码'] = df['code']
                                result_df['股票简称'] = df['code_name']
                                result_df['上市日期'] = df['list_date']
                                
                                # 复制其他所有字段
                                for col in df.columns:
                                    if col not in ['code', 'code_name', 'list_date'] and col not in result_df.columns:
                                        result_df[col] = df[col]
                                
                                # 添加类型标识
                                result_df['类型'] = '股票'
                                
                                # 只严格检查关键字段
                                if check_new_listing_completeness(result_df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(result_df)} 条新上市交易信息")
                                    return result_df  # 返回完整数据，不丢弃任何字段
                                else:
                                    logger.warning(f"{'测试模式' if test else '正常模式'}: Baostock返回的数据缺少关键字段，跳过此数据源")
                except AttributeError:
                    pass
                
                try:
                    # 尝试方法2: query_all_stock
                    logger.info("Baostock备用接口: bs.query_all_stock()")
                    rs = bs.query_all_stock()
                    if rs.error_code == '0':
                        data_list = []
                        while (rs.error_code == '0') & rs.next():
                            data_list.append(rs.get_row_data())
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        if not df.empty:
                            # 仅记录列名信息
                            logger.info(f"Baostock返回新上市交易数据列数: {len(df.columns)}")
                            logger.info(f"Baostock返回新上市交易数据记录数: {len(df)}")
                            logger.info(f"Baostock返回新上市交易数据列名: {df.columns.tolist()}")
                            
                            # 标准化日期格式
                            df['list_date'] = pd.to_datetime(df['list_date']).dt.strftime('%Y-%m-%d')
                            df = df[df['list_date'] == date_str]
                            if not df.empty:
                                # 创建新的DataFrame，但保留所有字段
                                result_df = pd.DataFrame()
                                result_df['股票代码'] = df['code']
                                result_df['股票简称'] = df['code_name']
                                result_df['上市日期'] = df['list_date']
                                
                                # 复制其他所有字段
                                for col in df.columns:
                                    if col not in ['code', 'code_name', 'list_date'] and col not in result_df.columns:
                                        result_df[col] = df[col]
                                
                                # 添加类型标识
                                result_df['类型'] = '股票'
                                
                                # 只严格检查关键字段
                                if check_new_listing_completeness(result_df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(result_df)} 条新上市交易信息")
                                    return result_df  # 返回完整数据，不丢弃任何字段
                                else:
                                    logger.warning(f"{'测试模式' if test else '正常模式'}: Baostock返回的数据缺少关键字段，跳过此数据源")
                except AttributeError:
                    pass
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: Baostock获取新上市交易信息失败: {str(e)}", exc_info=True)
            finally:
                try:
                    bs.logout()
                except:
                    pass
        
        logger.info(f"{'测试模式' if test else '正常模式'}: 未找到新上市交易数据")
        return pd.DataFrame()
        
    except Exception as e:
        error_msg = f"{'测试模式' if test else '正常模式'}: 【数据错误】获取新上市交易信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wecom_message(error_msg)
        return pd.DataFrame()

def get_all_etf_list():
    """从多数据源获取所有ETF列表
    返回:
        DataFrame: ETF列表，包含代码和名称
    """
    # 尝试AkShare主接口（实时ETF列表）
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        logger.info("AkShare接口: ak.fund_etf_spot_em()")
        # 使用 fund_etf_spot_em 获取实时ETF列表
        df = ak.fund_etf_spot_em()
        logger.info(f"AkShare返回ETF列表数据列数: {len(df.columns)}")
        logger.info(f"AkShare返回ETF列表数据记录数: {len(df)}")
        logger.info(f"AkShare返回ETF列表数据列名: {df.columns.tolist()}")
        
        if not df.empty:
            # 动态匹配列名 - 增强容错能力
            code_col = next((col for col in df.columns 
                           if any(kw in col.lower() for kw in ['代码', 'symbol', 'code'])), None)
            name_col = next((col for col in df.columns 
                           if any(kw in col.lower() for kw in ['名称', 'name', '简称'])), None)
            
            if code_col and name_col:
                # 确保代码列是字符串类型
                df[code_col] = df[code_col].astype(str)
                
                # 创建ETF列表DataFrame
                etf_list = pd.DataFrame()
                etf_list['code'] = df[code_col].apply(
                    lambda x: f"sh.{x}" if x.startswith(('5', '119')) else f"sz.{x}"
                )
                etf_list['name'] = df[name_col]
                
                # 过滤掉非ETF代码（如以其他数字开头的）
                etf_list = etf_list[etf_list['code'].str.contains(r'^sh\.\d{6}|^sz\.\d{6}')]
                
                logger.info(f"从AkShare成功获取 {len(etf_list)} 只ETF")
                return etf_list
            else:
                logger.error(f"AkShare返回数据缺少必要列: {df.columns.tolist()}")
                logger.error(f"期望的列: 代码列 - {code_col}, 名称列 - {name_col}")
        else:
            logger.warning("AkShare fund_etf_spot_em 返回空ETF列表")
    except Exception as e:
        logger.error(f"AkShare fund_etf_spot_em 获取ETF列表失败: {str(e)}", exc_info=True)

    # 尝试AkShare备用接口（历史ETF数据）
    try:
        logger.info("尝试从AkShare备用接口获取ETF列表...")
        logger.info("AkShare接口: ak.fund_etf_hist_sina(symbol='etf')")
        df = ak.fund_etf_hist_sina(symbol="etf")
        logger.info(f"AkShare备用接口返回ETF列表数据列数: {len(df.columns)}")
        logger.info(f"AkShare备用接口返回ETF列表数据记录数: {len(df)}")
        logger.info(f"AkShare备用接口返回ETF列表数据列名: {df.columns.tolist()}")
        
        if not df.empty:
            # 动态匹配列名 - 增强容错能力
            code_col = next((col for col in df.columns 
                           if any(kw in col.lower() for kw in ['基金代码', '代码', 'symbol'])), None)
            name_col = next((col for col in df.columns 
                           if any(kw in col.lower() for kw in ['基金简称', '名称', 'name'])), None)
            
            if code_col and name_col:
                # 确保代码列是字符串类型
                df[code_col] = df[code_col].astype(str)
                
                # 创建ETF列表DataFrame
                etf_list = pd.DataFrame()
                etf_list['code'] = df[code_col].apply(
                    lambda x: f"sh.{x}" if x.startswith('5') else f"sz.{x}"
                )
                etf_list['name'] = df[name_col]
                
                logger.info(f"从AkShare备用接口成功获取 {len(etf_list)} 只ETF")
                return etf_list
            else:
                logger.error(f"AkShare备用接口返回数据缺少必要列: {df.columns.tolist()}")
                logger.error(f"期望的列: 代码列 - {code_col}, 名称列 - {name_col}")
        else:
            logger.warning("AkShare备用接口返回空ETF列表")
    except Exception as e:
        logger.error(f"AkShare备用接口获取ETF列表失败: {str(e)}", exc_info=True)

    # 尝试Baostock（使用兼容性更强的接口）
    try:
        logger.info("尝试从Baostock获取ETF列表...")
        logger.info("Baostock接口: bs.query_all_stock()")
        lg = bs.login()
        if lg.error_code != '0':
            logger.warning(f"Baostock登录失败: {lg.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 使用 query_all_stock 获取所有股票，然后筛选ETF
        rs = bs.query_all_stock()
        if rs.error_code == '0':
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            df = pd.DataFrame(data_list, columns=rs.fields)
            if not df.empty:
                logger.info(f"Baostock返回ETF列表数据列数: {len(df.columns)}")
                logger.info(f"Baostock返回ETF列表数据记录数: {len(df)}")
                logger.info(f"Baostock返回ETF列表数据列名: {df.columns.tolist()}")
                
                # 筛选ETF类型证券（通常以51或15开头）
                etf_list = df[df['code'].str.startswith(('51', '58', '15', '16'))]
                if not etf_list.empty:
                    etf_list = etf_list[['code', 'code_name']]
                    etf_list.columns = ['code', 'name']
                    # 添加sh./sz.前缀
                    etf_list['code'] = etf_list['code'].apply(
                        lambda x: f"sh.{x}" if x.startswith(('51', '58')) else f"sz.{x}"
                    )
                    logger.info(f"从Baostock成功获取 {len(etf_list)} 只ETF")
                    return etf_list
                else:
                    logger.warning("Baostock返回的股票列表中没有ETF")
            else:
                logger.warning("Baostock返回空股票列表")
        else:
            logger.error(f"Baostock查询失败: {rs.error_msg}")
    except Exception as e:
        logger.error(f"Baostock获取ETF列表失败: {str(e)}", exc_info=True)
    finally:
        try:
            bs.logout()
        except:
            pass

    # 尝试新浪财经备用接口
    try:
        logger.info("尝试从新浪财经获取ETF列表...")
        logger.info("新浪财经接口: http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList")
        url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = json.loads(response.text)
            if data:
                logger.info(f"新浪财经返回ETF列表数据记录数: {len(data)}")
                # 假设返回的数据有symbol和name字段
                if data and 'symbol' in data[0] and 'name' in data[0]:
                    logger.info("新浪财经返回ETF列表数据包含必要字段")
                else:
                    logger.warning("新浪财经返回ETF列表数据缺少必要字段")
                
                etf_list = pd.DataFrame(data)
                etf_list['code'] = etf_list['symbol'].apply(
                    lambda x: f"sh.{x}" if x.startswith('5') else f"sz.{x}"
                )
                etf_list = etf_list[['code', 'name']]
                logger.info(f"从新浪财经成功获取 {len(etf_list)} 只ETF")
                return etf_list
            else:
                logger.warning("新浪财经返回空ETF列表")
        else:
            logger.warning(f"新浪财经请求失败，状态码: {response.status_code}")
    except Exception as e:
        logger.error(f"新浪财经获取ETF列表失败: {str(e)}", exc_info=True)

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
        DataFrame: ETF数据或None（如果所有数据源都失败）"""
    # 确保etf_code是标准化格式（sh.588940或sz.159915）
    if not etf_code.startswith(('sh.', 'sz.')):
        if etf_code.startswith(('5', '119')):
            etf_code = f"sh.{etf_code}"
        else:
            etf_code = f"sz.{etf_code}"
        logger.info(f"标准化ETF代码格式为: {etf_code}")
    
    # 首先检查缓存
    cached_data = load_from_cache(etf_code, data_type)
    if cached_data is not None and not cached_data.empty:
        logger.info(f"使用缓存数据 {etf_code} ({len(cached_data)}条记录)")
        
        # 获取起始日期（从缓存中获取最后日期）
        last_date = cached_data['date'].max()
        start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"ETF {etf_code} 已有数据到 {last_date.strftime('%Y-%m-%d')}，从 {start_date} 开始获取新数据")
    else:
        logger.info(f"ETF {etf_code} 无缓存数据，将获取全部数据")
        start_date = None
    
    # 尝试AkShare（主数据源）
    try:
        logger.info(f"尝试从AkShare获取{etf_code}数据...")
        # 从代码中提取纯数字代码（移除sh./sz.前缀）
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        # 使用最新确认可用的ETF历史数据接口
        logger.info(f"尝试使用fund_etf_hist_em接口获取{etf_code}数据...")
        logger.info(f"AkShare接口: ak.fund_etf_hist_em(symbol='{pure_code}')")
        df = akshare_retry(ak.fund_etf_hist_em, symbol=pure_code)
        
        # 添加关键日志 - 检查AkShare返回
        if df is not None and not df.empty:
            logger.info(f"AkShare返回ETF数据列数: {len(df.columns)}")
            logger.info(f"AkShare返回ETF数据记录数: {len(df)}")
            logger.info(f"AkShare返回ETF数据列名: {df.columns.tolist()}")
        else:
            logger.warning("AkShare fund_etf_hist_em 返回空数据")
        
        if df is None or df.empty:
            logger.warning(f"AkShare fund_etf_hist_em返回空数据 {etf_code}")
            # 尝试备用接口
            logger.info(f"尝试使用fund_etf_hist_sina接口获取{etf_code}数据...")
            logger.info(f"AkShare接口: ak.fund_etf_hist_sina(symbol='{pure_code}')")
            df = akshare_retry(ak.fund_etf_hist_sina, symbol=pure_code)
            
            # 添加关键日志 - 检查备用接口返回
            if df is not None and not df.empty:
                logger.info(f"AkShare返回ETF数据列数: {len(df.columns)}")
                logger.info(f"AkShare返回ETF数据记录数: {len(df)}")
                logger.info(f"AkShare返回ETF数据列名: {df.columns.tolist()}")
            else:
                logger.warning("AkShare fund_etf_hist_sina 返回空数据")
        
        if df is None or df.empty:
            logger.warning(f"AkShare返回空数据 {etf_code}")
        
        if df is not None and not df.empty:
            # 重命名列为标准格式
            column_mapping = {
                '日期': 'date', 'date': 'date',
                '开盘': 'open', 'open': 'open', '开盘价': 'open',
                '最高': 'high', 'high': 'high', '最高价': 'high',
                '最低': 'low', 'low': 'low', '最低价': 'low',
                '收盘': 'close', 'close': 'close', '收盘价': 'close',
                '成交量': 'volume', 'volume': 'volume', '成交额': 'volume'
            }
            
            # 选择存在的列
            existing_columns = [col for col in column_mapping.keys() if col in df.columns]
            rename_dict = {col: column_mapping[col] for col in existing_columns}
            
            # 重命名列
            df = df.rename(columns=rename_dict)
            
            # 确保必要列存在
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                if col not in df.columns:
                    # 如果缺少必要列，尝试从其他列映射
                    if col == 'volume' and '成交额' in df.columns:
                        df['volume'] = df['成交额']
            
            # 将日期转换为datetime
            if 'date' in df.columns:
                try:
                    df['date'] = pd.to_datetime(df['date'])
                except Exception as e:
                    logger.error(f"日期转换失败: {str(e)}")
                    return None
            
            # 如果指定了起始日期，只获取新数据
            if start_date and 'date' in df.columns:
                try:
                    start_date_dt = pd.to_datetime(start_date)
                    df = df[df['date'] >= start_date_dt]
                except Exception as e:
                    logger.error(f"日期筛选失败: {str(e)}")
            
            # 按日期排序
            if 'date' in df.columns:
                df = df.sort_values('date')
            
            # 关键修复：添加详细日志记录数据完整性检查
            is_complete = check_data_completeness(df)
            logger.info(f"数据完整性检查结果 {etf_code}: {'通过' if is_complete else '未通过'}")
            
            # 检查数据完整性
            if is_complete:
                logger.info(f"从AkShare成功获取 {etf_code} 新数据 ({len(df)}条记录)")
                # 保存到缓存
                save_to_cache(etf_code, df, data_type)
                return df
            else:
                logger.warning(f"AkShare返回的{etf_code}数据不完整，将尝试备用数据源...")
    except Exception as e:
        logger.error(f"AkShare获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 尝试Baostock（备用数据源）
    try:
        logger.info(f"尝试从Baostock获取{etf_code}数据...")
        logger.info(f"Baostock接口: bs.query_history_k_data_plus()")
        lg = bs.login()
        if lg.error_code != '0':
            logger.warning(f"Baostock登录失败: {lg.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 如果有缓存，只获取新数据；否则获取最近100天数据
        if start_date:
            start_date_str = start_date
            end_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        else:
            start_date_str = (datetime.datetime.now() - datetime.timedelta(days=100)).strftime('%Y-%m-%d')
            end_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 获取日线数据
        rs = bs.query_history_k_data_plus(
            etf_code.replace('sh.', '').replace('sz.', ''),
            "date,open,high,low,close,volume",
            start_date=start_date_str,
            end_date=end_date_str,
            frequency="d", 
            adjustflag="3"
        )
        if rs.error_code != '0':
            logger.error(f"Baostock查询失败: {rs.error_msg}")
            bs.logout()
            raise Exception("Baostock查询失败")
        
        # 转换为DataFrame
        df = rs.get_data()
        bs.logout()  # 使用后登出
        
        if not df.empty:
            logger.info(f"Baostock返回ETF数据列数: {len(df.columns)}")
            logger.info(f"Baostock返回ETF数据记录数: {len(df)}")
            logger.info(f"Baostock返回ETF数据列名: {df.columns.tolist()}")
            
            # 转换数据类型
            try:
                df['date'] = pd.to_datetime(df['date'])
                df['open'] = df['open'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                df['volume'] = df['volume'].astype(float)
            except Exception as e:
                logger.error(f"数据类型转换失败: {str(e)}")
                return None
            
            # 按日期排序
            df = df.sort_values('date')
            
            # 关键修复：添加详细日志记录数据完整性检查
            is_complete = check_data_completeness(df)
            logger.info(f"Baostock数据完整性检查结果 {etf_code}: {'通过' if is_complete else '未通过'}")
            
            # 检查数据完整性
            if is_complete:
                logger.info(f"从Baostock成功获取 {etf_code} 新数据 ({len(df)}条记录)")
                # 保存到缓存
                save_to_cache(etf_code, df, data_type)
                return df
            else:
                logger.warning(f"Baostock返回的{etf_code}数据不完整")
    except Exception as e:
        logger.error(f"Baostock获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    error_msg = f"【数据错误】无法获取{etf_code}数据"
    logger.error(error_msg)
    send_wecom_message(error_msg)
    return None

def get_market_sentiment():
    """获取市场情绪指标（简化版）"""
    try:
        # 从多个数据源获取市场情绪指标
        # 这里简化处理，实际应从多个指标计算
        return 0.5  # 返回一个0-1之间的值，0表示极度悲观，1表示极度乐观
    except Exception as e:
        logger.error(f"获取市场情绪数据失败: {str(e)}")
        return 0.5  # 默认值

def get_etf_iopv_data(etf_code):
    """获取ETF的IOPV数据（简化版）"""
    try:
        # 从多个数据源获取ETF的IOPV数据
        # 这里简化处理，实际应从交易所数据获取
        # 模拟生成IOPV数据
        etf_data = get_etf_data(etf_code, 'daily')
        if etf_data is None or etf_data.empty:
            return None
        
        # 简单模拟IOPV，比收盘价略低
        etf_data['iopv'] = etf_data['close'] * 0.995
        return etf_data[['date', 'iopv']]
    except Exception as e:
        logger.error(f"获取{etf_code} IOPV数据失败: {str(e)}")
        return None

def read_new_stock_pushed_flag(date):
    """读取新股信息是否已推送标志
    参数:
        date: 日期对象
    返回:
        tuple: (flag_path, is_pushed)"""
    flag_path = os.path.join(Config.NEW_STOCK_DIR, f'new_stock_pushed_{date.strftime("%Y%m%d")}.flag')
    is_pushed = os.path.exists(flag_path)
    return flag_path, is_pushed

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    flag_path, _ = read_new_stock_pushed_flag(get_beijing_time().date())
    with open(flag_path, 'w') as f:
        f.write(get_beijing_time().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info(f"标记新股信息已推送: {flag_path}")

def read_listing_pushed_flag(date):
    """读取新上市交易信息是否已推送标志
    参数:
        date: 日期对象
    返回:
        tuple: (flag_path, is_pushed)"""
    flag_path = os.path.join(Config.NEW_STOCK_DIR, f'listing_pushed_{date.strftime("%Y%m%d")}.flag')
    is_pushed = os.path.exists(flag_path)
    return flag_path, is_pushed

def mark_listing_info_pushed():
    """标记新上市交易信息已推送"""
    flag_path, _ = read_listing_pushed_flag(get_beijing_time().date())
    with open(flag_path, 'w') as f:
        f.write(get_beijing_time().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info(f"标记新上市交易信息已推送: {flag_path}")

def get_cache_path(etf_code, data_type='daily'):
    """获取缓存文件路径"""
    # 确保etf_code是标准化格式
    if not etf_code.startswith(('sh.', 'sz.')):
        if etf_code.startswith(('5', '119')):
            etf_code = f"sh.{etf_code}"
        else:
            etf_code = f"sz.{etf_code}"
    
    cache_dir = os.path.join(Config.RAW_DATA_DIR, 'cache', data_type)
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f'{etf_code}.csv')

def load_from_cache(etf_code, data_type='daily', days=30):
    """从缓存加载ETF数据
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
        days: 返回最近days天的数据
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    # 确保etf_code是标准化格式
    if not etf_code.startswith(('sh.', 'sz.')):
        if etf_code.startswith(('5', '119')):
            etf_code = f"sh.{etf_code}"
        else:
            etf_code = f"sz.{etf_code}"
    
    cache_path = get_cache_path(etf_code, data_type)
    
    # 关键修复：检查缓存文件是否存在
    if not os.path.exists(cache_path):
        logger.info(f"缓存文件不存在: {cache_path}")
        return None
        
    try:
        df = pd.read_csv(cache_path)
        logger.info(f"成功加载缓存文件: {cache_path}")
        
        # 确保日期列存在
        if 'date' not in df.columns:
            logger.error(f"缓存文件缺少'date'列: {cache_path}")
            return None
            
        df['date'] = pd.to_datetime(df['date'])
        
        # 筛选近期数据
        if data_type == 'daily':
            df = df[df['date'] >= (datetime.datetime.now() - datetime.timedelta(days=days))]
            
        return df
    except Exception as e:
        logger.error(f"缓存加载错误 {etf_code}: {str(e)}", exc_info=True)
        return None

def save_to_cache(etf_code, data, data_type='daily'):
    """将ETF数据保存到缓存（增量保存）
    参数:
        etf_code: ETF代码
         DataFrame数据
        data_type: 'daily'或'intraday'
    """
    # 确保etf_code是标准化格式
    if not etf_code.startswith(('sh.', 'sz.')):
        if etf_code.startswith(('5', '119')):
            etf_code = f"sh.{etf_code}"
        else:
            etf_code = f"sz.{etf_code}"
    
    if data is None or data.empty:
        return False
    
    cache_path = get_cache_path(etf_code, data_type)
    temp_path = cache_path + '.tmp'
    
    try:
        # 1. 先写入临时文件
        data.to_csv(temp_path, index=False)
        
        # 2. 如果存在原文件，合并数据
        if os.path.exists(cache_path):
            existing_data = pd.read_csv(cache_path)
            combined = pd.concat([existing_data, data])
            
            # 去重并按日期排序
            combined = combined.drop_duplicates(subset=['date'], keep='last')
            combined = combined.sort_values('date')
            combined.to_csv(temp_path, index=False)
        
        # 3. 原子操作：先删除原文件，再重命名
        if os.path.exists(cache_path):
            os.remove(cache_path)
        os.rename(temp_path, cache_path)
        
        logger.info(f"成功保存 {etf_code} 数据到 {cache_path}")
        return True
    except Exception as e:
        logger.error(f"保存 {etf_code} 数据失败: {str(e)}", exc_info=True)
        # 清理临时文件
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return False

def update_crawl_status(etf_code, status, message=None):
    """更新爬取状态"""
    status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
    try:
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                status_data = json.load(f)
        else:
            status_data = {}
        
        timestamp = get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')
        status_data[etf_code] = {
            'status': status,
            'timestamp': timestamp,
            'message': message
        }
        
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=2)
    except Exception as e:
        logger.error(f"更新爬取状态失败: {str(e)}", exc_info=True)

def get_crawl_status():
    """获取爬取状态"""
    status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def crawl_etf_data(data_type='daily'):
    """爬取ETF数据（增量爬取）
    参数:
        data_type: 数据类型 ('daily' 或 'intraday')
    返回:
        dict: 爬取结果统计
    """
    # 获取ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        error_msg = "【数据错误】ETF列表获取失败，无法爬取数据"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return {"status": "error", "message": "ETF list retrieval failed"}
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for _, etf in etf_list.iterrows():
        # 确保ETF代码是标准化格式
        etf_code = standardize_code(etf['code'])
        try:
            # 获取起始日期（从缓存中获取最后日期）
            cached_data = load_from_cache(etf_code, 'daily')
            start_date = None
            if cached_data is not None and not cached_data.empty:
                last_date = cached_data['date'].max()
                # 检查是否已获取到最新数据
                if (datetime.datetime.now().date() - last_date.date()).days <= 1:
                    logger.info(f"ETF {etf_code} 已有最新数据到 {last_date.strftime('%Y-%m-%d')}，跳过爬取")
                    skipped_count += 1
                    continue
                
                start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"ETF {etf_code} 已有数据到 {last_date.strftime('%Y-%m-%d')}，从 {start_date} 开始获取新数据")
            else:
                logger.info(f"ETF {etf_code} 无缓存数据，将获取全部数据")
            
            # 尝试主数据源(AkShare)
            data = get_etf_data(etf_code, 'daily')
            
            # 如果获取到完整数据
            if data is not None and not data.empty and check_data_completeness(data):
                logger.info(f"成功从AkShare爬取{etf_code}日线数据")
                success_count += 1
                continue
            
            # 所有数据源均失败
            logger.warning(f"无法获取{etf_code}的完整数据")
            failed_count += 1
            
        except Exception as e:
            error_msg = f"【系统错误】爬取{etf_code}日线数据异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            send_wecom_message(error_msg)
            failed_count += 1
    
    # 检查是否所有ETF都爬取成功
    if success_count == len(etf_list):
        # 所有ETF爬取成功，清理状态文件
        status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
        if os.path.exists(status_file):
            try:
                os.remove(status_file)
                logger.info("所有ETF爬取成功，已清理状态文件")
            except Exception as e:
                logger.warning(f"清理状态文件失败: {str(e)}")
    
    return {
        "status": "success" if failed_count == 0 else "partial_success",
        "success_count": success_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count
    }

def cron_crawl_daily():
    """爬取日线数据（增量爬取）"""
    logger.info("日线数据爬取任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过日线数据爬取")
        return {"status": "skipped", "message": "Not trading day"}
    
    # 爬取日线数据
    result = crawl_etf_data(data_type='daily')
    
    return result

def cron_cleanup():
    """清理旧数据"""
    logger.info("数据清理任务触发")
    
    # 清理原始数据
    cleanup_directory(Config.RAW_DATA_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 清理股票池数据
    cleanup_directory(Config.STOCK_POOL_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 交易流水永久保存，不清理
    logger.info("交易流水目录不清理，永久保存")
    
    # 清理套利数据
    cleanup_directory(Config.ARBITRAGE_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 清理错误日志
    cleanup_directory(Config.ERROR_LOG_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    # 清理新股数据
    cleanup_directory(Config.NEW_STOCK_DIR, Config.OTHER_DATA_RETENTION_DAYS)
    
    logger.info("数据清理完成")
    return {"status": "success", "message": "Data cleanup completed"}

def cleanup_directory(directory, days_to_keep=None):
    """清理指定目录中的旧文件
    参数:
        directory: 目录路径
        days_to_keep: 保留天数，None表示不清理"""
    if days_to_keep is None:
        logger.info(f"跳过目录清理: {directory} (永久保存)")
        return
    
    now = datetime.datetime.now()
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        # 获取文件修改时间
        file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
        # 计算文件年龄
        file_age = (now - file_time).days
        
        # 如果文件超过保留天数，删除
        if file_age > days_to_keep:
            try:
                os.remove(filepath)
                logger.info(f"已清理旧数据文件: {filename}")
            except Exception as e:
                logger.error(f"清理文件 {filename} 时出错: {str(e)}")

def git_push():
    """推送更改到远程仓库，失败时立即终止"""
    try:
        # 添加所有更改
        subprocess.run(['git', 'add', '.'], check=True)
        
        # 提交更改
        commit_msg = f"自动数据更新 {get_beijing_time().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(['git', 'commit', '-m', commit_msg], check=True)
        
        # 推送到远程仓库
        subprocess.run(['git', 'push', 'origin', 'main'], check=True)
        
        logger.info("数据更改已成功推送到远程仓库")
        return True
    except subprocess.CalledProcessError as e:
        error_msg = f"【系统错误】Git操作失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return False
    except Exception as e:
        error_msg = f"【系统错误】Git操作异常: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return False

def send_wecom_message(message):
    """发送消息到企业微信"""
    # 检查配置
    if not Config.WECOM_WEBHOOK:
        logger.error("WECOM_WEBHOOK 未设置，无法发送企业微信消息")
        return False
    
    # 在消息结尾添加全局备注
    if hasattr(Config, 'MESSAGE_FOOTER') and Config.MESSAGE_FOOTER:
        message = f"{message}\n{Config.MESSAGE_FOOTER}"
    
    try:
        # 构建消息
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        # 发送请求
        response = requests.post(Config.WECOM_WEBHOOK, json=payload, timeout=10)
        # 检查响应
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                logger.info("企业微信消息发送成功")
                return True
        logger.error(f"企业微信消息发送失败: {response.text}")
        return False
    except Exception as e:
        logger.error(f"发送企业微信消息时出错: {str(e)}")
        return False

def akshare_retry(func, *args, **kwargs):
    """AkShare请求重试机制（修复headers参数问题）"""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # 从kwargs中移除headers参数（如果存在）
            # 因为某些AkShare函数不接受headers参数
            original_kwargs = kwargs.copy()
            if 'headers' in original_kwargs:
                del original_kwargs['headers']
            
            return func(*args, **original_kwargs)
        except Exception as e:
            if attempt < max_attempts - 1:
                wait_time = 2 ** attempt  # 指数退避
                logger.warning(f"AkShare请求失败，{wait_time}秒后重试 ({attempt+1}/{max_attempts}): {str(e)}")
                time.sleep(wait_time)
            else:
                raise

def check_data_integrity():
    """检查全局数据完整性
    返回:
        str: 错误信息，None表示数据完整"""
    # 检查ETF数据
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        error_msg = "【数据错误】ETF列表获取失败"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return error_msg
    
    # 检查每只ETF的最新数据
    today = datetime.datetime.now().date()
    for _, etf in etf_list.iterrows():
        # 确保ETF代码是标准化格式
        etf_code = standardize_code(etf['code'])
        data = load_from_cache(etf_code, 'daily')
        if data is None or data.empty:
            error_msg = f"【数据错误】{etf_code}日线数据缺失"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return error_msg
        
        # 检查数据是否足够新
        last_date = data['date'].max().date()
        if (today - last_date).days > Config.MAX_DATA_AGE:
            error_msg = f"【数据错误】{etf_code}日线数据过期（最新日期: {last_date}）"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return error_msg
        
        # 检查数据量是否足够
        if len(data) < Config.MIN_DATA_DAYS:
            error_msg = f"【数据错误】{etf_code}日线数据不足（仅{len(data)}天）"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return error_msg
    
    return None
