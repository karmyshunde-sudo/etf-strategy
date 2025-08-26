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
import subprocess
from config import Config
from logger import get_logger
from retrying import retry

# 从data_source导入所有数据源爬取函数
from data_source import (
    crawl_akshare_primary,
    crawl_akshare_backup,
    crawl_baostock,
    crawl_sina_finance,
    get_all_etf_list,
    check_data_completeness,
    enhance_data_integrity
)

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
    if beijing_time.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return False
    
    # TODO: 这里可以添加中国股市休市日历检查
    # 例如：检查是否为法定节假日
    
    return True

def get_cache_path(etf_code, data_type='daily'):
    """
    生成指定ETF和数据类型的缓存文件路径
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        str: 缓存文件路径
    """
    # 确保etf_code是标准化格式
    if not etf_code.startswith(('sh.', 'sz.')):
        if etf_code.startswith(('5', '119')):
            etf_code = f"sh.{etf_code}"
        else:
            etf_code = f"sz.{etf_code}"
    
    # 严格遵循config.py配置
    cache_dir = Config.RAW_DATA_DIR
    os.makedirs(cache_dir, exist_ok=True)
    
    # 添加关键日志，确认实际保存路径
    cache_path = os.path.join(cache_dir, f'{etf_code}_{data_type}.csv')
    logger.info(f"【路径确认】将保存文件到: {cache_path}")
    
    return cache_path

def load_from_cache(etf_code, data_type='daily', days=365):
    """从缓存加载ETF数据（增强容错性）
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
        # 尝试多种编码方式读取CSV
        encodings = ['utf-8', 'gbk', 'gb2312', 'latin1']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(cache_path, encoding=encoding)
                logger.info(f"成功使用 {encoding} 编码加载缓存文件: {cache_path}")
                break
            except:
                continue
        
        if df is None:
            logger.error(f"无法用常见编码加载缓存文件: {cache_path}")
            return None
        
        logger.info(f"成功加载缓存文件: {cache_path}，共 {len(df)} 条记录")
        
        # 确保日期列存在
        if '日期' not in df.columns:
            logger.error(f"缓存文件缺少'日期'列: {cache_path}")
            return None
            
        # 尝试多种日期格式转换
        try:
            df['日期'] = pd.to_datetime(df['日期'])
        except:
            try:
                df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d')
            except:
                try:
                    df['日期'] = pd.to_datetime(df['日期'], format='%Y/%m/%d')
                except:
                    logger.error(f"无法转换日期格式: {cache_path}")
                    return None
        
        # 筛选近期数据
        if data_type == 'daily':
            df = df[df['日期'] >= (datetime.datetime.now() - datetime.timedelta(days=days))]
        return df
    except Exception as e:
        logger.error(f"缓存加载错误 {etf_code}: {str(e)}", exc_info=True)
        return None

def save_to_cache(etf_code, data, data_type='daily'):
    """将ETF数据保存到缓存（增量保存，增强容错性）
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
        data.to_csv(temp_path, index=False, encoding='utf-8')
        
        # 2. 如果存在原文件，合并数据
        if os.path.exists(cache_path):
            existing_data = load_from_cache(etf_code, data_type, days=365)
            if existing_data is not None and not existing_data.empty:
                combined = pd.concat([existing_data, data])
                
                # 去重并按日期排序
                combined = combined.drop_duplicates(subset=['日期'], keep='last')
                combined = combined.sort_values('日期')
                
                # 仅保留统一列名
                combined = combined[[col for col in list(UNIFIED_COLUMNS.values()) if col in combined.columns]]
                
                combined.to_csv(temp_path, index=False, encoding='utf-8')
            else:
                logger.warning(f"无法加载原缓存文件，将覆盖原文件: {cache_path}")
                data.to_csv(temp_path, index=False, encoding='utf-8')
        else:
            logger.info(f"缓存文件不存在，将创建新文件: {cache_path}")
            data.to_csv(temp_path, index=False, encoding='utf-8')
        
        # 3. 原子操作：先删除原文件，再重命名
        if os.path.exists(cache_path):
            os.remove(cache_path)
        os.rename(temp_path, cache_path)
        
        # 4. 验证文件确实存在且可读
        if not os.path.exists(cache_path):
            raise FileNotFoundError(f"文件保存后未找到: {cache_path}")
        
        # 5. 验证文件内容
        try:
            test_df = pd.read_csv(cache_path)
            if len(test_df) < len(data):
                logger.warning(f"保存的文件 {cache_path} 数据量少于预期 ({len(test_df)}/{len(data)})")
        except Exception as e:
            logger.error(f"验证保存的文件失败: {str(e)}")
            raise
        
        logger.info(f"成功保存 {etf_code} 数据到 {cache_path} ({len(data)}条记录)")
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

def get_etf_data(etf_code, data_type='daily', start_date=None):
    """从多数据源获取ETF数据（增量获取，增强容错性）
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
        start_date: 开始日期（可选）
    返回:
        DataFrame: ETF数据或None（如果所有数据源都失败）"""
    # 确保etf_code是标准化格式
    if not etf_code.startswith(('sh.', 'sz.')):
        if etf_code.startswith(('5', '119')):
            etf_code = f"sh.{etf_code}"
        else:
            etf_code = f"sz.{etf_code}"
    
    # 尝试主数据源(AkShare主接口)
    try:
        df = crawl_akshare_primary(etf_code, start_date)
        if df is not None and not df.empty and check_data_completeness(df):
            return df
    except Exception as e:
        logger.error(f"AkShare主接口获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 尝试备用数据源1(AkShare备用接口)
    try:
        df = crawl_akshare_backup(etf_code, start_date)
        if df is not None and not df.empty and check_data_completeness(df):
            return df
    except Exception as e:
        logger.error(f"AkShare备用接口获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 尝试备用数据源2(Baostock)
    try:
        df = crawl_baostock(etf_code, start_date)
        if df is not None and not df.empty and check_data_completeness(df):
            return df
    except Exception as e:
        logger.error(f"Baostock获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 尝试备用数据源3(新浪财经)
    try:
        df = crawl_sina_finance(etf_code, start_date)
        if df is not None and not df.empty and check_data_completeness(df):
            return df
    except Exception as e:
        logger.error(f"新浪财经获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 所有数据源均失败
    logger.error(f"【数据错误】无法获取{etf_code}数据")
    return None

def get_new_stock_subscriptions(test=False):
    """获取新股申购信息（增强容错性）
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
            logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {date_str} 的新上市交易数据")
            
            # 尝试AkShare（主数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取新上市交易信息...")
                logger.info(f"AkShare接口: ak.stock_xgsglb_em()")
                df = ak.stock_xgsglb_em()
                if not df.empty:
                    # 仅记录列名信息
                    logger.info(f"AkShare返回新上市交易数据列数: {len(df.columns)}")
                    logger.info(f"AkShare返回新上市交易数据记录数: {len(df)}")
                    logger.info(f"AkShare返回新上市交易数据列名: {df.columns.tolist()}")
                    
                    # 动态匹配上市日期列
                    listing_date_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['上市日期', 'listing_date'])), None)
                    
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
                            # 尝试修复列名
                            code_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['代码', 'code'])), None)
                            name_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['名称', 'name', '简称'])), None)
                            price_col = next((col for col in df.columns 
                                            if any(kw in col.lower() for kw in ['价格', 'price'])), None)
                            
                            if code_col and name_col:
                                valid_df = df[[code_col, name_col]].copy()
                                valid_df.rename(columns={code_col: '股票代码', name_col: '股票简称'}, inplace=True)
                                
                                if price_col:
                                    valid_df['发行价格'] = df[price_col]
                                
                                valid_df['上市日期'] = date_str
                                valid_df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_listing_completeness(valid_df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(valid_df)} 条新上市交易信息")
                                    return valid_df[['股票代码', '股票简称', '发行价格', '上市日期', '类型']]
                                else:
                                    logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的新上市交易数据不完整，将尝试备用数据源...")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取新上市交易信息失败: {str(e)}", exc_info=True)
            
            # 尝试Baostock（备用数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从Baostock获取新上市交易信息...")
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
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        if not df.empty:
                            # 标准化日期格式
                            df['list_date'] = pd.to_datetime(df['list_date']).dt.strftime('%Y-%m-%d')
                            df = df[df['list_date'] == date_str]
                            if not df.empty:
                                # 重命名列以匹配股票格式
                                df = df.rename(columns={
                                    'code': '股票代码',
                                    'code_name': '股票简称',
                                    'issue_price': '发行价格',
                                    'list_date': '上市日期'
                                })
                                df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_listing_completeness(df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新上市交易信息")
                                    return df[['股票代码', '股票简称', '发行价格', '上市日期', '类型']]
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
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        if not df.empty:
                            # 标准化日期格式
                            df['list_date'] = pd.to_datetime(df['list_date']).dt.strftime('%Y-%m-%d')
                            df = df[df['list_date'] == date_str]
                            if not df.empty:
                                # 重命名列以匹配股票格式
                                df = df.rename(columns={
                                    'code': '股票代码',
                                    'code_name': '股票简称',
                                    'issue_price': '发行价格',
                                    'list_date': '上市日期'
                                })
                                df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_listing_completeness(df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新上市交易信息")
                                    return df[['股票代码', '股票简称', '发行价格', '上市日期', '类型']]
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

def get_new_stock_listings(test=False):
    """获取新上市交易的新股信息（增强容错性）
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
                logger.info(f"AkShare接口: ak.stock_xgsglb_em()")
                df = ak.stock_xgsglb_em()
                if not df.empty:
                    # 仅记录列名信息
                    logger.info(f"AkShare返回新上市交易数据列数: {len(df.columns)}")
                    logger.info(f"AkShare返回新上市交易数据记录数: {len(df)}")
                    logger.info(f"AkShare返回新上市交易数据列名: {df.columns.tolist()}")
                    
                    # 动态匹配上市日期列
                    listing_date_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['上市日期', 'listing_date'])), None)
                    
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
                            # 尝试修复列名
                            code_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['代码', 'code'])), None)
                            name_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['名称', 'name', '简称'])), None)
                            price_col = next((col for col in df.columns 
                                            if any(kw in col.lower() for kw in ['价格', 'price'])), None)
                            
                            if code_col and name_col:
                                valid_df = df[[code_col, name_col]].copy()
                                valid_df.rename(columns={code_col: '股票代码', name_col: '股票简称'}, inplace=True)
                                
                                if price_col:
                                    valid_df['发行价格'] = df[price_col]
                                
                                valid_df['上市日期'] = date_str
                                valid_df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_listing_completeness(valid_df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(valid_df)} 条新上市交易信息")
                                    return valid_df[['股票代码', '股票简称', '发行价格', '上市日期', '类型']]
                                else:
                                    logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的新上市交易数据不完整，将尝试备用数据源...")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取新上市交易信息失败: {str(e)}", exc_info=True)
            
            # 尝试Baostock（备用数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从Baostock获取新上市交易信息...")
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
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        if not df.empty:
                            # 标准化日期格式
                            df['list_date'] = pd.to_datetime(df['list_date']).dt.strftime('%Y-%m-%d')
                            df = df[df['list_date'] == date_str]
                            if not df.empty:
                                # 重命名列以匹配股票格式
                                df = df.rename(columns={
                                    'code': '股票代码',
                                    'code_name': '股票简称',
                                    'issue_price': '发行价格',
                                    'list_date': '上市日期'
                                })
                                df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_listing_completeness(df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新上市交易信息")
                                    return df[['股票代码', '股票简称', '发行价格', '上市日期', '类型']]
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
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        if not df.empty:
                            # 标准化日期格式
                            df['list_date'] = pd.to_datetime(df['list_date']).dt.strftime('%Y-%m-%d')
                            df = df[df['list_date'] == date_str]
                            if not df.empty:
                                # 重命名列以匹配股票格式
                                df = df.rename(columns={
                                    'code': '股票代码',
                                    'code_name': '股票简称',
                                    'issue_price': '发行价格',
                                    'list_date': '上市日期'
                                })
                                df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_listing_completeness(df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新上市交易信息")
                                    return df[['股票代码', '股票简称', '发行价格', '上市日期', '类型']]
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

def check_data_integrity():
    """检查全局数据完整性（增强容错性）
    返回:
        str: 错误信息，None表示数据完整
    """
    # 检查ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        error_msg = "【数据错误】ETF列表获取失败"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return error_msg
    
    # 检查每只ETF的最新数据
    today = get_beijing_time().date()
    for _, etf in etf_list.iterrows():
        # 确保ETF代码是标准化格式
        etf_code = etf['code']
        if not etf_code.startswith(('sh.', 'sz.')):
            if etf_code.startswith(('5', '119')):
                etf_code = f"sh.{etf_code}"
            else:
                etf_code = f"sz.{etf_code}"
        
        # 尝试获取最新日线数据
        df = get_etf_data(etf_code, 'daily')
        if df is None or df.empty:
            # 关键修复：即使数据不完整，也不立即返回错误
            logger.warning(f"ETF {etf_code} 数据为空，但继续检查其他ETF")
            continue
            
        # 检查最新数据是否包含今天的数据
        last_date = df['日期'].max()
        if (today - last_date.date()).days > 1:
            logger.warning(f"ETF {etf_code} 数据未更新到最新，最新日期: {last_date.date()}")
            # 不返回错误，继续检查其他ETF
    
    # 检查新股数据
    try:
        new_stock = get_new_stock_subscriptions()
        if new_stock is None or new_stock.empty:
            logger.warning("新股申购数据为空")
        else:
            # 检查是否有今天的申购数据
            today = get_beijing_time().strftime('%Y-%m-%d')
            today_data = new_stock[new_stock['申购日期'] == today]
            if today_data.empty:
                logger.info(f"今天({today})没有新股申购数据")
    except Exception as e:
        logger.warning(f"检查新股数据时出错: {str(e)}")
    
    # 检查新上市数据
    try:
        new_listings = get_new_stock_listings()
        if new_listings is None or new_listings.empty:
            logger.warning("新上市股票数据为空")
        else:
            # 检查是否有今天的上市数据
            today = get_beijing_time().strftime('%Y-%m-%d')
            today_data = new_listings[new_listings['上市日期'] == today]
            if today_data.empty:
                logger.info(f"今天({today})没有新上市股票数据")
    except Exception as e:
        logger.warning(f"检查新上市数据时出错: {str(e)}")
    
    return None

def cleanup_old_data(days=365):
    """清理旧数据
    参数:
        days: 保留最近days天的数据
    """
    logger.info(f"开始清理超过 {days} 天的旧数据...")
    
    # 清理原始数据
    raw_dir = Config.RAW_DATA_DIR
    if os.path.exists(raw_dir):
        for file in os.listdir(raw_dir):
            if file.endswith('_daily.csv'):
                file_path = os.path.join(raw_dir, file)
                try:
                    # 获取文件最后修改时间
                    file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                    if (datetime.datetime.now() - file_time).days > days:
                        os.remove(file_path)
                        logger.info(f"已删除旧数据文件: {file_path}")
                except Exception as e:
                    logger.error(f"删除文件 {file_path} 失败: {str(e)}")
    
    # 清理股票池数据
    stock_pool_dir = Config.STOCK_POOL_DIR
    if os.path.exists(stock_pool_dir):
        for file in os.listdir(stock_pool_dir):
            if file.startswith('stock_pool_') and file.endswith('.csv'):
                file_path = os.path.join(stock_pool_dir, file)
                try:
                    # 从文件名中提取日期
                    date_str = file.split('_')[2].split('.')[0]
                    file_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
                    if (datetime.datetime.now().date() - file_date).days > days:
                        os.remove(file_path)
                        logger.info(f"已删除旧股票池文件: {file_path}")
                except Exception as e:
                    logger.error(f"处理股票池文件 {file_path} 失败: {str(e)}")
    
    logger.info("旧数据清理完成")

def update_stock_pool():
    """更新股票池文件，确保有稳定的ETF列表"""
    try:
        # 获取ETF列表
        etf_list = get_all_etf_list()
        if etf_list is None or etf_list.empty:
            logger.error("【数据错误】无法获取ETF列表，无法更新股票池")
            return False
        
        # 保存到股票池目录
        stock_pool_path = os.path.join(Config.STOCK_POOL_DIR, 'stock_pool.csv')
        etf_list.to_csv(stock_pool_path, index=False)
        logger.info(f"股票池已更新: {stock_pool_path} ({len(etf_list)}只ETF)")
        
        # 同时保存一份到RAW_DATA_DIR用于备份
        backup_path = os.path.join(Config.RAW_DATA_DIR, 'stock_pool.csv')
        etf_list.to_csv(backup_path, index=False)
        
        return True
    except Exception as e:
        logger.error(f"更新股票池失败: {str(e)}", exc_info=True)
        return False

def generate_stock_pool():
    """生成股票池（5只稳健仓 + 5只激进仓）"""
    # 检查数据完整性
    integrity_check = check_data_integrity()
    if integrity_check:
        error_msg = f"【数据错误】{integrity_check}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None
    
    try:
        # 获取ETF列表
        etf_list = get_all_etf_list()
        if etf_list is None or etf_list.empty:
            error_msg = "【数据错误】股票池生成失败：ETF列表为空"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 评分所有ETF
        scored_etfs = []
        for _, etf in etf_list.iterrows():
            try:
                # 确保ETF代码是标准化格式
                etf_code = etf['code']
                score = calculate_ETF_score(etf_code)
                if score:
                    scored_etfs.append(score)
            except Exception as e:
                error_msg = f"【系统错误】计算{etf.get('code', '未知')}评分失败: {str(e)}"
                logger.error(error_msg)
                send_wecom_message(error_msg)
                continue
        
        if not scored_etfs:
            error_msg = "【数据错误】股票池生成失败：无有效评分数据"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 转换为DataFrame
        scores_df = pd.DataFrame(scored_etfs)
        
        # 按综合评分排序
        scores_df = scores_df.sort_values('total_score', ascending=False)
        
        # 选择稳健仓（高评分、低波动）
        # 稳健仓选择标准：综合评分高，风险评分高
        # 先按总评分排序，再按风险评分排序
        selected_stable = scores_df.nlargest(10, 'total_score')
        selected_stable = selected_stable.nlargest(5, 'risk_score')
        
        # 选择激进仓（高收益潜力）
        # 激进仓选择标准：收益评分高，溢价率低
        selected_aggressive = scores_df.nlargest(10, 'return_score')
        selected_aggressive = selected_aggressive.nlargest(5, 'premium_score')
        
        # 检查是否选择到足够的ETF
        if len(selected_stable) < 5:
            error_msg = f"【数据错误】稳健仓ETF数量不足（仅{len(selected_stable)}只），无法生成完整股票池"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
            
        if len(selected_aggressive) < 5:
            error_msg = f"【数据错误】激进仓ETF数量不足（仅{len(selected_aggressive)}只），无法生成完整股票池"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 合并股票池
        final_pool = pd.concat([selected_stable, selected_aggressive])
        
        # 为每只ETF添加类型标识
        final_pool['type'] = ''
        final_pool.loc[final_pool.index.isin(selected_stable.index), 'type'] = '稳健仓'
        final_pool.loc[final_pool.index.isin(selected_aggressive.index), 'type'] = '激进仓'
        
        # 保存到数据目录
        os.makedirs(Config.STOCK_POOL_DIR, exist_ok=True)
        filename = f"stock_pool_{get_beijing_time().strftime('%Y%m%d')}.csv"
        filepath = os.path.join(Config.STOCK_POOL_DIR, filename)
        final_pool.to_csv(filepath, index=False)
        logger.info(f"股票池生成成功。保存为: {filename}")
        logger.info(f"选定{len(selected_stable)}只稳健ETF和{len(selected_aggressive)}只激进ETF")
        
        # 生成消息
        message = "【ETF股票池】\n"
        message += "【稳健仓】\n"
        for _, etf in selected_stable.iterrows():
            message += f"• {etf['etf_code']} - {etf.get('name', etf.get('etf_name', '未知'))} (评分: {etf['total_score']:.2f})\n"
            message += f"  流动性: {etf['liquidity_score']:.1f} | 风险: {etf['risk_score']:.1f} | 收益: {etf['return_score']:.1f}\n"
        
        message += "\n【激进仓】\n"
        for _, etf in selected_aggressive.iterrows():
            message += f"• {etf['etf_code']} - {etf.get('name', etf.get('etf_name', '未知'))} (评分: {etf['total_score']:.2f})\n"
            message += f"  流动性: {etf['liquidity_score']:.1f} | 风险: {etf['risk_score']:.1f} | 收益: {etf['return_score']:.1f}\n"
        
        return message
    except Exception as e:
        error_msg = f"【系统错误】股票池生成失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None

def calculate_ETF_score(etf_code):
    """计算ETF评分（基于实际市场数据）"""
    try:
        # 获取ETF日线数据
        df = get_etf_data(etf_code, 'daily')
        if df is None or df.empty:
            logger.warning(f"无法获取{etf_code}数据，跳过评分")
            return None
        
        # 获取最新价格
        latest = df.iloc[-1]
        current_price = latest['收盘']
        
        # 计算流动性评分（基于成交量）
        avg_volume = df['成交量'].mean()
        liquidity_score = min(10, max(1, np.log10(avg_volume) * 2))
        
        # 计算风险评分（基于波动率）
        daily_returns = df['收盘'].pct_change().dropna()
        volatility = daily_returns.std() * np.sqrt(252)  # 年化波动率
        risk_score = 10 - min(9, volatility * 50)  # 波动率越低，风险评分越高
        
        # 计算收益评分（基于近期表现）
        returns_1m = df['收盘'].iloc[-1] / df['收盘'].iloc[-21] - 1 if len(df) >= 21 else np.nan
        returns_3m = df['收盘'].iloc[-1] / df['收盘'].iloc[-63] - 1 if len(df) >= 63 else np.nan
        returns_6m = df['收盘'].iloc[-1] / df['收盘'].iloc[-126] - 1 if len(df) >= 126 else np.nan
        returns_1y = df['收盘'].iloc[-1] / df['收盘'].iloc[-252] - 1 if len(df) >= 252 else np.nan
        
        # 综合收益评分（近期权重更高）
        weights = [0.4, 0.3, 0.2, 0.1]
        returns = [returns_1m, returns_3m, returns_6m, returns_1y]
        weighted_returns = [w * r for w, r in zip(weights, returns) if not np.isnan(r)]
        
        if weighted_returns:
            return_score = 5 + sum(weighted_returns) * 20  # 将收益率转换为1-10分
            return_score = min(10, max(1, return_score))  # 限制在1-10范围内
        else:
            return_score = 5  # 默认中等评分
        
        # 计算溢价率评分
        # 这里简化处理，实际中应从IOPV计算
        premium_score = 5  # 默认中等评分
        
        # 综合评分
        total_score = (
            0.4 * return_score +
            0.3 * risk_score +
            0.2 * liquidity_score +
            0.1 * premium_score
        )
        
        return {
            'etf_code': etf_code,
            'name': get_etf_name(etf_code),
            'total_score': round(total_score, 2),
            'return_score': round(return_score, 1),
            'risk_score': round(risk_score, 1),
            'liquidity_score': round(liquidity_score, 1),
            'premium_score': round(premium_score, 1)
        }
    except Exception as e:
        logger.error(f"计算{etf_code}评分失败: {str(e)}", exc_info=True)
        return None

def get_etf_name(etf_code):
    """获取ETF名称"""
    # 从ETF列表中获取名称
    etf_list = get_all_etf_list()
    if etf_list is not None and not etf_list.empty:
        etf = etf_list[etf_list['code'] == etf_code]
        if not etf.empty:
            return etf.iloc[0]['name']
    
    return etf_code  # 如果没有数据，返回代码

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
                "content": message,
                "mentioned_list": ["@all"]
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

def get_crawl_status():
    """获取当前爬取状态"""
    status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def update_crawl_status(etf_code, status, error=None):
    """更新爬取状态"""
    status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
    crawl_status = get_crawl_status()
    crawl_status[etf_code] = {
        'status': status,
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'error': error if error else ''
    }
    try:
        with open(status_file, 'w') as f:
            json.dump(crawl_status, f, indent=2)
    except Exception as e:
        logger.error(f"更新爬取状态失败: {str(e)}")

def crawl_etf_data(data_type='daily'):
    """爬取ETF数据
    参数:
        data_type: 数据类型 ('daily' 或 'intraday')
    返回:
        dict: 爬取结果
    """
    try:
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
        date_str = get_beijing_time().strftime('%Y-%m-%d')
        
        for _, etf in etf_list.iterrows():
            etf_code = etf['code']
            
            # 获取起始日期（从缓存中获取最后日期）- 关键增量爬取逻辑
            cached_data = load_from_cache(etf_code, data_type)
            start_date = None
            if cached_data is not None and not cached_data.empty:
                last_date = cached_data['日期'].max()
                # 检查是否已获取到最新数据
                if (datetime.datetime.now().date() - last_date.date()).days <= 1:
                    logger.info(f"ETF {etf_code} 已有最新数据到 {last_date.strftime('%Y-%m-%d')}，跳过爬取")
                    skipped_count += 1
                    continue
                start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"ETF {etf_code} 已有数据到 {last_date.strftime('%Y-%m-%d')}，从 {start_date} 开始获取新数据")
            else:
                logger.info(f"ETF {etf_code} 无缓存数据，将获取最近一年的数据")
                # 设置起始日期为一年前
                start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
            
            try:
                # 标记开始
                update_crawl_status(etf_code, 'in_progress')
                
                # 爬取数据
                data = get_etf_data(etf_code, data_type, start_date=start_date)
                
                # 检查结果
                if data is not None and not data.empty and check_data_completeness(data):
                    # 保存数据
                    save_to_cache(etf_code, data, data_type)
                    update_crawl_status(etf_code, 'success')
                    success_count += 1
                    logger.info(f"成功爬取 {etf_code} {data_type}数据，共 {len(data)} 条记录")
                else:
                    update_crawl_status(etf_code, 'failed', 'Incomplete data')
                    failed_count += 1
                    logger.warning(f"爬取 {etf_code} {data_type}数据失败：返回不完整数据")
            except Exception as e:
                error_msg = f"【系统错误】爬取 {etf_code} 时出错: {str(e)}"
                logger.error(error_msg)
                send_wecom_message(error_msg)
                update_crawl_status(etf_code, 'failed', error_msg)
                failed_count += 1
            
            # 避免请求过快
            time.sleep(1)
        
        logger.info(f"ETF数据爬取完成: 成功 {success_count}, 失败 {failed_count}, 跳过 {skipped_count}")
        return {
            "status": "success" if failed_count == 0 else "partial_success",
            "success_count": success_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count
        }
    except Exception as e:
        error_msg = f"【系统错误】ETF数据爬取任务异常: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return {"status": "error", "message": str(e)}
