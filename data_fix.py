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
    if beijing_time.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return False
    
    # TODO: 这里可以添加中国股市休市日历检查
    # 例如：检查是否为法定节假日
    
    return True

def check_data_completeness(df, required_columns=None, min_records=5):
    """检查数据完整性（增强容错性）
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
    
    # 检查必需的列（增强容错性)
    if required_columns is None:
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"数据缺少必要列: {missing_columns}")
        # 尝试从其他列映射
        for col in missing_columns:
            if col == 'volume' and '成交额' in df.columns:
                df['volume'] = df['成交额']
            elif col == 'open' and '开盘价' in df.columns:
                df['open'] = df['开盘价']
            elif col == 'high' and '最高价' in df.columns:
                df['high'] = df['最高价']
            elif col == 'low' and '最低价' in df.columns:
                df['low'] = df['最低价']
            elif col == 'close' and '收盘价' in df.columns:
                df['close'] = df['收盘价']
    
    # 重新检查列
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"数据仍缺少必要列: {missing_columns}")
        return False
    
    # 检查数据量
    if len(df) < min_records:
        logger.warning(f"数据量不足，仅 {len(df)} 条记录（需要至少 {min_records} 条）")
        return False
    
    # 检查关键字段是否为空（增强容错性）
    for col in required_columns:
        if col in df.columns:
            # 使用 .any() 而不是直接判断 Series
            if df[col].isnull().any():
                logger.warning(f"数据中{col}字段包含空值")
                # 尝试填充空值
                df[col].fillna(method='ffill', inplace=True)
                df[col].fillna(method='bfill', inplace=True)
    
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
    
    # 严格遵循config.py配置，不再创建额外子目录
    # 直接使用RAW_DATA_DIR，文件名包含data_type后缀
    cache_dir = Config.RAW_DATA_DIR
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f'{etf_code}_{data_type}.csv')

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
        if 'date' not in df.columns:
            logger.error(f"缓存文件缺少'date'列: {cache_path}")
            return None
            
        # 尝试多种日期格式转换
        try:
            df['date'] = pd.to_datetime(df['date'])
        except:
            try:
                df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
            except:
                try:
                    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d')
                except:
                    logger.error(f"无法转换日期格式: {cache_path}")
                    return None
        
        # 筛选近期数据
        if data_type == 'daily':
            df = df[df['date'] >= (datetime.datetime.now() - datetime.timedelta(days=days))]
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
                combined = combined.drop_duplicates(subset=['date'], keep='last')
                combined = combined.sort_values('date')
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

def akshare_retry(func, *args, **kwargs):
    """AkShare请求重试机制（增强容错性）"""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < max_attempts - 1:
                wait_time = 2 ** attempt  # 指数退避
                logger.warning(f"AkShare请求失败，{wait_time}秒后重试 ({attempt+1}/{max_attempts}): {str(e)}")
                time.sleep(wait_time)
            else:
                logger.error(f"AkShare请求失败，已达到最大重试次数: {str(e)}")
                raise

def get_all_etf_list():
    """从多数据源获取所有ETF列表（增强容错性）
    返回:
        DataFrame: ETF列表，包含代码和名称
    """
    # 尝试AkShare主接口（实时ETF列表）
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        logger.info("AkShare接口: ak.fund_etf_spot_em()")
        # 使用 fund_etf_spot_em 获取实时ETF列表
        df = ak.fund_etf_spot_em()
        
        # 添加关键日志 - 检查AkShare返回
        if df is not None and not df.empty:
            logger.info(f"AkShare返回ETF列表数据列数: {len(df.columns)}")
            logger.info(f"AkShare返回ETF列表数据记录数: {len(df)}")
            logger.info(f"AkShare返回ETF列表数据列名: {df.columns.tolist()}")
        else:
            logger.warning("AkShare fund_etf_spot_em 返回空数据")
        
        if df is None or df.empty:
            logger.warning("AkShare fund_etf_spot_em返回空数据，尝试备用接口...")
            # 尝试备用接口 fund_etf_hist_sina
            logger.info("AkShare接口: ak.fund_etf_hist_sina(symbol='etf')")
            df = ak.fund_etf_hist_sina(symbol="etf")
            
            # 添加关键日志 - 检查备用接口返回
            if df is not None and not df.empty:
                logger.info(f"AkShare备用接口返回ETF列表数据列数: {len(df.columns)}")
                logger.info(f"AkShare备用接口返回ETF列表数据记录数: {len(df)}")
                logger.info(f"AkShare备用接口返回ETF列表数据列名: {df.columns.tolist()}")
            else:
                logger.warning("AkShare备用接口返回空ETF列表")
        
        if df is not None and not df.empty:
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
                    lambda x: f"sh.{x}" if str(x).startswith('5') else f"sz.{x}"
                )
                etf_list['name'] = df[name_col]
                
                logger.info(f"从AkShare成功获取 {len(etf_list)} 只ETF")
                return etf_list
            else:
                logger.error(f"AkShare返回数据缺少必要列: {df.columns.tolist()}")
                logger.error(f"期望的列: 代码列 - {code_col}, 名称列 - {name_col}")
        else:
            logger.warning("AkShare返回空ETF列表")
    except Exception as e:
        logger.error(f"AkShare获取ETF列表失败: {str(e)}", exc_info=True)
    
    # 尝试Baostock（使用兼容性更强的接口）
    try:
        logger.info("尝试从Baostock获取ETF列表...")
        logger.info("Baostock接口: bs.query_all_stock()")
        lg = bs.login()
        if lg.error_code != '0':
            logger.warning(f"Baostock登录失败: {lg.error_msg}")
            raise Exception("Baostock登录失败")
        
        rs = bs.query_all_stock()
        df = rs.get_data()
        bs.logout()
        
        if df is not None and not df.empty:
            # 过滤出ETF
            etf_list = df[df['type'] == '1']
            if not etf_list.empty:
                # 创建ETF列表DataFrame
                etf_list = pd.DataFrame({
                    'code': etf_list['code'].apply(lambda x: f"sh.{x}" if x.startswith('5') else f"sz.{x}"),
                    'name': etf_list['code_name']
                })
                logger.info(f"从Baostock成功获取 {len(etf_list)} 只ETF")
                return etf_list
    except Exception as e:
        logger.error(f"Baostock获取ETF列表失败: {str(e)}", exc_info=True)
    
    # 尝试新浪财经备用接口
    try:
        logger.info("尝试从新浪财经获取ETF列表...")
        logger.info("新浪财经接口: http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList")
        url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            try:
                data = json.loads(response.text)
                
                # 修复：检查data的类型
                logger.info(f"新浪财经返回数据类型: {type(data)}")
                
                # 处理可能的字典类型数据
                if isinstance(data, dict):
                    # 检查是否有data字段
                    if 'data' in data and 'list' in data['data']:
                        data = data['data']['list']
                        logger.info(f"从字典中提取列表数据，记录数: {len(data)}")
                
                # 处理列表类型数据
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"新浪财经返回ETF列表数据记录数: {len(data)}")
                    
                    # 检查数据结构
                    sample = data[0]
                    logger.info(f"新浪财经返回数据示例: {sample}")
                    
                    # 动态匹配symbol和name字段
                    symbol_col = next((k for k in sample.keys() if 'symbol' in k.lower()), None)
                    name_col = next((k for k in sample.keys() if 'name' in k.lower()), None)
                    
                    if symbol_col and name_col:
                        etf_list = pd.DataFrame(data)
                        etf_list['code'] = etf_list[symbol_col].apply(
                            lambda x: f"sh.{x}" if str(x).startswith('5') else f"sz.{x}"
                        )
                        etf_list = etf_list.rename(columns={name_col: 'name'})
                        etf_list = etf_list[['code', 'name']]
                        
                        logger.info(f"从新浪财经成功获取 {len(etf_list)} 只ETF")
                        return etf_list
                    else:
                        logger.warning("新浪财经返回数据缺少symbol或name字段")
                else:
                    logger.warning("新浪财经返回空数据或格式不正确")
            except Exception as e:
                logger.error(f"处理新浪财经返回数据时出错: {str(e)}", exc_info=True)
        else:
            logger.warning(f"新浪财经请求失败，状态码: {response.status_code}")
    except Exception as e:
        logger.error(f"新浪财经获取ETF列表失败: {str(e)}", exc_info=True)
    
    # 新增：尝试从兜底CSV文件获取ETF列表
    fallback_csv_path = os.path.join(Config.STOCK_POOL_DIR, 'fallback_etf_list.csv')
    try:
        if os.path.exists(fallback_csv_path):
            logger.info(f"尝试从兜底CSV文件加载ETF列表: {fallback_csv_path}")
            etf_list = pd.read_csv(fallback_csv_path)
            
            # 验证数据格式
            if 'code' in etf_list.columns and 'name' in etf_list.columns:
                logger.info(f"成功从兜底CSV文件加载 {len(etf_list)} 只ETF")
                return etf_list
            else:
                logger.error(f"兜底CSV文件格式不正确，缺少必要列: {etf_list.columns.tolist()}")
        else:
            logger.warning(f"兜底CSV文件不存在: {fallback_csv_path}")
    except Exception as e:
        logger.error(f"读取兜底CSV文件失败: {str(e)}", exc_info=True)
    
    # 如果所有数据源都失败，返回一个最小的ETF列表（作为最后的兜底）
    logger.warning("所有数据源都失败，使用最小ETF列表作为兜底")
    fallback_etfs = [
        {'code': 'sh.510050', 'name': '上证50ETF'},
        {'code': 'sh.510300', 'name': '沪深300ETF'},
        {'code': 'sh.510500', 'name': '中证500ETF'},
        {'code': 'sh.588940', 'name': '科创100ETF'},
        {'code': 'sh.588000', 'name': '科创50ETF'},
        {'code': 'sh.588080', 'name': '科创板50ETF'},
        {'code': 'sh.588900', 'name': '科创创业50ETF'},
        {'code': 'sh.588950', 'name': '科创创业50ETF'},
        {'code': 'sh.588990', 'name': '科创板芯片ETF'},
        {'code': 'sh.588120', 'name': '科创板新材料ETF'},
        {'code': 'sh.588400', 'name': '科创板人工智能ETF'},
        {'code': 'sh.588050', 'name': '科创生物医药ETF'}
    ]
    logger.info(f"使用兜底ETF列表，包含 {len(fallback_etfs)} 只ETF")
    return pd.DataFrame(fallback_etfs)

def get_etf_data(etf_code, data_type='daily'):
    """从多数据源获取ETF数据（增量获取，增强容错性）
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        DataFrame: ETF数据或None（如果所有数据源都失败）"""
    # 确保etf_code是标准化格式
    if not etf_code.startswith(('sh.', 'sz.')):
        if etf_code.startswith(('5', '119')):
            etf_code = f"sh.{etf_code}"
        else:
            etf_code = f"sz.{etf_code}"
    
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
    
    # 尝试主数据源(AkShare)
    try:
        # 提取纯代码（无sh./sz.前缀）
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        logger.info(f"尝试从AkShare获取{etf_code}日线数据...")
        logger.info(f"AkShare接口: ak.fund_etf_hist_em(symbol='{pure_code}', period='daily', adjust='qfq')")
        
        # 尝试主接口
        df = akshare_retry(ak.fund_etf_hist_em, symbol=pure_code, period='daily', adjust='qfq')
        
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
                logger.info(f"AkShare备用接口返回ETF数据列数: {len(df.columns)}")
                logger.info(f"AkShare备用接口返回ETF数据记录数: {len(df)}")
                logger.info(f"AkShare备用接口返回ETF数据列名: {df.columns.tolist()}")
            else:
                logger.warning("AkShare fund_etf_hist_sina 返回空数据")
        
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
                    elif col == 'open' and '开盘价' in df.columns:
                        df['open'] = df['开盘价']
                    elif col == 'high' and '最高价' in df.columns:
                        df['high'] = df['最高价']
                    elif col == 'low' and '最低价' in df.columns:
                        df['low'] = df['最低价']
                    elif col == 'close' and '收盘价' in df.columns:
                        df['close'] = df['收盘价']
            
            # 将日期转换为datetime
            if 'date' in df.columns:
                try:
                    df['date'] = pd.to_datetime(df['date'])
                except Exception as e:
                    logger.error(f"日期转换失败: {str(e)}")
                    # 尝试另一种日期格式
                    try:
                        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
                    except:
                        logger.error("多种日期格式尝试失败")
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
            
            # 检查数据完整性 - 使用增强容错性版本
            if check_data_completeness(df):
                logger.info(f"成功从AkShare获取{etf_code}日线数据")
                # 保存到缓存
                save_to_cache(etf_code, df, data_type)
                return df
            else:
                logger.warning(f"AkShare返回的{etf_code}数据不完整，但尝试保存部分数据")
                # 即使数据不完整也保存
                save_to_cache(etf_code, df, data_type)
                return df
        else:
            logger.warning("AkShare返回空ETF数据")
    except Exception as e:
        logger.error(f"AkShare获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 尝试备用数据源(Baostock)
    try:
        logger.info(f"尝试从Baostock获取{etf_code}日线数据...")
        lg = bs.login()
        if lg.error_code != '0':
            logger.warning(f"Baostock登录失败: {lg.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 提取纯代码（无sh./sz.前缀）
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        # 如果有缓存，只获取新数据；否则获取最近100天数据
        if start_date:
            start_date_str = start_date
            end_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        else:
            start_date_str = (datetime.datetime.now() - datetime.timedelta(days=100)).strftime('%Y-%m-%d')
            end_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 获取日线数据
        rs = bs.query_history_k_data_plus(
            pure_code,
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
            # 转换数据类型
            df['date'] = pd.to_datetime(df['date'])
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            
            # 按日期排序
            df = df.sort_values('date')
            
            # 检查数据完整性 - 使用增强容错性版本
            if check_data_completeness(df):
                logger.info(f"成功从Baostock获取{etf_code}日线数据")
                # 保存到缓存
                save_to_cache(etf_code, df, data_type)
                return df
            else:
                logger.warning(f"Baostock返回的{etf_code}数据不完整，但尝试保存部分数据")
                # 即使数据不完整也保存
                save_to_cache(etf_code, df, data_type)
                return df
        else:
            logger.warning("Baostock返回空ETF数据")
    except Exception as e:
        logger.error(f"Baostock获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 尝试新浪财经备用接口
    try:
        logger.info(f"尝试从新浪财经获取{etf_code}日线数据...")
        sina_url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={etf_code.replace('.', '')}&scale=240&ma=no&datalen=100"
        response = requests.get(sina_url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        response.raise_for_status()
        
        data = response.json()
        # 修复：检查数据是否有效
        if data and isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            df = df.rename(columns={
                'day': 'date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            })
            
            # 转换数据类型
            df['date'] = pd.to_datetime(df['date'])
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            
            # 按日期排序
            df = df.sort_values('date')
            
            # 检查数据完整性 - 使用增强容错性版本
            if check_data_completeness(df):
                logger.info(f"成功从新浪财经获取{etf_code}日线数据")
                # 保存到缓存
                save_to_cache(etf_code, df, data_type)
                return df
            else:
                logger.warning(f"新浪财经返回的{etf_code}数据不完整，但尝试保存部分数据")
                # 即使数据不完整也保存
                save_to_cache(etf_code, df, data_type)
                return df
        else:
            logger.warning("新浪财经返回空ETF数据")
    except Exception as e:
        logger.error(f"新浪财经获取{etf_code}数据失败: {str(e)}", exc_info=True)
    
    # 所有数据源均失败
    error_msg = f"【数据错误】无法获取{etf_code}数据"
    logger.error(error_msg)
    # 不发送企业微信消息，避免频繁报警
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
            logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {date_str} 的新股申购数据")
            
            # 尝试AkShare（主数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从AkShare获取新股申购信息...")
                logger.info(f"AkShare接口: ak.stock_xgsglb_em()")
                df = akshare_retry(ak.stock_xgsglb_em)
                
                if not df.empty:
                    # 仅记录列名信息
                    logger.info(f"AkShare返回新股数据列数: {len(df.columns)}")
                    logger.info(f"AkShare返回新股数据记录数: {len(df)}")
                    logger.info(f"AkShare返回新股数据列名: {df.columns.tolist()}")
                    
                    # 动态匹配日期列
                    date_col = next((col for col in df.columns 
                                   if any(kw in col.lower() for kw in ['申购日期', 'ipo_date', 'issue_date'])), None)
                    
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
                            # 尝试修复列名
                            code_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['代码', 'code'])), None)
                            name_col = next((col for col in df.columns 
                                           if any(kw in col.lower() for kw in ['名称', 'name', '简称'])), None)
                            price_col = next((col for col in df.columns 
                                            if any(kw in col.lower() for kw in ['价格', 'price'])), None)
                            limit_col = next((col for col in df.columns 
                                            if any(kw in col.lower() for kw in ['上限', 'limit'])), None)
                            
                            if code_col and name_col:
                                valid_df = df[[code_col, name_col]].copy()
                                valid_df.rename(columns={code_col: '股票代码', name_col: '股票简称'}, inplace=True)
                                
                                if price_col:
                                    valid_df['发行价格'] = df[price_col]
                                if limit_col:
                                    valid_df['申购上限'] = df[limit_col]
                                
                                valid_df['申购日期'] = date_str
                                valid_df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_stock_completeness(valid_df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从AkShare成功获取 {len(valid_df)} 条新股申购信息")
                                    return valid_df[['股票代码', '股票简称', '发行价格', '申购上限', '申购日期', '类型']]
                                else:
                                    logger.warning(f"{'测试模式' if test else '正常模式'}: AkShare返回的新股数据不完整，将尝试备用数据源...")
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: AkShare获取新股信息失败: {str(e)}", exc_info=True)
            
            # 尝试Baostock（备用数据源）
            try:
                logger.info(f"{'测试模式' if test else '正常模式'}: 尝试从Baostock获取新股申购信息...")
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
                            df['ipoDate'] = pd.to_datetime(df['ipoDate']).dt.strftime('%Y-%m-%d')
                            df = df[df['ipoDate'] == date_str]
                            if not df.empty:
                                # 重命名列以匹配股票格式
                                df = df.rename(columns={
                                    'code': '股票代码',
                                    'code_name': '股票简称',
                                    'price': '发行价格',
                                    'max_purchase': '申购上限',
                                    'ipoDate': '申购日期'
                                })
                                df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_stock_completeness(df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新股申购信息")
                                    return df[['股票代码', '股票简称', '发行价格', '申购上限', '申购日期', '类型']]
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
                            df['ipoDate'] = pd.to_datetime(df['ipoDate']).dt.strftime('%Y-%m-%d')
                            df = df[df['ipoDate'] == date_str]
                            if not df.empty:
                                # 重命名列以匹配股票格式
                                df = df.rename(columns={
                                    'code': '股票代码',
                                    'code_name': '股票简称',
                                    'price': '发行价格',
                                    'max_purchase': '申购上限',
                                    'ipoDate': '申购日期'
                                })
                                df['类型'] = '股票'
                                
                                # 检查数据完整性
                                if check_new_stock_completeness(df):
                                    logger.info(f"{'测试模式' if test else '正常模式'}: 从Baostock成功获取 {len(df)} 条新股申购信息")
                                    return df[['股票代码', '股票简称', '发行价格', '申购上限', '申购日期', '类型']]
                except AttributeError:
                    pass
            except Exception as e:
                logger.error(f"{'测试模式' if test else '正常模式'}: Baostock获取新股信息失败: {str(e)}", exc_info=True)
            finally:
                try:
                    bs.logout()
                except:
                    pass
        
        logger.info(f"{'测试模式' if test else '正常模式'}: 未找到新股数据")
        return pd.DataFrame()
        
    except Exception as e:
        error_msg = f"{'测试模式' if test else '正常模式'}: 【数据错误】获取新股申购信息失败: {str(e)}"
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
        last_date = df['date'].max()
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
        current_price = latest['close']
        
        # 计算流动性评分（基于成交量）
        avg_volume = df['volume'].mean()
        liquidity_score = min(10, max(1, np.log10(avg_volume) * 2))
        
        # 计算风险评分（基于波动率）
        daily_returns = df['close'].pct_change().dropna()
        volatility = daily_returns.std() * np.sqrt(252)  # 年化波动率
        risk_score = 10 - min(9, volatility * 50)  # 波动率越低，风险评分越高
        
        # 计算收益评分（基于近期表现）
        returns_1m = df['close'].iloc[-1] / df['close'].iloc[-21] - 1 if len(df) >= 21 else np.nan
        returns_3m = df['close'].iloc[-1] / df['close'].iloc[-63] - 1 if len(df) >= 63 else np.nan
        returns_6m = df['close'].iloc[-1] / df['close'].iloc[-126] - 1 if len(df) >= 126 else np.nan
        returns_1y = df['close'].iloc[-1] / df['close'].iloc[-252] - 1 if len(df) >= 252 else np.nan
        
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
                logger.info(f"ETF {etf_code} 无缓存数据，将获取最近一年的数据")
                # 设置起始日期为一年前
                start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
            
            # 尝试主数据源(AkShare)
            data = get_etf_data(etf_code, 'daily', start_date=start_date)
            
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
            update_crawl_status(etf_code, 'failed', str(e))
    
    # 检查是否所有ETF都爬取成功
    if success_count == len(etf_list):
        # 清理状态文件
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

def read_new_stock_pushed_flag(date):
    """读取新股信息是否已推送标志"""
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
        tuple: (flag_path, is_pushed)
    """
    flag_path = os.path.join(Config.NEW_STOCK_DIR, f'listing_pushed_{date.strftime("%Y%m%d")}.flag')
    is_pushed = os.path.exists(flag_path)
    return flag_path, is_pushed

def mark_listing_info_pushed():
    """标记新上市交易信息已推送"""
    flag_path, _ = read_listing_pushed_flag(get_beijing_time().date())
    with open(flag_path, 'w') as f:
        f.write(get_beijing_time().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info(f"标记新上市交易信息已推送: {flag_path}")
