"""数据源模块 - 仅包含数据爬取相关功能"""
import os
import pandas as pd
import numpy as np
import datetime
import requests
import akshare as ak
import baostock as bs
from config import Config
from logger import get_logger
from retrying import retry

logger = get_logger(__name__)

# 定义统一的中文列名标准
UNIFIED_COLUMNS = {
    'date': '日期',
    'open': '开盘',
    'high': '最高',
    'low': '最低',
    'close': '收盘',
    'volume': '成交量',
    'amount': '成交额',
    'amplitude': '振幅',
    'change_pct': '涨跌幅',
    'change_amount': '涨跌额',
    'turnover_rate': '换手率'
}

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

def crawl_akshare_primary(etf_code, start_date=None):
    """从AkShare主接口爬取ETF数据
    参数:
        etf_code: ETF代码
        start_date: 开始日期（可选）
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 提取纯代码（无sh./sz.前缀）
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        logger.info(f"尝试从AkShare主接口获取{etf_code}日线数据...")
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
            return None
        
        # 统一列名
        column_mapping = {
            '日期': UNIFIED_COLUMNS['date'],
            '开盘': UNIFIED_COLUMNS['open'],
            '最高': UNIFIED_COLUMNS['high'],
            '最低': UNIFIED_COLUMNS['low'],
            '收盘': UNIFIED_COLUMNS['close'],
            '成交量': UNIFIED_COLUMNS['volume'],
            '成交额': UNIFIED_COLUMNS['amount'],
            '振幅': UNIFIED_COLUMNS['amplitude'],
            '涨跌幅': UNIFIED_COLUMNS['change_pct'],
            '涨跌额': UNIFIED_COLUMNS['change_amount'],
            '换手率': UNIFIED_COLUMNS['turnover_rate']
        }
        
        # 选择存在的列
        existing_columns = [col for col in column_mapping.keys() if col in df.columns]
        rename_dict = {col: column_mapping[col] for col in existing_columns}
        
        # 重命名列
        df = df.rename(columns=rename_dict)
        
        # 确保必要列存在
        required_columns = list(UNIFIED_COLUMNS.values())
        for col in required_columns:
            if col not in df.columns:
                # 尝试从其他列映射
                if col == UNIFIED_COLUMNS['volume'] and UNIFIED_COLUMNS['amount'] in df.columns:
                    df[UNIFIED_COLUMNS['volume']] = df[UNIFIED_COLUMNS['amount']]
        
        # 将日期转换为datetime
        if UNIFIED_COLUMNS['date'] in df.columns:
            try:
                df[UNIFIED_COLUMNS['date']] = pd.to_datetime(df[UNIFIED_COLUMNS['date']])
            except Exception as e:
                logger.error(f"日期转换失败: {str(e)}")
                return None
        
        # 如果指定了起始日期，筛选数据
        if start_date:
            try:
                start_date_dt = pd.to_datetime(start_date)
                df = df[df[UNIFIED_COLUMNS['date']] >= start_date_dt]
            except Exception as e:
                logger.error(f"日期筛选失败: {str(e)}")
        
        # 按日期排序
        if UNIFIED_COLUMNS['date'] in df.columns:
            df = df.sort_values(UNIFIED_COLUMNS['date'])
        
        # 检查数据完整性
        if check_data_completeness(df):
            logger.info(f"成功从AkShare主接口获取{etf_code}日线数据")
            return df
        else:
            logger.warning(f"AkShare主接口返回的{etf_code}数据不完整")
            return None
    except Exception as e:
        logger.error(f"AkShare主接口获取{etf_code}数据失败: {str(e)}", exc_info=True)
        return None

def crawl_akshare_backup(etf_code, start_date=None):
    """从AkShare备用接口爬取ETF数据
    参数:
        etf_code: ETF代码
        start_date: 开始日期（可选）
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 提取纯代码（无sh./sz.前缀）
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        logger.info(f"尝试从AkShare备用接口获取{etf_code}日线数据...")
        logger.info(f"AkShare接口: ak.fund_etf_hist_sina(symbol='{pure_code}')")
        
        # 尝试备用接口
        df = akshare_retry(ak.fund_etf_hist_sina, symbol=pure_code)
        
        # 添加关键日志 - 检查AkShare返回
        if df is not None and not df.empty:
            logger.info(f"AkShare备用接口返回ETF数据列数: {len(df.columns)}")
            logger.info(f"AkShare备用接口返回ETF数据记录数: {len(df)}")
            logger.info(f"AkShare备用接口返回ETF数据列名: {df.columns.tolist()}")
        else:
            logger.warning("AkShare fund_etf_hist_sina 返回空数据")
            return None
        
        # 统一列名
        column_mapping = {
            '日期': UNIFIED_COLUMNS['date'],
            '开盘': UNIFIED_COLUMNS['open'],
            '最高': UNIFIED_COLUMNS['high'],
            '最低': UNIFIED_COLUMNS['low'],
            '收盘': UNIFIED_COLUMNS['close'],
            '成交量': UNIFIED_COLUMNS['volume'],
            '成交额': UNIFIED_COLUMNS['amount']
        }
        
        # 选择存在的列
        existing_columns = [col for col in column_mapping.keys() if col in df.columns]
        rename_dict = {col: column_mapping[col] for col in existing_columns}
        
        # 重命名列
        df = df.rename(columns=rename_dict)
        
        # 确保必要列存在
        required_columns = list(UNIFIED_COLUMNS.values())
        for col in required_columns:
            if col not in df.columns:
                # 尝试从其他列映射
                if col == UNIFIED_COLUMNS['volume'] and UNIFIED_COLUMNS['amount'] in df.columns:
                    df[UNIFIED_COLUMNS['volume']] = df[UNIFIED_COLUMNS['amount']]
        
        # 将日期转换为datetime
        if UNIFIED_COLUMNS['date'] in df.columns:
            try:
                df[UNIFIED_COLUMNS['date']] = pd.to_datetime(df[UNIFIED_COLUMNS['date']])
            except Exception as e:
                logger.error(f"日期转换失败: {str(e)}")
                return None
        
        # 如果指定了起始日期，筛选数据
        if start_date:
            try:
                start_date_dt = pd.to_datetime(start_date)
                df = df[df[UNIFIED_COLUMNS['date']] >= start_date_dt]
            except Exception as e:
                logger.error(f"日期筛选失败: {str(e)}")
        
        # 按日期排序
        if UNIFIED_COLUMNS['date'] in df.columns:
            df = df.sort_values(UNIFIED_COLUMNS['date'])
        
        # 增强数据完整性，计算缺失指标
        df = enhance_data_integrity(df)
        
        # 检查数据完整性
        if check_data_completeness(df):
            logger.info(f"成功从AkShare备用接口获取{etf_code}日线数据")
            return df
        else:
            logger.warning(f"AkShare备用接口返回的{etf_code}数据不完整")
            return None
    except Exception as e:
        logger.error(f"AkShare备用接口获取{etf_code}数据失败: {str(e)}", exc_info=True)
        return None

def crawl_baostock(etf_code, start_date=None):
    """从Baostock爬取ETF数据
    参数:
        etf_code: ETF代码
        start_date: 开始日期（可选）
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 提取纯代码（无sh./sz.前缀）
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        logger.info(f"尝试从Baostock获取{etf_code}日线数据...")
        
        # 登录Baostock
        lg = bs.login()
        if lg.error_code != '0':
            logger.warning(f"Baostock登录失败: {lg.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 获取更多字段：增加成交额(amount)、涨跌幅(percent)、换手率(turn)
        fields = "date,open,high,low,close,volume,amount,change,percent,turn"
        
        # 如果有缓存，只获取新数据；否则获取最近100天数据
        if start_date:
            start_date_str = start_date
            end_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        else:
            start_date_str = (datetime.datetime.now() - datetime.timedelta(days=100)).strftime('%Y-%m-%d')
            end_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 获取日线数据 - 请求更多字段
        rs = bs.query_history_k_data_plus(
            pure_code,
            fields,
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
            # 统一列名
            column_mapping = {
                'date': UNIFIED_COLUMNS['date'],
                'open': UNIFIED_COLUMNS['open'],
                'high': UNIFIED_COLUMNS['high'],
                'low': UNIFIED_COLUMNS['low'],
                'close': UNIFIED_COLUMNS['close'],
                'volume': UNIFIED_COLUMNS['volume'],
                'amount': UNIFIED_COLUMNS['amount'],
                'percent': UNIFIED_COLUMNS['change_pct'],
                'change': UNIFIED_COLUMNS['change_amount'],
                'turn': UNIFIED_COLUMNS['turnover_rate']
            }
            
            # 选择存在的列
            existing_columns = [col for col in column_mapping.keys() if col in df.columns]
            rename_dict = {col: column_mapping[col] for col in existing_columns}
            
            # 重命名列
            df = df.rename(columns=rename_dict)
            
            # 转换数据类型
            df[UNIFIED_COLUMNS['date']] = pd.to_datetime(df[UNIFIED_COLUMNS['date']])
            numeric_cols = [col for col in UNIFIED_COLUMNS.values() 
                           if col in df.columns and col != UNIFIED_COLUMNS['date']]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            
            # 计算缺失指标（如果存在基础数据）
            if UNIFIED_COLUMNS['high'] in df.columns and UNIFIED_COLUMNS['low'] in df.columns:
                # 计算振幅：(最高价-最低价)/前一日收盘价
                if UNIFIED_COLUMNS['close'] in df.columns:
                    df[UNIFIED_COLUMNS['amplitude']] = (
                        (df[UNIFIED_COLUMNS['high']] - df[UNIFIED_COLUMNS['low']]) / 
                        df[UNIFIED_COLUMNS['close']].shift(1) * 100
                    ).round(2)
            
            # 按日期排序
            df = df.sort_values(UNIFIED_COLUMNS['date'])
            
            # 检查数据完整性
            if check_data_completeness(df):
                logger.info(f"成功从Baostock获取{etf_code}日线数据（{len(df)}条记录）")
                return df
            else:
                logger.warning(f"Baostock返回的{etf_code}数据不完整，但将尝试使用")
                return df
        else:
            logger.warning("Baostock返回空ETF数据")
            return None
    except Exception as e:
        logger.error(f"Baostock获取{etf_code}数据失败: {str(e)}", exc_info=True)
        return None

def crawl_sina_finance(etf_code, start_date=None):
    """从新浪财经爬取ETF数据
    参数:
        etf_code: ETF代码
        start_date: 开始日期（可选）
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 修正交易所前缀
        if etf_code.startswith(('sh.', 'sz.')):
            full_code = etf_code.replace('.', '')
        elif etf_code.startswith('5'):
            full_code = f"sh{etf_code}"
        else:
            full_code = f"sz{etf_code}"
        
        logger.info(f"尝试从新浪财经获取{etf_code}日线数据...")
        
        # 使用更全面的API端点
        sina_url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={full_code}&scale=240&ma=no&datalen=100"
        
        response = requests.get(sina_url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        response.raise_for_status()
        
        data = response.json()
        # 修复：检查数据是否有效
        if data and isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            
            # 统一列名
            column_mapping = {
                'day': UNIFIED_COLUMNS['date'],
                'open': UNIFIED_COLUMNS['open'],
                'high': UNIFIED_COLUMNS['high'],
                'low': UNIFIED_COLUMNS['low'],
                'close': UNIFIED_COLUMNS['close'],
                'volume': UNIFIED_COLUMNS['volume'],
                'amount': UNIFIED_COLUMNS['amount'],
                'amplitude': UNIFIED_COLUMNS['amplitude'],
                'change_pct': UNIFIED_COLUMNS['change_pct'],
                'change_amount': UNIFIED_COLUMNS['change_amount'],
                'turnover_rate': UNIFIED_COLUMNS['turnover_rate']
            }
            
            # 选择存在的列
            existing_columns = [col for col in column_mapping.keys() if col in df.columns]
            rename_dict = {col: column_mapping[col] for col in existing_columns}
            
            # 重命名列
            df = df.rename(columns=rename_dict)
            
            # 转换数据类型
            df[UNIFIED_COLUMNS['date']] = pd.to_datetime(df[UNIFIED_COLUMNS['date']])
            numeric_cols = [col for col in UNIFIED_COLUMNS.values() 
                           if col in df.columns and col != UNIFIED_COLUMNS['date']]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            
            # 计算缺失指标
            if UNIFIED_COLUMNS['high'] in df.columns and UNIFIED_COLUMNS['low'] in df.columns:
                # 计算振幅
                if UNIFIED_COLUMNS['close'] in df.columns:
                    df[UNIFIED_COLUMNS['amplitude']] = (
                        (df[UNIFIED_COLUMNS['high']] - df[UNIFIED_COLUMNS['low']]) / 
                        df[UNIFIED_COLUMNS['close']].shift(1) * 100
                    ).round(2)
            
            # 计算涨跌幅和涨跌额（如果缺失）
            if UNIFIED_COLUMNS['close'] in df.columns and UNIFIED_COLUMNS['change_pct'] not in df.columns:
                df[UNIFIED_COLUMNS['change_amount']] = df[UNIFIED_COLUMNS['close']] - df[UNIFIED_COLUMNS['close']].shift(1)
                df[UNIFIED_COLUMNS['change_pct']] = (df[UNIFIED_COLUMNS['change_amount']] / 
                                                   df[UNIFIED_COLUMNS['close']].shift(1) * 100).round(2)
            
            # 按日期排序
            df = df.sort_values(UNIFIED_COLUMNS['date'])
            
            # 检查数据完整性
            if check_data_completeness(df):
                logger.info(f"成功从新浪财经获取{etf_code}日线数据（{len(df)}条记录）")
                return df
            else:
                logger.warning(f"新浪财经返回的{etf_code}数据不完整，但将尝试使用")
                return df
        else:
            logger.warning("新浪财经返回空ETF数据")
            return None
    except Exception as e:
        logger.error(f"新浪财经获取{etf_code}数据失败: {str(e)}", exc_info=True)
        return None

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

def enhance_data_integrity(df):
    """增强数据完整性，从已有数据计算缺失指标"""
    if df is None or df.empty:
        return df
    
    # 确保日期列是datetime类型
    if UNIFIED_COLUMNS['date'] in df.columns:
        df[UNIFIED_COLUMNS['date']] = pd.to_datetime(df[UNIFIED_COLUMNS['date']])
        df = df.sort_values(UNIFIED_COLUMNS['date'])
    
    # 计算振幅（如果缺失）
    if (UNIFIED_COLUMNS['amplitude'] not in df.columns and 
        UNIFIED_COLUMNS['high'] in df.columns and 
        UNIFIED_COLUMNS['low'] in df.columns and
        UNIFIED_COLUMNS['close'] in df.columns):
        
        # 计算振幅：(最高价-最低价)/前一日收盘价
        df[UNIFIED_COLUMNS['amplitude']] = (
            (df[UNIFIED_COLUMNS['high']] - df[UNIFIED_COLUMNS['low']]) / 
            df[UNIFIED_COLUMNS['close']].shift(1) * 100
        ).round(2)
    
    # 计算涨跌幅和涨跌额（如果缺失）
    if (UNIFIED_COLUMNS['change_pct'] not in df.columns and 
        UNIFIED_COLUMNS['close'] in df.columns):
        
        df[UNIFIED_COLUMNS['change_amount']] = df[UNIFIED_COLUMNS['close']] - df[UNIFIED_COLUMNS['close']].shift(1)
        df[UNIFIED_COLUMNS['change_pct']] = (df[UNIFIED_COLUMNS['change_amount']] / 
                                           df[UNIFIED_COLUMNS['close']].shift(1) * 100).round(2)
    
    # 计算成交额（如果缺失但有成交量和均价）
    if (UNIFIED_COLUMNS['amount'] not in df.columns and 
        UNIFIED_COLUMNS['volume'] in df.columns and
        UNIFIED_COLUMNS['open'] in df.columns and
        UNIFIED_COLUMNS['high'] in df.columns and
        UNIFIED_COLUMNS['low'] in df.columns and
        UNIFIED_COLUMNS['close'] in df.columns):
        
        # 近似计算：(开盘+最高+最低+收盘)/4 * 成交量
        avg_price = (df[UNIFIED_COLUMNS['open']] + df[UNIFIED_COLUMNS['high']] + 
                    df[UNIFIED_COLUMNS['low']] + df[UNIFIED_COLUMNS['close']]) / 4
        df[UNIFIED_COLUMNS['amount']] = avg_price * df[UNIFIED_COLUMNS['volume']]
    
    # 计算换手率（如果缺失但有成交额和流通市值）
    if (UNIFIED_COLUMNS['turnover_rate'] not in df.columns and 
        UNIFIED_COLUMNS['amount'] in df.columns):
        
        # 假设流通市值为常数（实际应用中需要获取真实数据）
        if UNIFIED_COLUMNS['close'] in df.columns and UNIFIED_COLUMNS['volume'] in df.columns:
            circulating_market_cap = df[UNIFIED_COLUMNS['close']] * df[UNIFIED_COLUMNS['volume']].mean() * 0.5
            df[UNIFIED_COLUMNS['turnover_rate']] = (df[UNIFIED_COLUMNS['amount']] / 
                                                 circulating_market_cap * 100).round(2)
    
    return df

def check_data_completeness(df, required_columns=None, min_records=5):
    """检查数据完整性并尝试增强"""
    # 首先尝试增强数据
    df = enhance_data_integrity(df)
    
    # 检查必需的列
    if required_columns is None:
        required_columns = list(UNIFIED_COLUMNS.values())
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"数据缺少必要列: {missing_columns}")
        # 尝试从其他列映射
        for col in missing_columns:
            if col == UNIFIED_COLUMNS['volume'] and UNIFIED_COLUMNS['amount'] in df.columns:
                df[UNIFIED_COLUMNS['volume']] = df[UNIFIED_COLUMNS['amount']]
            elif col == UNIFIED_COLUMNS['amplitude'] and all(x in df.columns for x in [UNIFIED_COLUMNS['high'], UNIFIED_COLUMNS['low'], UNIFIED_COLUMNS['close']]):
                df[UNIFIED_COLUMNS['amplitude']] = ((df[UNIFIED_COLUMNS['high']] - df[UNIFIED_COLUMNS['low']]) / 
                                                 df[UNIFIED_COLUMNS['close']].shift(1) * 100).round(2)
    
    # 检查数据量
    if len(df) < min_records:
        logger.warning(f"数据量不足，仅 {len(df)} 条记录（需要至少 {min_records} 条）")
        return False
    
    # 检查关键字段是否为空
    for col in required_columns:
        if col in df.columns:
            try:
                # 确保我们得到的是明确的布尔值
                has_null = bool(df[col].isnull().any())
                if has_null:
                    logger.warning(f"数据中{col}字段包含空值")
                    # 尝试填充空值
                    df[col].fillna(method='ffill', inplace=True)
                    df[col].fillna(method='bfill', inplace=True)
            except Exception as e:
                logger.error(f"检查{col}字段空值时出错: {str(e)}")
                return False
    
    return True
