"""数据源处理模块 - 仅包含数据爬取、缓存和清理相关功能"""

import logging
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
import jqdatasdk as jq
from config import Config
from logger import get_logger
from retrying import retry

# 确保所有数据目录存在（关键修复）
Config.init_directories()

logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)  # 允许DEBUG日志输出

def get_beijing_time():
    """获取北京时间"""
    beijing_tz = pytz.timezone('Asia/Shanghai')
    return datetime.datetime.now(beijing_tz)

def is_trading_day():
    """检查今天是否为交易日"""
    # 获取北京时间
    beijing_time = get_beijing_time()
    
    # 检查是否为周末
    if beijing_time.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
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
    base_path = os.path.join(Config.RAW_DATA_DIR, 'etf_data')
    # 移除多余的 'etf-strategy' 目录
    base_path = base_path.replace('/etf-strategy', '')
    os.makedirs(base_path, exist_ok=True)

    # 添加路径验证
    if not os.path.exists(base_path):
        logger.error(f"缓存目录不存在: {base_path}")
        return None
        
    if data_type == 'daily':
        return os.path.join(base_path, f"{etf_code}_daily.csv")
    else:
        return os.path.join(base_path, f"{etf_code}_intraday_{datetime.datetime.now().strftime('%Y%m%d')}.csv")

def load_from_cache(etf_code, data_type='daily', days=365):
    """从缓存加载ETF数据
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
        days: 保留天数
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        cache_path = get_cache_path(etf_code, data_type)
        if not os.path.exists(cache_path):
            return None
        
        df = pd.read_csv(cache_path)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            # 筛选近期数据
            if data_type == 'daily':
                df = df[df['date'] >= (datetime.datetime.now() - datetime.timedelta(days=days))]
            return df
    except Exception as e:
        logger.error(f"缓存加载错误 {etf_code}: {str(e)}")
    return None

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

def save_to_cache(etf_code, data, data_type='daily'):
    """将ETF数据保存到缓存
    参数:
        etf_code: ETF代码
        DataFrame数据
        data_type: 'daily'或'intraday'
    """
    cache_path = get_cache_path(etf_code, data_type)
    # 使用临时文件确保原子操作
    temp_path = cache_path + '.tmp'
    
    try:
        # 1. 先写入临时文件
        data.to_csv(temp_path, index=False)
        
        # 2. 如果存在原文件，合并数据
        if os.path.exists(cache_path):
            existing_data = pd.read_csv(cache_path)
            combined = pd.concat([existing_data, data]).drop_duplicates(subset=['date'], keep='last')
            combined.to_csv(temp_path, index=False)
        
        # 3. 原子操作：先删除原文件，再重命名
        if os.path.exists(cache_path):
            os.remove(cache_path)
        os.rename(temp_path, cache_path)
        
        # 关键修改：将日志级别从DEBUG提升到INFO    
        logger.info(f"成功保存 {etf_code} 数据到 {cache_path}") 
        return True
    except Exception as e:
        logger.error(f"保存 {etf_code} 数据失败: {str(e)}")
        # 清理临时文件
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return False

def crawl_akshare(etf_code):
    """从AkShare爬取ETF数据（主数据源）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 从代码中提取纯数字代码（移除sh./sz.前缀）
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        # 使用最新确认可用的ETF历史数据接口
        logger.info(f"尝试使用fund_etf_hist_em接口获取{etf_code}数据...")
        df = ak.fund_etf_hist_em(symbol=pure_code)
        
        if df.empty:
            logger.warning(f"AkShare fund_etf_hist_em返回空数据 {etf_code}")
            # 尝试备用接口
            logger.info(f"尝试使用fund_etf_hist_sina接口获取{etf_code}数据...")
            df = ak.fund_etf_hist_sina(symbol=pure_code)
        
        if df.empty:
            logger.error(f"AkShare返回空数据 {etf_code}")
            return None
        
        # 重命名列为标准格式
        column_mapping = {
            '日期': 'date', 'date': 'date',
            '开盘': 'open', 'open': 'open', '开盘价': 'open',
            '最高': 'high', 'high': 'high', '最高价': 'high',
            '最低': 'low', 'low': 'low', '最低价': 'low',
            '收盘': 'close', 'close': 'close', '收盘价': 'close',
            '成交量': 'volume', 'volume': 'volume', '成交 volume': 'volume',
            '成交额': 'amount', 'amount': 'amount'
        }
        
        # 选择存在的列进行重命名
        existing_cols = [col for col in column_mapping.keys() if col in df.columns]
        rename_dict = {col: column_mapping[col] for col in existing_cols}
        df = df.rename(columns=rename_dict)
        
        # 确保必要列存在
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            logger.error(f"AkShare返回的数据缺少必要列: {df.columns.tolist()}")
            return None
        
        # 将日期转换为datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # 按日期排序
        df = df.sort_values('date')
        
        logger.info(f"从AkShare成功获取 {etf_code} 历史数据 ({len(df)}条记录)")
        return df
    except AttributeError as e:
        logger.error(f"AkShare接口错误: {str(e)} - 请确保akshare已升级至最新版")
    except Exception as e:
        logger.error(f"AkShare爬取错误 {etf_code}: {str(e)}")
    return None

def crawl_baostock(etf_code):
    """从Baostock爬取ETF数据（备用数据源1）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            return None
        
        # 为Baostock格式化ETF代码（添加sh.或sz.前缀）
        market = 'sh' if etf_code.startswith('5') else 'sz'
        code = f"{market}.{etf_code}"
        
        # 获取历史数据
        rs = bs.query_history_k_data_plus(
            code, 
            "date,open,high,low,close,volume",
            start_date=(datetime.datetime.now() - datetime.timedelta(days=100)).strftime('%Y-%m-%d'),
            end_date=datetime.datetime.now().strftime('%Y-%m-%d'),
            frequency="d", 
            adjustflag="3"
        )
        
        if rs.error_code != '0':
            logger.error(f"Baostock查询失败: {rs.error_msg}")
            return None
        
        # 转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        if df.empty:
            logger.warning(f"Baostock返回空数据 {etf_code}")
            return None
        
        # 转换数据类型
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # 将日期转换为datetime
        df['date'] = pd.to_datetime(df['date'])
        
        logger.info(f"成功从Baostock爬取 {etf_code} 数据 ({len(df)}条记录)")
        return df
    except Exception as e:
        logger.error(f"Baostock爬取错误 {etf_code}: {str(e)}")
    finally:
        try:
            bs.logout()
        except:
            pass
    return None

def crawl_sina_finance(etf_code):
    """从新浪财经爬取ETF数据（备用数据源2）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 1. 修正交易所前缀处理
        if etf_code.startswith('5'):
            exchange_prefix = 'sh'
        elif etf_code.startswith('1'):
            exchange_prefix = 'sz'
        else:
            exchange_prefix = ''
        
        # 2. 正确构建URL
        full_code = f"{exchange_prefix}{etf_code}"
        sina_url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={full_code}&scale=240&ma=no&datalen=100"
        
        # 3. 添加User-Agent头避免被拒绝
        response = requests.get(sina_url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 获取数据
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        
        # 解析JSON响应
        data = response.json()
        if not data or 'data' not in data:
            return None
        kline_data = data['data']
        
        # 转换为DataFrame
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
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # 将日期转换为datetime
        df['date'] = pd.to_datetime(df['date'])
        
        logger.info(f"成功从新浪财经爬取 {etf_code} 数据 ({len(df)}条记录)")
        return df
    except Exception as e:
        logger.error(f"新浪财经爬取错误 {etf_code}: {str(e)}")
    return None

def login_joinquant():
    """登录聚宽账号"""
    try:
        jq.auth(Config.JOINQUANT_USERNAME, Config.JOINQUANT_PASSWORD)
        logger.info("聚宽登录成功")
        return True
    except Exception as e:
        logger.error(f"聚宽登录失败: {str(e)}")
        return False

def get_etf_data_joinquant(etf_code, data_type='daily'):
    """从聚宽获取ETF数据
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        if not login_joinquant():
            logger.error("聚宽登录失败，无法获取数据")
            return None
        
        # 转换代码格式
        jq_code = etf_code.replace('sh.', '.XSHG').replace('sz.', '.XSHE')
        
        # 设置要获取的数据数量
        count = 100 if data_type == 'daily' else 1
        
        # 获取数据
        df = jq.get_price(
            jq_code,
            end_date=datetime.datetime.now(),
            frequency='daily',
            fields=['open', 'high', 'low', 'close', 'volume'],
            skip_paused=True,
            count=count
        )
        
        if df is None or df.empty:
            logger.error(f"聚宽返回空数据 {etf_code}")
            return None
        
        # 重置索引
        df = df.reset_index()
        df = df.rename(columns={'time': 'date'})
        
        logger.info(f"成功从聚宽爬取 {etf_code} 数据 ({len(df)}条记录)")
        return df
    except Exception as e:
        logger.error(f"聚宽获取数据失败 {etf_code}: {str(e)}")
    return None

def get_etf_data(etf_code, data_type='daily'):
    """从多数据源获取ETF数据
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        DataFrame: ETF数据或None（如果所有数据源都失败）"""
    
    # 首先检查缓存
    cached_data = load_from_cache(etf_code, data_type)
    if cached_data is not None and not cached_data.empty:
        logger.info(f"从缓存加载{etf_code}数据")
        return cached_data
    
    # 尝试主数据源(AkShare) - 仅用于日线数据
    if data_type == 'daily':
        data = crawl_akshare(etf_code)
        if data is not None and not data.empty:
            logger.info(f"【数据获取】成功从AkShare爬取{etf_code}日线数据 ({len(data)}条记录)")
            # 保存数据
            if save_to_cache(etf_code, data, data_type):
                # 添加额外验证
                saved_data = load_from_cache(etf_code, data_type)
                if saved_data is not None and not saved_data.empty:
                    logger.info(f"【数据验证】{etf_code}数据已成功保存并可重新加载 ({len(saved_data)}条记录)")
                else:
                    logger.error(f"【数据验证】{etf_code}数据保存后无法重新加载")
            return data
    
    # 尝试备用数据源1(Baostock)
    data = crawl_baostock(etf_code)
    if data is not None and not data.empty:
        logger.info(f"成功从Baostock爬取{etf_code}数据")
        save_to_cache(etf_code, data, data_type)
        return data
    
    # 尝试备用数据源2(新浪财经)
    data = crawl_sina_finance(etf_code)
    if data is not None and not data.empty:
        logger.info(f"成功从新浪财经爬取{etf_code}数据")
        save_to_cache(etf_code, data, data_type)
        return data
    
    # 尝试备用数据源3(聚宽)
    data = get_etf_data_joinquant(etf_code, data_type)
    if data is not None and not data.empty:
        logger.info(f"成功从聚宽爬取{etf_code}数据")
        save_to_cache(etf_code, data, data_type)
        return data
    
    # 尝试默认数据
    if data_type == 'daily':
        logger.info(f"使用默认数据填充{etf_code}日线数据")
        return generate_default_daily_data(etf_code)
    
    # 所有数据源均失败
    logger.error(f"无法从所有数据源获取{etf_code}数据")
    return None

def generate_default_daily_data(etf_code):
    """生成默认日线数据（当所有数据源都失败时使用）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: 默认日线数据"""
    
    # 创建最近30天的日期范围
    dates = pd.date_range(end=datetime.datetime.now(), periods=30, freq='D')
    
    # 生成随机价格数据
    np.random.seed(42)  # 为了可重复性
    base_price = 1.0 if etf_code.startswith('sh.5') else 2.0
    
    # 生成价格波动
    prices = [base_price]
    for _ in range(29):
        change = np.random.normal(0, 0.01)  # 每日小幅波动
        prices.append(prices[-1] * (1 + change))
    
    # 创建DataFrame
    df = pd.DataFrame({
        'date': dates,
        'open': [p * (1 + np.random.uniform(-0.005, 0.005)) for p in prices],
        'high': [p * (1 + np.random.uniform(0, 0.01)) for p in prices],
        'low': [p * (1 - np.random.uniform(0, 0.01)) for p in prices],
        'close': prices,
        'volume': [np.random.randint(1000000, 10000000) for _ in range(30)]
    })
    
    logger.warning(f"生成了{etf_code}的默认日线数据（30天随机数据）")
    return df

def get_all_etf_list():
    """从多数据源获取所有ETF列表
    返回:DataFrame: ETF列表，包含代码和名称"""
    
    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
    def fetch_akshare_primary():
        # 添加1-3秒随机延迟
        time.sleep(random.uniform(1, 3))
        return ak.fund_etf_spot_em()
    
    # 尝试AkShare（主数据源）
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        
        # 使用重试机制获取数据
        df = fetch_akshare_primary()
        
        if df.empty:
            logger.warning("AkShare返回空ETF列表")
            raise Exception("数据为空")
        
        # 动态匹配列名 - 关键修复
        code_col = next((col for col in df.columns if '代码' in col or 'symbol' in col.lower()), None)
        name_col = next((col for col in df.columns if '名称' in col or 'name' in col.lower()), None)
        
        if code_col is None or name_col is None:
            logger.error(f"AkShare返回数据缺少必要列。可用列: {df.columns.tolist()}")
            raise Exception("数据格式不匹配")
        
        etf_list = df[[code_col, name_col]].copy()
        etf_list.columns = ['code', 'name']
        etf_list['code'] = etf_list['code'].apply(
            lambda x: f"sh.{x}" if str(x).startswith('5') else f"sz.{x}"
        )
        logger.info(f"从AkShare成功获取 {len(etf_list)} 只ETF")
        return etf_list
    except Exception as e:
        logger.error(f"AkShare获取ETF列表失败: {str(e)}")
    
    # 尝试AkShare备用接口
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def fetch_akshare_backup():
        # 添加基础延迟
        time.sleep(1)
        return ak.fund_etf_hist_sina(symbol="etf")
    
    try:
        logger.info("尝试从AkShare备用接口获取ETF列表...")
        df = fetch_akshare_backup()
        if not df.empty:
            # 动态匹配列名 - 关键修复
            code_col = next((col for col in df.columns if '代码' in col or 'symbol' in col.lower()), None)
            name_col = next((col for col in df.columns if '名称' in col or 'name' in col.lower() or '简称' in col), None)
            
            if code_col is None or name_col is None:
                logger.error(f"AkShare备用接口返回数据缺少必要列。可用列: {df.columns.tolist()}")
            else:
                # 提取唯一ETF代码
                etf_codes = df[code_col].unique()
                etf_names = {row[code_col]: row[name_col] for _, row in df.iterrows() if code_col in row and name_col in row}
                
                etf_list = pd.DataFrame({
                    'code': [f"sh.{c}" if str(c).startswith('5') else f"sz.{c}" for c in etf_codes],
                    'name': [etf_names.get(c, c) for c in etf_codes]
                })
                
                logger.info(f"从AkShare备用接口成功获取 {len(etf_list)} 只ETF")
                return etf_list
    except Exception as e:
        logger.error(f"AkShare备用接口获取ETF列表失败: {str(e)}") 

    # 尝试Baostock
    try:
        logger.info("尝试从Baostock获取ETF列表...")
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 查询所有ETF基金
        etf_list = []
        
        # 上海ETF (以51开头)
        rs = bs.query_stock_basic(code="sh51")
        while (rs.error_code == '0') & rs.next():
            etf_list.append(rs.get_row_data())
        
        # 深圳ETF (以159开头)
        rs = bs.query_stock_basic(code="sz159")
        while (rs.error_code == '0') & rs.next():
            etf_list.append(rs.get_row_data())
        
        if etf_list:
            df = pd.DataFrame(etf_list, columns=rs.fields)
            # 过滤出ETF
            df = df[df['code_name'].str.contains('ETF', case=False, na=False)]
            df = df[['code', 'code_name']]
            df.columns = ['code', 'name']
            
            # 移除交易所前缀，然后重新添加标准前缀
            df['code'] = df['code'].str.replace('sh.', '').str.replace('sz.', '')
            df['code'] = df['code'].apply(
                lambda x: f"sh.{x}" if x.startswith('51') else f"sz.{x}"
            )
            
            logger.info(f"从Baostock成功获取 {len(df)} 只ETF")
            return df
        else:
            logger.warning("Baostock返回空ETF列表")
    except Exception as e:
        logger.error(f"Baostock获取ETF列表失败: {str(e)}")
    finally:
        try:
            bs.logout()
        except:
            pass
    
    # 尝试新浪财经
    try:
        logger.info("尝试从新浪财经获取ETF列表...")
        sina_urls = [
            "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=etf_hk&symbol=&_s_r_a=page",
            "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=etf_sz&symbol=&_s_r_a=page",
            "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=etf_sh&symbol=&_s_r_a=page"
        ]
        
        all_etfs = []
        for url in sina_urls:
            try:
                response = requests.get(url, timeout=15, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                })
                response.raise_for_status()
                data = response.json()
                
                if data:
                    for item in data['data']:
                        code = item.get('symbol', '')
                        name = item.get('name', '')
                        if code and name:
                            all_etfs.append({'code': code, 'name': name})
            except Exception as e:
                logger.debug(f"访问新浪财经URL {url} 失败: {str(e)}")
                continue
        
        if all_etfs:
            df = pd.DataFrame(all_etfs)
            # 标准化代码格式
            df['code'] = df['code'].apply(
                lambda x: f"sh.{x}" if x.startswith('5') else f"sz.{x}" if x.startswith('1') else x
            )
            logger.info(f"从新浪财经成功获取 {len(df)} 只ETF")
            return df
        else:
            logger.warning("新浪财经返回空ETF列表")
    except Exception as e:
        logger.error(f"新浪财经获取ETF列表失败: {str(e)}")
    
    # 尝试聚宽
    try:
        logger.info("尝试从聚宽获取ETF列表...")
        if login_joinquant():
            df = jq.get_all_securities(types=['etf'])
            if not df.empty:
                df = df.reset_index()
                df['code'] = df['code'].apply(lambda x: x.replace('.XSHE', '.sz').replace('.XSHG', '.sh'))
                df = df[['code', 'display_name']]
                df.columns = ['code', 'name']
                logger.info(f"从聚宽成功获取 {len(df)} 只ETF")
                return df
            else:
                logger.warning("聚宽返回空ETF列表")
        else:
            logger.warning("聚宽登录失败，跳过数据获取")
    except Exception as e:
        logger.error(f"聚宽获取ETF列表失败: {str(e)}")
    
    # 如果所有数据源都失败，返回一个默认列表
    logger.error("所有数据源均无法获取ETF列表，使用默认ETF列表")
    default_etfs = [
        {'code': 'sh.510050', 'name': '上证50ETF'},
        {'code': 'sh.510300', 'name': '沪深300ETF'},
        {'code': 'sh.510500', 'name': '中证500ETF'},
        {'code': 'sz.159915', 'name': '创业板ETF'},
        {'code': 'sh.512888', 'name': '消费ETF'},
        {'code': 'sh.512480', 'name': '半导体ETF'},
        {'code': 'sh.512660', 'name': '军工ETF'},
        {'code': 'sh.512980', 'name': '通信ETF'},
        {'code': 'sz.159825', 'name': '新能源ETF'},
        {'code': 'sz.159995', 'name': '芯片ETF'}
    ]
    return pd.DataFrame(default_etfs)

def read_new_stock_pushed_flag(target_date=None):
    """读取新股信息推送标记文件"""
    flag_file = Config.NEW_STOCK_PUSHED_FLAG
    if not os.path.exists(flag_file):
        return (None, False) if target_date is not None else None
    
    try:
        # 读取文件内容
        with open(flag_file, 'r') as f:
            content = f.read().strip()
        
        # 尝试解析日期
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                file_date = datetime.datetime.strptime(content, fmt).date()
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"无法解析日期: {content}")
        
        # 根据是否提供目标日期返回不同结果
        if target_date is None:
            return file_date
        else:
            # 确保target_date是日期类型
            if isinstance(target_date, datetime.datetime):
                target_date = target_date.date()
            return file_date, (file_date == target_date)
    except Exception as e:
        logger.error(f"读取推送标记文件错误: {str(e)}")
        return (None, False) if target_date is not None else None

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    with open(Config.NEW_STOCK_PUSHED_FLAG, 'w') as f:
        f.write(get_beijing_time().strftime('%Y-%m-%d'))

def read_listing_pushed_flag(target_date=None):
    """读取新上市交易股票信息推送标记文件"""
    flag_file = Config.LISTING_PUSHED_FLAG
    if not os.path.exists(flag_file):
        return (None, False) if target_date is not None else None
    
    try:
        # 读取文件内容
        with open(flag_file, 'r') as f:
            content = f.read().strip()
        
        # 尝试解析日期
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                file_date = datetime.datetime.strptime(content, fmt).date()
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"无法解析日期: {content}")
        
        # 根据是否提供目标日期返回不同结果
        if target_date is None:
            return file_date
        else:
            # 确保target_date是日期类型
            if isinstance(target_date, datetime.datetime):
                target_date = target_date.date()
            return file_date, (file_date == target_date)
    except Exception as e:
        logger.error(f"读取推送标记文件错误: {str(e)}")
        return (None, False) if target_date is not None else None

def mark_listing_info_pushed():
    """标记新上市交易股票信息已推送"""
    with open(Config.LISTING_PUSHED_FLAG, 'w') as f:
        f.write(get_beijing_time().strftime('%Y-%m-%d'))

def get_new_stock_subscriptions():
    """获取当天新股数据"""
    try:
        # 尝试AkShare（主数据源）
        today = get_beijing_time().strftime('%Y-%m-%d')
        df = ak.stock_xgsglb_em()
        if not df.empty and '申购日期' in df.columns:
            df = df[df['申购日期'] == today]
            if not df.empty:
                return df[['股票代码', '股票简称', '发行价格', '申购上限', '申购日期']]
    except Exception as e:
        logger.error(f"AkShare获取新股信息失败: {str(e)}")
    
    # 尝试Tushare（备用数据源1）
    try:
        ts_token = os.getenv('TUSHARE_TOKEN')
        if ts_token:
            import tushare as ts
            ts.set_token(ts_token)
            pro = ts.pro_api()
            
            # 获取当日新股申购信息
            df = pro.new_share(start_date=today, end_date=today)
            if not df.empty:
                return df[['ts_code', 'name', 'price', 'max_supply', 'ipo_date']]
    except Exception as e:
        logger.error(f"Tushare获取新股信息失败: {str(e)}")
    
    # 尝试新浪财经（备用数据源2）
    try:
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        new_stocks = []
        for item in data['data']:
            if item.get('ipo_date') == today:
                new_stocks.append({
                    '股票代码': item.get('symbol'),
                    '股票简称': item.get('name'),
                    '发行价格': item.get('price'),
                    '申购上限': item.get('max_purchase'),
                    '申购日期': item.get('ipo_date')
                })
        
        if new_stocks:
            return pd.DataFrame(new_stocks)
    except Exception as e:
        logger.error(f"新浪财经获取新股信息失败: {str(e)}")
    
    return pd.DataFrame()

def get_new_stock_listings():
    """获取当天新上市交易的新股数据"""
    try:
        # 尝试AkShare（主数据源）
        today = get_beijing_time().strftime('%Y-%m-%d')
        df = ak.stock_zh_a_new()
        if not df.empty and 'listing_date' in df.columns:
            df = df[df['listing_date'] == today]
            if not df.empty:
                return df[['code', 'name', 'issue_price', 'max_purchase', 'listing_date']]
    except Exception as e:
        logger.error(f"AkShare获取新上市交易股票信息失败: {str(e)}")
    
    # 尝试Baostock（备用数据源1）
    try:
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 获取新股列表
        rs = bs.query_stock_new()
        if rs.error_code != '0':
            logger.error(f"Baostock查询失败: {rs.error_msg}")
            return pd.DataFrame()
        
        # 转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if data_list:
            df = pd.DataFrame(data_list, columns=rs.fields)
            return df[['code', 'code_name', 'price', 'max_purchase', 'ipoDate']].rename(columns={
                'code': 'code',
                'code_name': 'name',
                'price': 'issue_price',
                'max_purchase': 'max_purchase',
                'ipoDate': 'listing_date'
            })
        else:
            logger.warning("Baostock返回空数据，尝试下一个数据源...")
    except Exception as e:
        logger.error(f"Baostock获取新上市交易股票信息失败: {str(e)}")
    
    # 尝试新浪财经（备用数据源2）
    try:
        logger.info("尝试从新浪财经获取新上市交易股票信息...")
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        new_listings = []
        for item in data['data']:
            if item.get('listing_date') == today:
                new_listings.append({
                    'code': item.get('symbol'),
                    'name': item.get('name'),
                    'issue_price': item.get('price'),
                    'max_purchase': item.get('max_purchase'),
                    'listing_date': item.get('listing_date')
                })
        
        if new_listings:
            return pd.DataFrame(new_listings)
    except Exception as e:
        logger.error(f"新浪财经获取新上市交易股票信息失败: {str(e)}")
    
    return pd.DataFrame()

def cleanup_directory(directory, days_to_keep=None):
    """清理指定目录中的旧文件
    参数:
        directory: 目录路径
        days_to_keep: 保留天数，None表示不清理
    """
    if days_to_keep is None:
        logger.info(f"跳过目录清理: {directory} (永久保存)")
        return
    
    now = datetime.datetime.now()
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        # 获取文件修改时间
        file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
        # 计算文件年龄
        file_age = now - file_time
        # 如果文件年龄大于指定天数，删除文件
        if file_age.days > days_to_keep:
            try:
                os.remove(filepath)
                logger.info(f"已删除旧文件: {filepath}")
            except Exception as e:
                logger.error(f"删除文件失败 {filepath}: {str(e)}")

def cron_crawl_daily():
    """日线数据爬取任务"""
    logger.info("日线数据爬取任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过爬取")
        return {"status": "skipped", "message": "Not trading day"}
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        logger.error("未获取到ETF列表，跳过爬取")
        return {"status": "skipped", "message": "No ETF list available"}

    logger.info(f"【任务准备】开始爬取 {len(etf_list)} 只ETF的日线数据")

    # 加载爬取状态
    crawl_status = get_crawl_status()
    beijing_now = get_beijing_time()
    date_str = beijing_now.strftime('%Y%m%d')
    
    # 统计
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    # 爬取每只ETF的日线数据
    for _, etf in etf_list.iterrows():
        etf_code = etf['code']
        
        # 检查是否已成功爬取
        if etf_code in crawl_status and crawl_status[etf_code].get('status') == 'success':
            # 检查是否为今天的数据
            last_success = crawl_status[etf_code].get('timestamp', '')
            if last_success.startswith(date_str):
                logger.info(f"ETF {etf_code} 已成功爬取，跳过")
                skipped_count += 1
                continue
        
        try:
            # 标记开始
            update_crawl_status(etf_code, 'in_progress')
            logger.info(f"【任务开始】开始爬取 {etf_code}")
            
            data = get_etf_data(etf_code, 'daily')
            if data is None or data.empty:
                logger.error(f"爬取{etf_code}日线数据失败")
                success = False
                # 标记失败
                update_crawl_status(etf_code, 'failed', 'Empty data')
                failed_count += 1
            else:
                # 标记成功
                update_crawl_status(etf_code, 'success')
                success_count += 1
                logger.info(f"成功爬取 {etf_code} 日线数据，共 {len(data)} 条记录")
            
            # 避免请求过快
            time.sleep(1)
        except Exception as e:
            logger.error(f"爬取{etf_code}日线数据异常: {str(e)}")
            # 标记异常
            update_crawl_status(etf_code, 'failed', str(e))
            failed_count += 1
    
    # 清理状态文件（如果全部成功）
    if success_count + skipped_count == len(etf_list):
        status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
        if os.path.exists(status_file):
            try:
                os.remove(status_file)
                logger.info("所有ETF爬取成功，已清理状态文件")
            except Exception as e:
                logger.warning(f"清理状态文件失败: {str(e)}")
    
    # === 关键修改：添加Git推送逻辑 ===
    try:
        if success_count > 0:  # 只有成功爬取数据才推送
            logger.info("开始Git推送流程...")
            git_push()  # 调用推送函数
            logger.info("Git推送成功完成")
    except Exception as e:
        logger.critical(f"Git推送失败，终止任务执行: {str(e)}")
        # 关键修改：出错立即终止，不再继续
        return {
            "status": "error",
            "message": f"数据爬取完成但Git推送失败: {str(e)}",
            "success_count": success_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count
        }
    
    # 在所有数据保存完成后提交到 Git
    try:
        # 添加所有更改
        subprocess.run(["git", "add", "."], check=True)
        
        # 提交更改
        commit_msg = f"Auto save ETF data {get_beijing_time().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        
        # 推送到远程仓库
        subprocess.run(["git", "push", "origin", "main"], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Git push failed: {e.output.decode()}")
        raise
    
    return {"status": "success", "message": f"成功: {success_count}, 失败: {failed_count}"}

def cron_crawl_intraday():
    """盘中数据爬取任务"""
    logger.info("盘中数据爬取任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过爬取")
        return {"status": "skipped", "message": "Not trading day"}
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        logger.error("未获取到ETF列表，跳过爬取")
        return {"status": "skipped", "message": "No ETF list available"}
    
    # 爬取每只ETF的盘中数据
    success = True
    for _, etf in etf_list.iterrows():
        data = get_etf_data(etf['code'], 'intraday')
        if data is None or data.empty:
            logger.error(f"爬取{etf['code']}盘中数据失败")
            success = False
        time.sleep(1) # 避免请求过快
    
    return {"status": "success" if success else "error"}

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

def resume_crawl():
    """断点续爬任务"""
    logger.info("断点续爬任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过爬取")
        return {"status": "skipped", "message": "Not trading day"}
    
    # 检查状态文件
    status_file = os.path.join(Config.RAW_DATA_DIR, 'crawl_status.json')
    if not os.path.exists(status_file):
        logger.info("无待续爬任务，启动全新爬取")
        return cron_crawl_daily()
    
    # 加载状态
    try:
        with open(status_file, 'r') as f:
            crawl_status = json.load(f)
    except Exception as e:
        logger.error(f"加载爬取状态失败: {str(e)}")
        return {"status": "error", "message": "Failed to load status"}
    
    # 检查是否有未完成任务
    pending_etfs = [code for code, status in crawl_status.items() 
                   if status.get('status') in ['in_progress', 'failed']]
    
    if not pending_etfs:
        logger.info("无待续爬ETF，任务已完成")
        return {"status": "success", "message": "No pending ETFs"}
    
    logger.info(f"发现 {len(pending_etfs)} 个待续爬ETF")
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        logger.error("未获取到ETF列表，跳过爬取")
        return {"status": "skipped", "message": "No ETF list available"}
    
    # 筛选待爬ETF
    pending_etf_list = etf_list[etf_list['code'].isin(pending_etfs)]
    
    # 继续爬取
    success_count = 0
    failed_count = 0
    
    for _, etf in pending_etf_list.iterrows():
        etf_code = etf['code']
        
        try:
            # 标记开始
            update_crawl_status(etf_code, 'in_progress')
            
            # 爬取数据
            data = get_etf_data(etf_code, 'daily')
            
            # 检查结果
            if data is not None and not data.empty:
                update_crawl_status(etf_code, 'success')
                success_count += 1
                logger.info(f"成功续爬 {etf_code}，共 {len(data)} 条记录")
            else:
                update_crawl_status(etf_code, 'failed', 'Empty data')
                failed_count += 1
                logger.warning(f"续爬 {etf_code} 失败：返回空数据")
        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"续爬 {etf_code} 时出错: {error_msg}")
            update_crawl_status(etf_code, 'failed', error_msg)
            failed_count += 1
        
        # 避免请求过快
        time.sleep(1)
    
    # 检查是否全部完成
    remaining = [code for code, status in get_crawl_status().items() 
                if status.get('status') in ['in_progress', 'failed']]
    
    if not remaining:
        try:
            os.remove(status_file)
            logger.info("所有ETF爬取成功，已清理状态文件")
        except Exception as e:
            logger.warning(f"清理状态文件失败: {str(e)}")
    
    return {
        "status": "partial_success" if failed_count > 0 else "success",
        "total_pending": len(pending_etfs),
        "success": success_count,
        "failed": failed_count
    }

def git_push():
    """推送更改到远程仓库，失败时立即终止"""
    try:
        logger.info("【Git操作】开始同步远程仓库")
        
        # 1. 拉取远程最新代码（解决 rejected 问题）
        logger.info("【Git操作】拉取远程最新代码")
        pull_result = subprocess.run(
            ["git", "pull", "origin", "main"], 
            capture_output=True, 
            text=True,
            check=False  # 不立即抛出异常，先检查输出
        )
        
        # 检查拉取结果
        if pull_result.returncode != 0:
            logger.warning(f"【Git操作】拉取远程代码时警告: {pull_result.stderr.strip()}")
            # 即使有警告也继续，因为可能只是没有新提交
        
        # 2. 添加数据目录更改
        logger.info("【Git操作】添加数据目录更改")
        subprocess.run(
            ["git", "add", "data/"], 
            check=True,
            capture_output=True,
            text=True
        )
        
        # 3. 检查是否有更改需要提交
        status_result = subprocess.run(
            ["git", "status", "--porcelain", "data/"], 
            capture_output=True, 
            text=True
        )
        
        if not status_result.stdout.strip():
            logger.info("【Git操作】无数据更改，跳过提交")
            return True
            
        # 4. 提交更改
        commit_msg = f"自动保存ETF数据 {get_beijing_time().strftime('%Y-%m-%d %H:%M')}"
        logger.info(f"【Git操作】提交更改: {commit_msg}")
        subprocess.run(
            ["git", "commit", "-m", commit_msg], 
            check=True,
            capture_output=True,
            text=True
        )
        
        # 5. 推送到远程仓库
        logger.info("【Git操作】推送更改到远程仓库")
        push_result = subprocess.run(
            ["git", "push", "origin", "main"], 
            capture_output=True, 
            text=True,
            check=True
        )
        
        logger.info("【Git操作】成功推送到远程仓库")
        return True
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Git操作失败: {e}\n输出: {e.stdout}\n错误: {e.stderr}"
        logger.error(error_msg)
        # 关键修改：出错立即终止，不再继续
        raise RuntimeError(error_msg) from None
    except Exception as e:
        error_msg = f"Git操作异常: {str(e)}"
        logger.error(error_msg)
        # 关键修改：出错立即终止，不再继续
        raise RuntimeError(error_msg) from None
