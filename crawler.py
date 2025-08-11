"""
鱼盆ETF投资量化模型 - 数据爬取模块
说明:
  本文件负责从多个数据源获取ETF数据
  所有文件放在根目录，简化导入关系
  主数据源：AkShare
  备用数据源：Baostock、新浪财经、Tushare
"""

import os
import time
import pandas as pd
import numpy as np
import akshare as ak
import baostock as bs
import requests
import tushare as ts
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from config import Config
from logger import get_logger
from time_utils import convert_to_beijing_time

logger = get_logger(__name__)

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
    os.makedirs(base_path, exist_ok=True)
    
    if data_type == 'daily':
        return os.path.join(base_path, f"{etf_code}_daily.csv")
    else:
        return os.path.join(base_path, f"{etf_code}_intraday_{datetime.now().strftime('%Y%m%d')}.csv")

def load_from_cache(etf_code, data_type='daily', days=30):
    """
    从缓存加载ETF数据（如果可用）
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
        days: 要加载的天数
    返回:
        DataFrame: 缓存数据或None（如果不可用）
    """
    cache_path = get_cache_path(etf_code, data_type)
    try:
        if os.path.exists(cache_path):
            df = pd.read_csv(cache_path)
            df['date'] = pd.to_datetime(df['date'])
            # 筛选近期数据
            if data_type == 'daily':
                df = df[df['date'] >= (datetime.now() - timedelta(days=days))]
            return df
    except Exception as e:
        logger.error(f"缓存加载错误 {etf_code}: {str(e)}")
    return None

def save_to_cache(etf_code, data, data_type='daily'):
    """
    将ETF数据保存到缓存
    参数:
        etf_code: ETF代码
         DataFrame数据
        data_type: 'daily'或'intraday'
    """
    cache_path = get_cache_path(etf_code, data_type)
    
    if os.path.exists(cache_path):
        # 追加到现有文件
        existing_data = pd.read_csv(cache_path)
        combined = pd.concat([existing_data, data]).drop_duplicates(subset=['date'], keep='last')
        combined.to_csv(cache_path, index=False)
    else:
        # 创建新文件
        data.to_csv(cache_path, index=False)

def crawl_akshare(etf_code):
    """
    从AkShare爬取ETF数据（主数据源）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）
    """
    try:
        # 从AkShare获取日线数据
        df = ak.fund_etf_hist_sina(symbol=etf_code, period="daily", start_date="", end_date="")
        if df.empty:
            logger.error(f"AkShare返回空数据 {etf_code}")
            return None
        
        # 重命名列为标准格式
        df = df.rename(columns={
            'date': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        # 将日期转换为datetime
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        logger.error(f"AkShare爬取错误 {etf_code}: {str(e)}")
        return None

def crawl_baostock(etf_code):
    """
    从Baostock爬取ETF数据（备用数据源1）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）
    """
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
            code, "date,open,high,low,close,volume",
            start_date=(datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3"
        )
        
        if rs.error_code != '0':
            logger.error(f"Baostock查询失败: {rs.error_msg}")
            bs.logout()
            return None
        
        # 转换为DataFrame
        df = rs.get_data()
        bs.logout()  # 使用后登出
        
        if df.empty:
            return None
        
        # 转换数据类型
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        return df
    except Exception as e:
        logger.error(f"Baostock爬取错误 {etf_code}: {str(e)}")
        try:
            bs.logout()
        except:
            pass
        return None

def crawl_sina_finance(etf_code):
    """
    从新浪财经爬取ETF数据（备用数据源2）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）
    """
    try:
        # 构建新浪财经API URL
        sina_url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol=sh{etf_code if etf_code.startswith('5') else etf_code}&scale=240&ma=no&datalen=100"
        if etf_code.startswith('1') or etf_code.startswith('5'):
            sina_url = sina_url.replace('sh', 'sh')
        else:
            sina_url = sina_url.replace('sh', 'sz')
        
        # 获取数据
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        
        # 解析JSON响应
        data = response.json()
        if not data:
            return None
        
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
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        return df
    except Exception as e:
        logger.error(f"新浪财经爬取错误 {etf_code}: {str(e)}")
        return None

def crawl_tushare(etf_code):
    """
    从Tushare爬取ETF数据（备用数据源3）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: ETF数据或None（如果失败）
    """
    try:
        # 设置Tushare token
        ts.set_token(Config.TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        # 获取ETF基础信息
        basic_info = pro.fund_basic(market='E', status='L')
        etf_info = basic_info[basic_info['ts_code'].str.startswith(etf_code)]
        
        if etf_info.empty:
            logger.warning(f"Tushare未找到{etf_code}的基础信息")
            return None
        
        # 获取ETF日线数据
        df = pro.fund_daily(ts_code=etf_info['ts_code'].values[0], 
                           start_date=(datetime.now() - timedelta(days=100)).strftime('%Y%m%d'),
                           end_date=datetime.now().strftime('%Y%m%d'))
        
        if df.empty:
            logger.warning(f"Tushare未找到{etf_code}的日线数据")
            return None
        
        # 重命名列为标准格式
        df = df.rename(columns={
            'trade_date': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'vol': 'volume'
        })
        
        # 转换日期格式
        df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')
        
        # 按日期排序
        df = df.sort_values('date', ascending=True).reset_index(drop=True)
        
        return df
    except Exception as e:
        logger.error(f"Tushare爬取错误 {etf_code}: {str(e)}")
        return None

def get_etf_data(etf_code, data_type='daily'):
    """
    从多数据源获取ETF数据（带自动回退机制）
    参数:
        etf_code: ETF代码
        data_type: 'daily'或'intraday'
    返回:
        DataFrame: ETF数据或None（如果所有数据源都失败）
    """
    # 首先检查缓存
    cached_data = load_from_cache(etf_code, data_type)
    if cached_data is not None and not cached_data.empty:
        logger.info(f"从缓存加载{etf_code}数据")
        return cached_data
    
    # 尝试主数据源(AkShare)
    data = crawl_akshare(etf_code)
    if data is not None and not data.empty:
        logger.info(f"成功从AkShare爬取{etf_code}数据")
        save_to_cache(etf_code, data, data_type)
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
    
    # 尝试备用数据源3(Tushare)
    data = crawl_tushare(etf_code)
    if data is not None and not data.empty:
        logger.info(f"成功从Tushare爬取{etf_code}数据")
        save_to_cache(etf_code, data, data_type)
        return data
    
    # 所有数据源均失败
    logger.error(f"无法从所有数据源获取{etf_code}数据")
    return None

def get_all_etf_list():
    """
    从多数据源获取所有ETF列表
    返回:
        DataFrame: ETF列表，包含代码和名称
    """
    try:
        # 从AkShare获取ETF列表（主数据源）
        logger.info("尝试从AkShare获取ETF列表...")
        df = ak.fund_etf_category(symbol="ETF基金")
        if not df.empty:
            # 筛选仅保留ETF
            etf_list = df[df['基金类型'] == 'ETF'].copy()
            etf_list = etf_list[['基金代码', '基金简称']]
            etf_list.columns = ['code', 'name']
            logger.info(f"从AkShare成功获取 {len(etf_list)} 只ETF")
            return etf_list
    except Exception as e:
        logger.error(f"AkShare获取ETF列表失败: {str(e)}")
    
    # 尝试Baostock（备用数据源1）
    try:
        logger.info("尝试从Baostock获取ETF列表...")
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            raise Exception("Baostock登录失败")
        
        # 查询ETF基金
        rs = bs.query_stock_basic(code="sh51")
        etf_list = []
        while (rs.error_code == '0') & rs.next():
            etf_list.append(rs.get_row_data())
        
        rs = bs.query_stock_basic(code="sz159")
        while (rs.error_code == '0') & rs.next():
            etf_list.append(rs.get_row_data())
        
        if etf_list:
            df = pd.DataFrame(etf_list, columns=rs.fields)
            # 筛选ETF
            df = df[df['code_name'].str.contains('ETF')]
            df = df[['code', 'code_name']]
            df.columns = ['code', 'name']
            # 移除交易所前缀
            df['code'] = df['code'].str.replace('sh.', '').str.replace('sz.', '')
            
            logger.info(f"从Baostock成功获取 {len(df)} 只ETF")
            return df
        
        logger.warning("Baostock返回空数据，尝试下一个数据源...")
    except Exception as e:
        logger.error(f"Baostock获取ETF列表失败: {str(e)}")
    finally:
        try:
            bs.logout()
        except:
            pass
    
    # 尝试新浪财经（备用数据源2）
    try:
        logger.info("尝试从新浪财经获取ETF列表...")
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=etf_hk&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data:
            etf_list = pd.DataFrame(data)
            etf_list = etf_list[['symbol', 'name']]
            etf_list.columns = ['code', 'name']
            logger.info(f"从新浪财经成功获取 {len(etf_list)} 只ETF")
            return etf_list
    except Exception as e:
        logger.error(f"新浪财经获取ETF列表失败: {str(e)}")
    
    # 如果所有数据源都失败，返回一个默认列表
    logger.error("所有数据源均无法获取ETF列表，使用默认ETF列表")
    return pd.DataFrame({
        'code': ['510050', '510300', '510500', '159915', '512888', '512480', '512660', '512980', '159825', '159995'],
        'name': ['上证50ETF', '沪深300ETF', '中证500ETF', '创业板ETF', '消费ETF', '半导体ETF', '军工ETF', '通信ETF', '新能源ETF', '医疗ETF']
    })

def calculate_component_weights(etf_code):
    """
    计算ETF成分股权重
    参数:
        etf_code: ETF代码
    返回:
        dict: 成分股权重
    """
    try:
        # 从AkShare获取成分股数据
        component_data = ak.fund_etf_component_sina(symbol=etf_code)
        
        if component_data.empty:
            logger.warning(f"{etf_code}成分股数据为空")
            return {}
        
        # 计算权重
        total_market_cap = component_data['总市值'].sum()
        weights = {}
        for _, row in component_data.iterrows():
            weights[row['股票代码']] = row['总市值'] / total_market_cap
        
        return weights
    except Exception as e:
        logger.error(f"计算{etf_code}成分股权重失败: {str(e)}")
        
        # 尝试使用Tushare
        try:
            ts.set_token(Config.TUSHARE_TOKEN)
            pro = ts.pro_api()
            
            # 获取ETF持仓
            df = pro.fund_portfolio(ts_code=f"{etf_code}.SH", start_date=datetime.now().strftime('%Y%m%d'))
            if df.empty:
                df = pro.fund_portfolio(ts_code=f"{etf_code}.SZ", start_date=datetime.now().strftime('%Y%m%d'))
            
            if not df.empty:
                weights = {}
                for _, row in df.iterrows():
                    weights[row['stock_code']] = row['mkv_ratio'] / 100
                return weights
        except Exception as te:
            logger.error(f"使用Tushare计算{etf_code}成分股权重失败: {str(te)}")
        
        return {}

def estimate_etf_nav(etf_code, component_prices=None):
    """
    估算ETF净值
    参数:
        etf_code: ETF代码
        component_prices: 可选的成分股价格
    返回:
        float: 估算的净值
    """
    try:
        # 获取成分股权重
        weights = calculate_component_weights(etf_code)
        if not weights:
            return None
        
        # 获取成分股价格
        if component_prices is None:
            component_prices = {}
            for stock_code in weights.keys():
                stock_data = get_etf_data(stock_code, 'intraday')
                if stock_data is not None and not stock_data.empty:
                    component_prices[stock_code] = stock_data['close'].iloc[-1]
        
        # 计算净值
        nav = 0
        for stock_code, weight in weights.items():
            if stock_code in component_prices:
                nav += component_prices[stock_code] * weight
        
        # 考虑管理费等因素
        nav *= 0.995  # 假设年化管理费0.5%
        
        return nav
    except Exception as e:
        logger.error(f"估算{etf_code}净值失败: {str(e)}")
        return None

def calculate_premium_rate(etf_code, etf_price=None):
    """
    计算ETF溢价率
    参数:
        etf_code: ETF代码
        etf_price: 可选的ETF价格
    返回:
        float: 溢价率（百分比）
    """
    # 获取ETF价格（如未提供）
    if etf_price is None:
        data = get_etf_data(etf_code, 'intraday')
        if data is None or data.empty:
            return 0.0
        etf_price = data['close'].iloc[-1]
    
    # 估算净值
    nav = estimate_etf_nav(etf_code)
    if nav is None or nav == 0:
        return 0.0
    
    # 计算溢价率
    premium_rate = (etf_price - nav) / nav * 100
    return premium_rate

def get_new_stock_subscriptions():
    """
    获取当天可申购的新股（IPO）
    使用多数据源回退机制：
    1. 主数据源：AkShare
    2. 备用数据源1：Baostock
    3. 备用数据源2：新浪财经
    
    返回:
        DataFrame: 当天可申购的新股信息
    """
    # 尝试AkShare（主数据源）
    try:
        logger.info("尝试从AkShare获取新股申购信息...")
        # AkShare没有直接获取新股申购的API，使用替代方案
        # 使用AkShare获取IPO信息
        df = ak.stock_ipo_cninfo()
        
        if not df.empty:
            # 筛选今天可申购的
            today = datetime.now().strftime('%Y-%m-%d')
            df = df[df['网上申购日'] == today]
            
            if not df.empty:
                # 重命名列为标准格式
                df = df.rename(columns={
                    '证券代码': 'code',
                    '证券简称': 'name',
                    '发行价格': 'issue_price',
                    '申购上限': 'max_purchase',
                    '网上申购日': 'issue_date'
                })
                # 转换数据类型
                df['issue_price'] = df['issue_price'].astype(float)
                df['max_purchase'] = df['max_purchase'].astype(int)
                
                logger.info(f"从AkShare成功获取 {len(df)} 条新股信息")
                return df[['code', 'name', 'issue_price', 'max_purchase', 'issue_date']]
        
        logger.warning("AkShare返回空数据，尝试备用数据源...")
    except Exception as e:
        logger.error(f"AkShare获取新股申购信息失败: {str(e)}")
    
    # 尝试Baostock（备用数据源1）
    try:
        logger.info("尝试从Baostock获取新股申购信息...")
        # Baostock没有直接获取新股申购的API，使用替代方案
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            return pd.DataFrame(columns=['code', 'name', 'issue_price', 'max_purchase', 'issue_date'])
        
        # 获取新股信息（通过查询所有股票，然后筛选）
        rs = bs.query_stock_basic()
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 筛选新股
        today = datetime.now().strftime('%Y-%m-%d')
        df = df[df['ipoDate'] == today]
        
        if not df.empty:
            # 重命名列为标准格式
            df = df.rename(columns={
                'code': 'code',
                'code_name': 'name',
                'ipoDate': 'issue_date'
            })
            # 添加默认值
            df['issue_price'] = 0.0
            df['max_purchase'] = 0
            
            logger.info(f"从Baostock成功获取 {len(df)} 条新股信息")
            return df[['code', 'name', 'issue_price', 'max_purchase', 'issue_date']]
        
        logger.warning("Baostock返回空数据，尝试下一个数据源...")
    except Exception as e:
        logger.error(f"Baostock获取新股申购信息失败: {str(e)}")
    finally:
        try:
            bs.logout()
        except:
            pass
    
    # 尝试新浪财经（备用数据源2）
    try:
        logger.info("尝试从新浪财经获取新股申购信息...")
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        new_stocks = []
        if data:
            for item in data:
                code = item.get('symbol', '')
                name = item.get('name', '')
                issue_price = item.get('price', '')
                max_purchase = item.get('limit', '')
                issue_date = item.get('issue_date', '')
                
                # 只保留今天可申购的
                if issue_date == datetime.now().strftime('%Y-%m-%d'):
                    new_stocks.append({
                        'code': code,
                        'name': name,
                        'issue_price': issue_price,
                        'max_purchase': max_purchase,
                        'issue_date': issue_date
                    })
        
        if new_stocks:
            logger.info(f"从新浪财经成功获取 {len(new_stocks)} 条新股信息")
            return pd.DataFrame(new_stocks)
        else:
            logger.warning("新浪财经返回空数据，所有数据源均失败")
    except Exception as e:
        logger.error(f"新浪财经获取新股申购信息失败: {str(e)}")
    
    # 所有数据源均失败，返回空DataFrame
    logger.error("所有数据源均无法获取新股申购信息")
    return pd.DataFrame(columns=['code', 'name', 'issue_price', 'max_purchase', 'issue_date'])

def is_new_stock_info_pushed():
    """检查是否已经推送过新股信息"""
    return os.path.exists(Config.NEW_STOCK_INFO_PUSHED_FLAG)

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    with open(Config.NEW_STOCK_INFO_PUSHED_FLAG, 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M'))

def clear_new_stock_pushed_flag():
    """清除新股信息推送标记"""
    if os.path.exists(Config.NEW_STOCK_INFO_PUSHED_FLAG):
        os.remove(Config.NEW_STOCK_INFO_PUSHED_FLAG)

def get_new_stock_retry_time():
    """获取新股信息重试时间"""
    if os.path.exists(Config.NEW_STOCK_RETRY_FLAG):
        try:
            with open(Config.NEW_STOCK_RETRY_FLAG, 'r') as f:
                retry_time_str = f.read().strip()
                return datetime.strptime(retry_time_str, '%Y-%m-%d %H:%M')
        except:
            return None
    return None

def set_new_stock_retry():
    """设置新股信息重试"""
    with open(Config.NEW_STOCK_RETRY_FLAG, 'w') as f:
        f.write((datetime.now() + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M'))
    
    # 同时清除已推送标记，以便重试
    clear_new_stock_pushed_flag()

def clear_new_stock_retry_flag():
    """清除新股信息重试标记"""
    if os.path.exists(Config.NEW_STOCK_RETRY_FLAG):
        os.remove(Config.NEW_STOCK_RETRY_FLAG)

def get_test_new_stock_subscriptions():
    """
    获取测试用的新股申购信息（从真实数据源获取）
    返回:
        DataFrame: 测试数据（包含最近的新股信息）
    """
    # 尝试获取当天的新股信息
    new_stocks = get_new_stock_subscriptions()
    
    # 如果当天没有新股，尝试获取最近有新股的日期
    if new_stocks.empty:
        from datetime import datetime, timedelta
        
        # 尝试过去7天
        for i in range(1, 8):
            date_str = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
            try:
                # 使用Tushare获取历史新股数据
                ts.set_token(Config.TUSHARE_TOKEN)
                pro = ts.pro_api()
                df = pro.new_share(start_date=date_str, end_date=date_str)
                
                if not df.empty:
                    # 重命名列为标准格式
                    df = df.rename(columns={
                        'ts_code': 'code',
                        'name': 'name',
                        'price': 'issue_price',
                        'pe': 'pe_ratio',
                        'limit': 'max_purchase',
                        'amount': 'total_shares',
                        'mktcap': 'market_cap',
                        'ex_date': 'issue_date'
                    })
                    
                    # 只保留需要的列
                    df = df[['code', 'name', 'issue_price', 'max_purchase', 'issue_date']]
                    
                    # 添加历史标记
                    df['issue_date'] = df['issue_date'].apply(lambda x: f"{x} (历史数据)")
                    
                    logger.info(f"使用{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}的历史新股数据进行测试")
                    return df
            except Exception as e:
                logger.error(f"获取{date_str}新股申购信息失败: {str(e)}")
    
    # 如果有当天的新股，直接返回
    return new_stocks
