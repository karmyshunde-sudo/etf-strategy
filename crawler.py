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
        # 从AkShare获取ETF列表
        df = ak.fund_etf_category(symbol="ETF基金")
        if not df.empty:
            # 筛选仅保留ETF
            etf_list = df[df['基金类型'] == 'ETF'].copy()
            etf_list = etf_list[['基金代码', '基金简称']]
            etf_list.columns = ['code', 'name']
            return etf_list
        
        # 如果AkShare失败，尝试Tushare
        ts.set_token(Config.TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        # 获取ETF基础信息
        df = pro.fund_basic(market='E', status='L')
        if not df.empty:
            # 筛选ETF
            df = df[df['type'] == 'E']
            df = df[['ts_code', 'name']]
            df.columns = ['code', 'name']
            # 去掉交易所后缀
            df['code'] = df['code'].str.replace(r'\.(SH|SZ)', '', regex=True)
            return df
        
        # 如果Tushare也失败，尝试从新浪财经获取
        sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=etf_hk&symbol=&_s_r_a=page"
        response = requests.get(sina_url, timeout=15)
        data = response.json()
        
        if data:
            etf_list = pd.DataFrame(data)
            etf_list = etf_list[['symbol', 'name']]
            etf_list.columns = ['code', 'name']
            return etf_list
        
        # 如果所有数据源都失败，返回一个默认列表
        return pd.DataFrame({
            'code': ['510050', '510300', '510500', '159915', '512888', '512480', '512660', '512980', '159825', '159995'],
            'name': ['上证50ETF', '沪深300ETF', '中证500ETF', '创业板ETF', '消费ETF', '半导体ETF', '军工ETF', '通信ETF', '新能源ETF', '医疗ETF']
        })
    except Exception as e:
        logger.error(f"获取ETF列表失败: {str(e)}")
        # 返回默认ETF列表
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
    返回:
        DataFrame: 当天可申购的新股信息
    """
    try:
        # 使用Tushare获取新股申购信息
        ts.set_token(Config.TUSHARE_TOKEN)
        pro = ts.pro_api()
        
        # 获取今日新股申购信息
        today = datetime.now().strftime('%Y%m%d')
        df = pro.new_share(start_date=today, end_date=today)
        
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
            
            return df
    except Exception as e:
        logger.error(f"获取新股申购信息失败: {str(e)}")
    
    # 如果Tushare失败，尝试其他数据源
    try:
        # 备用数据源：东方财富
        eastmoney_url = "http://data.eastmoney.com/xg/xg/"
        response = requests.get(eastmoney_url, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取新股申购信息
        new_stocks = []
        table = soup.find('table', {'id': 'tb'})
        if table:
            rows = table.find_all('tr')[1:]  # 跳过表头
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    code = cols[1].text.strip()
                    name = cols[2].text.strip()
                    issue_price = cols[3].text.strip()
                    max_purchase = cols[4].text.strip()
                    issue_date = cols[5].text.strip()
                    
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
            return pd.DataFrame(new_stocks)
    except Exception as e:
        logger.error(f"从东方财富获取新股申购信息失败: {str(e)}")
    
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
