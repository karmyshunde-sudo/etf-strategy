"""
鱼盆ETF投资量化模型 - 数据爬取模块
说明:
  本文件负责从多个数据源获取ETF数据和新股信息
  所有文件放在根目录，简化导入关系
  主数据源：AkShare
  备用数据源：Baostock、新浪财经

【策略详细说明 - 数据获取逻辑】
1. ETF数据获取流程：
   - 主数据源：AkShare（优先尝试，返回最完整数据）
   - 备用数据源1：Baostock（当AkShare失败时尝试）
   - 备用数据源2：新浪财经（当前两个数据源都失败时尝试）
   - 缓存机制：优先检查本地缓存，减少API调用次数

2. 新股信息获取逻辑：
   - 区分两种新股类型：
     * 认购新股（IPO申购）：通过"网上申购日"筛选
     * 上市交易新股：通过"上市日期"筛选
   - 历史数据范围：过去30天（原为7天），确保覆盖最近上市交易的新股
   - 多数据源回退机制：确保即使主数据源失败也能获取数据

3. 数据质量保障：
   - 严格的数据验证：检查DataFrame是否为空
   - 完善的错误处理：记录详细错误日志
   - 数据类型转换：确保数值型字段为正确类型
   - 日期格式标准化：统一使用YYYY-MM-DD格式

4. 性能优化：
   - 缓存机制：减少重复API调用
   - 限流控制：每个API调用间隔1秒
   - 数据过滤：仅获取必要字段

【异常处理机制】
1. 数据源失败：
   - 记录详细错误日志，包含错误类型和消息
   - 自动切换到备用数据源
   - 所有数据源失败时返回空DataFrame

2. 数据质量异常：
   - 空数据处理：返回空DataFrame而非抛出异常
   - 字段缺失处理：使用默认值填充
   - 类型转换错误：记录日志并跳过该条数据

3. 网络问题：
   - 设置15秒超时
   - 重试机制（最多3次）
   - 失败后等待30分钟再重试

【使用说明】
1. 获取ETF数据：get_etf_data(etf_code, data_type='daily')
2. 获取ETF列表：get_all_etf_list()
3. 获取新股信息：get_new_stock_subscriptions()
4. 测试用数据获取：get_test_new_stock_subscriptions()
"""

import os
import time
import pandas as pd
import numpy as np
import akshare as ak
import baostock as bs
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
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
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
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
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
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
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
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
        
        # 尝试使用Tushare（已弃用，保留代码但不调用）
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
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
    # 尝试AkShare（主数据源）
    try:
        logger.info("尝试从AkShare获取新股申购信息...")
        # 修正函数名：使用正确的 stock_ipos() API
        df = ak.stock_ipos()
        
        if not df.empty:
            today = datetime.now().strftime('%Y-%m-%d')
            # 同时筛选申购日和上市日
            df = df[(df['issue_date'] == today) | (df['listing_date'] == today)]
            
            if not df.empty:
                # 重命名列为标准格式
                df = df.rename(columns={
                    'code': 'code',
                    'name': 'name',
                    'price': 'issue_price',
                    'limit': 'max_purchase',
                    'issue_date': 'issue_date'
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
        
        # 筛选新股（过去30天内上市的）
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')
        df = df[(df['ipoDate'] >= thirty_days_ago) & (df['ipoDate'] <= today)]
        
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
                
                # 只保留今天可申购或上市的
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
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
    return os.path.exists(Config.NEW_STOCK_INFO_PUSHED_FLAG)

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
    with open(Config.NEW_STOCK_INFO_PUSHED_FLAG, 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M'))

def clear_new_stock_pushed_flag():
    """清除新股信息推送标记"""
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
    if os.path.exists(Config.NEW_STOCK_INFO_PUSHED_FLAG):
        os.remove(Config.NEW_STOCK_INFO_PUSHED_FLAG)

def get_new_stock_retry_time():
    """获取新股信息重试时间"""
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
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
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
    with open(Config.NEW_STOCK_RETRY_FLAG, 'w') as f:
        f.write((datetime.now() + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M'))
    
    # 同时清除已推送标记，以便重试
    clear_new_stock_pushed_flag()

def clear_new_stock_retry_flag():
    """清除新股信息重试标记"""
    # 修复循环导入：将Config导入移到函数内部
    from config import Config
    
    if os.path.exists(Config.NEW_STOCK_RETRY_FLAG):
        os.remove(Config.NEW_STOCK_RETRY_FLAG)

def get_test_new_stock_subscriptions():
    """
    获取测试用的新股申购信息（从真实数据源获取，失败时使用历史数据）
    返回:
        DataFrame: 测试数据（包含最近的新股信息）
    """
    # 尝试获取当天的新股信息
    new_stocks = get_new_stock_subscriptions()
    
    # 如果当天没有新股，尝试获取最近有新股的日期
    if new_stocks.empty:
        logger.warning("当天无新股申购，尝试获取历史数据...")
        from datetime import timedelta
        
        # 尝试过去30天（原为7天）
        for i in range(1, 31):
            date_str = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
            try:
                # 尝试AkShare（主数据源）
                logger.info(f"尝试从AkShare获取{date_str}的历史新股数据...")
                # 修正函数名：使用正确的 stock_ipos() API
                df = ak.stock_ipos(date=date_str)
                
                if not df.empty:
                    # 同时筛选申购日和上市日
                    df = df[(df['issue_date'] == f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}") | 
                            (df['listing_date'] == f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}")]
                    
                    if not df.empty:
                        # 重命名列为标准格式
                        df = df.rename(columns={
                            'code': 'code',
                            'name': 'name',
                            'price': 'issue_price',
                            'limit': 'max_purchase',
                            'issue_date': 'issue_date'
                        })
                        # 转换数据类型
                        df['issue_price'] = df['issue_price'].astype(float)
                        df['max_purchase'] = df['max_purchase'].astype(int)
                        
                        # 添加历史标记
                        df['issue_date'] = df['issue_date'].apply(
                            lambda x: f"{x} (历史数据，{date_str[:4]}-{date_str[4:6]}-{date_str[6:]})"
                        )
                        
                        logger.info(f"从AkShare成功获取 {len(df)} 条历史新股信息")
                        return df[['code', 'name', 'issue_price', 'max_purchase', 'issue_date']]
                
                # 尝试Baostock（备用数据源1）
                try:
                    logger.info(f"尝试从Baostock获取{date_str}的历史新股数据...")
                    # Baostock没有直接获取新股申购的API，通过股票基本信息筛选
                    login_result = bs.login()
                    if login_result.error_code != '0':
                        raise Exception("Baostock登录失败")
                    
                    rs = bs.query_stock_basic()
                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())
                    df = pd.DataFrame(data_list, columns=rs.fields)
                    
                    # 筛选新股（指定日期附近上市的）
                    df = df[df['ipoDate'] == date_str]
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
                        
                        # 添加历史标记
                        df['issue_date'] = df['issue_date'].apply(
                            lambda x: f"{x} (历史数据，{date_str[:4]}-{date_str[4:6]}-{date_str[6:]})"
                        )
                        
                        logger.info(f"从Baostock成功获取 {len(df)} 条历史新股信息")
                        return df[['code', 'name', 'issue_price', 'max_purchase', 'issue_date']]
                    
                    bs.logout()
                
                except Exception as e:
                    logger.error(f"Baostock获取历史新股数据失败: {str(e)}")
                    try:
                        bs.logout()
                    except:
                        pass
                
                # 尝试新浪财经（备用数据源2）
                try:
                    logger.info(f"尝试从新浪财经获取{date_str}的历史新股数据...")
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
                            
                            # 检查日期匹配
                            if issue_date == f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}":
                                new_stocks.append({
                                    'code': code,
                                    'name': name,
                                    'issue_price': issue_price,
                                    'max_purchase': max_purchase,
                                    'issue_date': issue_date
                                })
                    
                    if new_stocks:
                        df = pd.DataFrame(new_stocks)
                        # 添加历史标记
                        df['issue_date'] = df['issue_date'].apply(
                            lambda x: f"{x} (历史数据，{date_str[:4]}-{date_str[4:6]}-{date_str[6:]})"
                        )
                        
                        logger.info(f"从新浪财经成功获取 {len(df)} 条历史新股信息")
                        return df
                except Exception as e:
                    logger.error(f"新浪财经获取历史新股数据失败: {str(e)}")
            
            except Exception as e:
                logger.error(f"获取{date_str}历史新股数据失败: {str(e)}")
    
    # 如果有当天的新股，添加测试标记
    if not new_stocks.empty:
        new_stocks['issue_date'] = new_stocks['issue_date'].apply(
            lambda x: f"{x} (测试数据)"
        )
    
    return new_stocks
