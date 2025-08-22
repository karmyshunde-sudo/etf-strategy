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
    os.makedirs(base_path, exist_ok=True)
    
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
        error_msg = f"【数据错误】缓存加载错误 {etf_code}: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None
    return None

def resume_crawl():
    """断点续爬任务（真正实现增量爬取）"""
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
        error_msg = f"【系统错误】加载爬取状态失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return {"status": "error", "message": "Failed to load status"}
    
    # 筛选未完成的ETF
    pending_etfs = [
        code for code, status in crawl_status.items()
        if status.get('status') in ['in_progress', 'failed']
    ]
    
    if not pending_etfs:
        logger.info("无待续爬ETF，任务已完成")
        return {"status": "success", "message": "No pending ETFs"}
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        error_msg = "【数据错误】未获取到ETF列表，跳过爬取"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return {"status": "skipped", "message": "No ETF list available"}
    
    # 筛选待爬ETF
    pending_etf_list = etf_list[etf_list['code'].isin(pending_etfs)]
    
    # 继续爬取
    success_count = 0
    failed_count = 0
    for _, etf in pending_etf_list.iterrows():
        etf_code = etf['code']
        try:
            # 获取起始日期（从缓存中获取最后日期）- 关键增量爬取逻辑
            cached_data = load_from_cache(etf_code, 'daily')
            start_date = None
            if cached_data is not None and not cached_data.empty:
                last_date = cached_data['date'].max()
                start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"ETF {etf_code} 已有数据到 {last_date.strftime('%Y-%m-%d')}，从 {start_date} 开始获取新数据")
            
            # 标记开始
            update_crawl_status(etf_code, 'in_progress')
            logger.info(f"【任务开始】开始续爬 {etf_code}")
            
            # 尝试主数据源(AkShare)
            data = crawl_akshare(etf_code, start_date=start_date)
            if data is None or data.empty:
                # 尝试备用数据源1(Baostock)
                data = crawl_baostock(etf_code, start_date=start_date)
                if data is None or data.empty:
                    # 尝试备用数据源2(新浪财经)
                    data = crawl_sina_finance(etf_code, start_date=start_date)
            
            # 检查结果
            if data is not None and not data.empty:
                # 保存数据（已在crawl_*函数内部完成增量保存）
                update_crawl_status(etf_code, 'success')
                success_count += 1
                logger.info(f"成功续爬 {etf_code}，共 {len(data)} 条新记录")
            else:
                error_msg = f"【数据错误】续爬 {etf_code} 失败：返回空数据"
                logger.warning(error_msg)
                send_wecom_message(error_msg)
                update_crawl_status(etf_code, 'failed', 'Empty data')
                failed_count += 1
        except Exception as e:
            error_msg = f"【系统错误】续爬 {etf_code} 时出错: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            update_crawl_status(etf_code, 'failed', str(e))
            failed_count += 1
        
        # 避免请求过快
        time.sleep(1)
    
    # 检查是否全部完成
    remaining = [
        code for code, status in get_crawl_status().items()
        if status.get('status') in ['in_progress', 'failed']
    ]
    
    if not remaining:
        try:
            os.remove(status_file)
            logger.info("所有ETF爬取成功，已清理状态文件")
        except Exception as e:
            error_msg = f"【系统错误】清理状态文件失败: {str(e)}"
            logger.warning(error_msg)
            send_wecom_message(error_msg)
    
    # 生成汇总
    logger.info(f"断点续爬完成：成功 {success_count}/{len(pending_etfs)}，失败 {failed_count}")
    
    return {
        "status": "partial_success" if failed_count > 0 else "success",
        "total_pending": len(pending_etfs),
        "success": success_count,
        "failed": failed_count
    }

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
        error_msg = f"【系统错误】更新爬取状态失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)

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
        # 1. 如果文件已存在，只添加新数据
        if os.path.exists(cache_path):
            existing_data = pd.read_csv(cache_path)
            if 'date' in existing_data.columns:
                existing_data['date'] = pd.to_datetime(existing_data['date'])
                
                # 获取现有数据的最后日期
                last_date = existing_data['date'].max()
                
                # 筛选新数据中日期大于last_date的部分
                data = data[data['date'] > last_date]
                
                if data.empty:
                    logger.info(f"没有新数据需要保存到 {cache_path}")
                    return True
                
                # 合并数据
                combined = pd.concat([existing_data, data]).drop_duplicates(subset=['date'], keep='last')
            else:
                combined = data
        else:
            combined = data
        
        # 2. 先写入临时文件
        combined.to_csv(temp_path, index=False)
        
        # 3. 原子操作：先删除原文件，再重命名
        if os.path.exists(cache_path):
            os.remove(cache_path)
        os.rename(temp_path, cache_path)
        
        logger.info(f"成功保存 {etf_code} 数据到 {cache_path} ({len(data)}条新记录)")
        return True
    except Exception as e:
        error_msg = f"【数据错误】保存 {etf_code} 数据失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        # 清理临时文件
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return False

def crawl_akshare(etf_code, start_date=None):
    """从AkShare爬取ETF数据（主数据源）
    参数:
        etf_code: ETF代码
        start_date: 开始日期（可选）
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
            error_msg = f"【数据错误】AkShare返回空数据 {etf_code}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
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
            error_msg = f"【数据错误】AkShare返回的数据缺少必要列: {df.columns.tolist()}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 将日期转换为datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # 按日期排序
        df = df.sort_values('date')
        
        # 如果指定了起始日期，筛选数据
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        
        logger.info(f"从AkShare成功获取 {etf_code} 历史数据 ({len(df)}条记录)")
        return df
    except AttributeError as e:
        error_msg = f"【数据错误】AkShare接口错误: {str(e)} - 请确保akshare已升级至最新版"
        logger.error(error_msg)
        send_wecom_message(error_msg)
    except Exception as e:
        error_msg = f"【数据错误】AkShare爬取错误 {etf_code}: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
    return None

def crawl_baostock(etf_code, start_date=None):
    """从Baostock爬取ETF数据（备用数据源1）
    参数:
        etf_code: ETF代码
        start_date: 开始日期（可选）
    返回:
        DataFrame: ETF数据或None（如果失败）"""
    try:
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            error_msg = f"【数据错误】Baostock登录失败: {login_result.error_msg}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 为Baostock格式化ETF代码（添加sh.或sz.前缀）
        market = 'sh' if etf_code.startswith('5') else 'sz'
        code = f"{market}.{etf_code}"
        
        # 设置日期范围
        if start_date:
            start_date_str = start_date
        else:
            start_date_str = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        
        end_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 获取历史数据
        rs = bs.query_history_k_data_plus(
            code, 
            "date,open,high,low,close,volume",
            start_date=start_date_str,
            end_date=end_date_str,
            frequency="d", 
            adjustflag="3"
        )
        
        if rs.error_code != '0':
            error_msg = f"【数据错误】Baostock查询失败: {rs.error_msg}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        if df.empty:
            error_msg = f"【数据错误】Baostock返回空数据 {etf_code}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
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
        error_msg = f"【数据错误】Baostock爬取错误 {etf_code}: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
    finally:
        try:
            bs.logout()
        except:
            pass
    return None

def crawl_sina_finance(etf_code, start_date=None):
    """从新浪财经爬取ETF数据（备用数据源2）
    参数:
        etf_code: ETF代码
        start_date: 开始日期（可选）
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
            error_msg = f"【数据错误】新浪财经返回空数据 {etf_code}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        kline_data = data['data']
        
        # 转换为DataFrame
        df = pd.DataFrame(kline_data)
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
        
        # 如果指定了起始日期，筛选数据
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        
        logger.info(f"成功从新浪财经爬取 {etf_code} 数据 ({len(df)}条记录)")
        return df
    except Exception as e:
        error_msg = f"【数据错误】新浪财经爬取错误 {etf_code}: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
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
    
    # 如果缓存为空，返回None（策略计算不应触发爬取）
    error_msg = f"【数据错误】缓存中无{etf_code}数据，请先运行数据爬取任务"
    logger.error(error_msg)
    send_wecom_message(error_msg)
    return None

def get_etf_iopv_data(etf_code):
    """获取ETF的IOPV数据（基金净值估算）
    参数:
        etf_code: ETF代码
    返回:
        DataFrame: IOPV数据或None（如果失败）"""
    try:
        # 从代码中提取纯数字代码
        pure_code = etf_code.replace('sh.', '').replace('sz.', '')
        
        # 使用AkShare获取IOPV数据
        df = ak.fund_etf_hist_em(symbol=pure_code)
        
        if df.empty:
            error_msg = f"【数据错误】获取{etf_code} IOPV数据失败：AkShare返回空数据"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
        
        # 重命名列为标准格式
        column_mapping = {
            '日期': 'date',
            '基金净值估算': 'iopv'
        }
        
        # 选择存在的列进行重命名
        existing_cols = [col for col in column_mapping.keys() if col in df.columns]
        if '基金净值估算' in existing_cols:
            df = df.rename(columns=column_mapping)
            
            # 将日期转换为datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # 按日期排序
            df = df.sort_values('date')
            
            return df[['date', 'iopv']]
        else:
            error_msg = f"【数据错误】获取{etf_code} IOPV数据失败：数据中缺少'基金净值估算'列"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return None
    except Exception as e:
        error_msg = f"【数据错误】获取{etf_code} IOPV数据失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return None

def get_market_sentiment():
    """获取市场情绪指标
    返回:
        float: 市场情绪指标（范围：-1到1，-1表示极度悲观，1表示极度乐观）"""
    try:
        # 从多个指标获取市场情绪
        # 1. 从A股涨跌比获取市场情绪
        df = ak.stock_zh_a_spot()
        if not df.empty:
            # 计算涨跌比
            up_count = len(df[df['涨跌幅'] > 0])
            down_count = len(df[df['涨跌幅'] < 0])
            total = up_count + down_count
            
            if total > 0:
                # 计算市场情绪（范围：-1到1）
                sentiment = (up_count - down_count) / total
                return sentiment
        
        # 2. 如果无法获取A股数据，使用默认市场情绪
        error_msg = "【数据错误】获取市场情绪数据失败，使用默认值"
        logger.warning(error_msg)
        send_wecom_message(error_msg)
        return 0.0
    except Exception as e:
        error_msg = f"【数据错误】获取市场情绪数据失败: {str(e)}，使用默认值"
        logger.warning(error_msg)
        send_wecom_message(error_msg)
        return 0.0

def check_data_integrity():
    """检查数据完整性
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
        etf_code = etf['code']
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

def crawl_etf_data(data_type='daily'):
    """爬取ETF数据并保存到缓存
    参数:
        data_type: 'daily'或'intraday'
    返回:
        bool: 是否成功"""
    
    # 检查是否为交易日
    if not is_trading_day() and data_type == 'daily':
        logger.info("今天不是交易日，跳过ETF数据爬取")
        return False
    
    # 获取所有ETF列表
    etf_list = get_all_etf_list()
    if etf_list is None or etf_list.empty:
        error_msg = "【数据错误】未获取到ETF列表，跳过爬取"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return False
    
    logger.info(f"【任务准备】开始爬取 {len(etf_list)} 只ETF的{data_type}数据")
    
    # 统计
    success_count = 0
    failed_count = 0
    
    # 爬取每只ETF的数据
    for _, etf in etf_list.iterrows():
        etf_code = etf['code']
        
        try:
            # 获取起始日期（从缓存中获取最后日期）
            cached_data = load_from_cache(etf_code, data_type)
            start_date = None
            if cached_data is not None and not cached_data.empty:
                last_date = cached_data['date'].max()
                start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"ETF {etf_code} 已有数据到 {last_date.strftime('%Y-%m-%d')}，从 {start_date} 开始获取新数据")
            
            # 标记开始
            update_crawl_status(etf_code, 'in_progress')
            logger.info(f"【任务开始】开始爬取 {etf_code}")
            
            # 尝试主数据源(AkShare) - 仅用于日线数据
            if data_type == 'daily':
                data = crawl_akshare(etf_code, start_date=start_date)
                if data is not None and not data.empty:
                    logger.info(f"成功从AkShare爬取{etf_code}日线数据")
                    save_to_cache(etf_code, data, data_type)
                    success_count += 1
                    continue
            
            # 尝试备用数据源1(Baostock)
            data = crawl_baostock(etf_code, start_date=start_date)
            if data is not None and not data.empty:
                logger.info(f"成功从Baostock爬取{etf_code}数据")
                save_to_cache(etf_code, data, data_type)
                success_count += 1
                continue
            
            # 尝试备用数据源2(新浪财经)
            data = crawl_sina_finance(etf_code, start_date=start_date)
            if data is not None and not data.empty:
                logger.info(f"成功从新浪财经爬取{etf_code}数据")
                save_to_cache(etf_code, data, data_type)
                success_count += 1
                continue
            
            # 所有数据源均失败
            error_msg = f"【数据错误】无法从所有数据源获取{etf_code}数据"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            update_crawl_status(etf_code, 'failed', 'All data sources failed')
            failed_count += 1
        except Exception as e:
            error_msg = f"【系统错误】爬取{etf_code}日线数据异常: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            # 标记异常
            update_crawl_status(etf_code, 'failed', str(e))
            failed_count += 1
        
        # 避免请求过快
        time.sleep(1)
    
    # 生成汇总报告
    total = len(etf_list)
    logger.info(f"【任务完成】{data_type}数据爬取完成：成功 {success_count}/{total}，失败 {failed_count}")
    
    return success_count > 0

def crawl_new_stock_info():
    """爬取新股申购信息并保存"""
    try:
        # 获取新股数据
        new_stocks = get_new_stock_subscriptions()
        if new_stocks is not None and not new_stocks.empty:
            # 保存到数据目录
            os.makedirs(Config.NEW_STOCK_DIR, exist_ok=True)
            filename = f"new_stocks_{get_beijing_time().strftime('%Y%m%d')}.csv"
            filepath = os.path.join(Config.NEW_STOCK_DIR, filename)
            new_stocks.to_csv(filepath, index=False)
            logger.info(f"成功保存新股数据到 {filepath}")
            return True
        else:
            logger.info("无新股数据需要保存")
            return False
    except Exception as e:
        error_msg = f"【数据错误】保存新股数据失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return False

def crawl_new_listing_info():
    """爬取新上市股票信息并保存"""
    try:
        # 获取新上市股票数据
        new_listings = get_new_stock_listings()
        if new_listings is not None and not new_listings.empty:
            # 保存到数据目录
            os.makedirs(Config.NEW_STOCK_DIR, exist_ok=True)
            filename = f"new_listings_{get_beijing_time().strftime('%Y%m%d')}.csv"
            filepath = os.path.join(Config.NEW_STOCK_DIR, filename)
            new_listings.to_csv(filepath, index=False)
            logger.info(f"成功保存新上市股票数据到 {filepath}")
            return True
        else:
            logger.info("无新上市股票数据需要保存")
            return False
    except Exception as e:
        error_msg = f"【数据错误】保存新上市股票数据失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return False

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
            error_msg = "【数据错误】AkShare返回空ETF列表"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            raise Exception("数据为空")
        
        # 处理列名
        required_columns = ['基金代码', '基金名称']
        if all(col in df.columns for col in required_columns):
            etf_list = df[required_columns].copy()
            etf_list['code'] = etf_list['基金代码'].apply(
                lambda x: f"sh.{x}" if str(x).startswith('5') else f"sz.{x}"
            )
            etf_list.columns = ['code', 'name']
            logger.info(f"从AkShare成功获取 {len(etf_list)} 只ETF")
            return etf_list
        else:
            error_msg = "【数据错误】AkShare返回数据缺少必要列"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            raise Exception("数据格式不匹配")
    except Exception as e:
        error_msg = f"【数据错误】AkShare获取ETF列表失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
    
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
            # 提取唯一ETF代码
            etf_codes = df['基金代码'].unique()
            etf_names = {row['基金代码']: row['基金简称'] for _, row in df.iterrows() if '基金代码' in row and '基金简称' in row}
            
            etf_list = pd.DataFrame({
                'code': [f"sh.{c}" if c.startswith('5') else f"sz.{c}" for c in etf_codes],
                'name': [etf_names.get(c, c) for c in etf_codes]
            })
            
            logger.info(f"从AkShare备用接口成功获取 {len(etf_list)} 只ETF")
            return etf_list
    except Exception as e:
        error_msg = f"【数据错误】AkShare备用接口获取ETF列表失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
    
    # 尝试Baostock
    try:
        logger.info("尝试从Baostock获取ETF列表...")
        login_result = bs.login()
        if login_result.error_code != '0':
            error_msg = f"【数据错误】Baostock登录失败: {login_result.error_msg}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
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
            error_msg = "【数据错误】Baostock返回空ETF列表"
            logger.error(error_msg)
            send_wecom_message(error_msg)
    except Exception as e:
        error_msg = f"【数据错误】Baostock获取ETF列表失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
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
                
                if 'data' in data:
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
            error_msg = "【数据错误】新浪财经返回空ETF列表"
            logger.error(error_msg)
            send_wecom_message(error_msg)
    except Exception as e:
        error_msg = f"【数据错误】新浪财经获取ETF列表失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
    
    # 如果所有数据源都失败，返回None
    error_msg = "【数据错误】所有数据源均无法获取ETF列表，无法继续执行"
    logger.error(error_msg)
    send_wecom_message(error_msg)
    return None

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
            error_msg = f"【系统错误】无法解析日期: {content}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return (None, False) if target_date is not None else None
        
        # 根据是否提供目标日期返回不同结果
        if target_date is None:
            return file_date
        else:
            # 确保target_date是日期类型
            if isinstance(target_date, datetime.datetime):
                target_date = target_date.date()
            return file_date, (file_date == target_date)
    except Exception as e:
        error_msg = f"【系统错误】读取推送标记文件错误: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return (None, False) if target_date is not None else None

def mark_new_stock_info_pushed():
    """标记新股信息已推送"""
    try:
        with open(Config.NEW_STOCK_PUSHED_FLAG, 'w') as f:
            f.write(get_beijing_time().strftime('%Y-%m-%d'))
    except Exception as e:
        error_msg = f"【系统错误】标记新股信息推送失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)

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
            error_msg = f"【系统错误】无法解析日期: {content}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return (None, False) if target_date is not None else None
        
        # 根据是否提供目标日期返回不同结果
        if target_date is None:
            return file_date
        else:
            # 确保target_date是日期类型
            if isinstance(target_date, datetime.datetime):
                target_date = target_date.date()
            return file_date, (file_date == target_date)
    except Exception as e:
        error_msg = f"【系统错误】读取推送标记文件错误: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        return (None, False) if target_date is not None else None

def mark_listing_info_pushed():
    """标记新上市交易股票信息已推送"""
    try:
        with open(Config.LISTING_PUSHED_FLAG, 'w') as f:
            f.write(get_beijing_time().strftime('%Y-%m-%d'))
    except Exception as e:
        error_msg = f"【系统错误】标记新上市交易信息推送失败: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)

def get_new_stock_subscriptions(test=False):
    """获取当天新股数据
    参数:
        test: 是否为测试模式（测试模式下若当天无数据则回溯21天）"""
    today = get_beijing_time().strftime('%Y-%m-%d')
    
    # 如果是测试模式，准备回溯21天
    if test:
        dates_to_try = [(datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d') 
                       for i in range(0, 22)]
    else:
        dates_to_try = [today]
    
    for date_str in dates_to_try:
        logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {date_str} 的新股数据")
        
        # 尝试AkShare（主数据源）
        try:
            df = ak.stock_xgsglb_em()
            if not df.empty:
                # 动态匹配列名
                date_col = next((col for col in df.columns if '日期' in col), None)
                
                if date_col and date_col in df.columns:
                    # 筛选目标日期数据
                    df = df[df[date_col] == date_str]
                    if not df.empty:
                        # 提取必要列
                        code_col = next((col for col in df.columns if '代码' in col), None)
                        name_col = next((col for col in df.columns if '名称' in col or '简称' in col), None)
                        price_col = next((col for col in df.columns if '价格' in col), None)
                        limit_col = next((col for col in df.columns if '上限' in col), None)
                        
                        if all(col is not None for col in [code_col, name_col]):
                            valid_df = df[[code_col, name_col]]
                            if price_col: valid_df['发行价格'] = df[price_col]
                            if limit_col: valid_df['申购上限'] = df[limit_col]
                            valid_df['申购日期'] = date_str
                            return valid_df.rename(columns={
                                code_col: '股票代码',
                                name_col: '股票简称'
                            })
        except Exception as e:
            error_msg = f"【数据错误】AkShare获取{date_str}新股信息失败: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
        
        # 尝试新浪财经（备用数据源2）
        try:
            sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
            response = requests.get(sina_url, timeout=15, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            })
            response.raise_for_status()
            data = response.json()
            
            new_stocks = []
            for item in data['data']:
                ipo_date = item.get('ipo_date', '')
                # 标准化日期格式
                if len(ipo_date) == 8:  # YYYYMMDD
                    ipo_date = f"{ipo_date[:4]}-{ipo_date[4:6]}-{ipo_date[6:]}"
                
                if ipo_date == date_str:
                    new_stocks.append({
                        '股票代码': item.get('symbol', ''),
                        '股票简称': item.get('name', ''),
                        '发行价格': item.get('price', ''),
                        '申购上限': item.get('max_purchase', ''),
                        '申购日期': date_str
                    })
            
            if new_stocks:
                return pd.DataFrame(new_stocks)
        except Exception as e:
            error_msg = f"【数据错误】新浪财经获取{date_str}新股信息失败: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
    
    error_msg = f"{'测试模式' if test else '正常模式'}: 未找到新股数据"
    logger.warning(error_msg)
    return pd.DataFrame()

def get_new_stock_listings(test=False):
    """获取当天新上市交易的新股数据
    参数:
        test: 是否为测试模式（测试模式下若当天无数据则回溯21天）"""
    today = get_beijing_time().strftime('%Y-%m-%d')
    
    # 如果是测试模式，准备回溯21天
    if test:
        dates_to_try = [(datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d') 
                       for i in range(0, 22)]
    else:
        dates_to_try = [today]
    
    for date_str in dates_to_try:
        logger.info(f"{'测试模式' if test else '正常模式'}: 尝试获取 {date_str} 的新上市交易数据")
        
        try:
            # 尝试AkShare（主数据源）
            df = ak.stock_xgsglb_em()
            if not df.empty:
                # 动态匹配上市日期列
                listing_date_col = next((col for col in df.columns if '上市日期' in col), None)
                
                if listing_date_col and listing_date_col in df.columns:
                    # 筛选目标日期数据
                    df = df[df[listing_date_col] == date_str]
                    if not df.empty:
                        # 提取必要列
                        code_col = next((col for col in df.columns if '代码' in col), None)
                        name_col = next((col for col in df.columns if '名称' in col or '简称' in col), None)
                        price_col = next((col for col in df.columns if '价格' in col), None)
                        
                        if all(col is not None for col in [code_col, name_col]):
                            valid_df = df[[code_col, name_col]]
                            if price_col: valid_df['发行价格'] = df[price_col]
                            valid_df['上市日期'] = date_str
                            return valid_df.rename(columns={
                                code_col: '股票代码',
                                name_col: '股票简称'
                            })
        except Exception as e:
            error_msg = f"【数据错误】AkShare获取{date_str}新上市交易信息失败: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
        
        # 尝试Baostock（备用数据源1）
        try:
            # 登录Baostock
            login_result = bs.login()
            if login_result.error_code != '0':
                error_msg = f"【数据错误】Baostock登录失败: {login_result.error_msg}"
                logger.error(error_msg)
                send_wecom_message(error_msg)
                raise Exception("Baostock登录失败")
            
            # 获取新股列表
            rs = bs.query_stock_new()
            if rs.error_code != '0':
                error_msg = f"【数据错误】Baostock查询失败: {rs.error_msg}"
                logger.error(error_msg)
                send_wecom_message(error_msg)
                return pd.DataFrame()
            
            # 转换为DataFrame
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                return df[['code', 'code_name', 'price', 'ipoDate']].rename(columns={
                    'code': '股票代码',
                    'code_name': '股票简称',
                    'price': '发行价格',
                    'ipoDate': '上市日期'
                })
            else:
                logger.warning("Baostock返回空数据，尝试下一个数据源...")
        except Exception as e:
            error_msg = f"【数据错误】Baostock获取{date_str}新上市交易信息失败: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
        
        # 尝试新浪财经（备用数据源2）
        try:
            logger.info("尝试从新浪财经获取新上市交易股票信息...")
            sina_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=1&node=iponew&symbol=&_s_r_a=page"
            response = requests.get(sina_url, timeout=15)
            response.raise_for_status()
            data = response.json()
            new_listings = []
            for item in data['data']:
                if item.get('listing_date') == date_str:
                    new_listings.append({
                        '股票代码': item.get('symbol'),
                        '股票简称': item.get('name'),
                        '发行价格': item.get('price'),
                        '上市日期': item.get('listing_date')
                    })
            
            if new_listings:
                return pd.DataFrame(new_listings)
        except Exception as e:
            error_msg = f"【数据错误】新浪财经获取{date_str}新上市交易信息失败: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
    
    error_msg = f"{'测试模式' if test else '正常模式'}: 未找到新上市交易数据"
    logger.warning(error_msg)
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
                error_msg = f"【系统错误】删除文件失败 {filepath}: {str(e)}"
                logger.error(error_msg)
                send_wecom_message(error_msg)

def cron_crawl_daily():
    """日线数据爬取任务"""
    logger.info("日线数据爬取任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过爬取")
        return {"status": "skipped", "message": "Not trading day"}
    
    # 执行ETF数据爬取
    success = crawl_etf_data(data_type='daily')
    
    # 执行Git推送
    if success:
        try:
            git_push()
            logger.info("Git推送成功完成")
        except Exception as e:
            error_msg = f"【系统错误】Git推送失败: {str(e)}"
            logger.error(error_msg)
            send_wecom_message(error_msg)
            return {"status": "error", "message": error_msg}
    
    return {"status": "success" if success else "error", "message": "Daily data crawl completed"}

def cron_crawl_intraday():
    """盘中数据爬取任务"""
    logger.info("盘中数据爬取任务触发")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，跳过爬取")
        return {"status": "skipped", "message": "Not trading day"}
    
    # 执行ETF数据爬取
    success = crawl_etf_data(data_type='intraday')
    
    return {"status": "success" if success else "error", "message": "Intraday data crawl completed"}

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
            check=False
        )
        
        # 检查拉取结果
        if pull_result.returncode != 0:
            error_msg = f"【Git操作】拉取远程代码时警告: {pull_result.stderr.strip()}"
            logger.warning(error_msg)
            send_wecom_message(error_msg)
        
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
        error_msg = f"【系统错误】Git操作失败: {e}\n输出: {e.stdout}\n错误: {e.stderr}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        # 关键修改：出错立即终止，不再继续
        raise RuntimeError(error_msg) from None
    except Exception as e:
        error_msg = f"【系统错误】Git操作异常: {str(e)}"
        logger.error(error_msg)
        send_wecom_message(error_msg)
        # 关键修改：出错立即终止，不再继续
        raise RuntimeError(error_msg) from None

def send_wecom_message(message):
    """发送消息到企业微信"""
    try:
        wecom_webhook = os.getenv('WECOM_WEBHOOK')
        if not wecom_webhook:
            error_msg = "【系统错误】企业微信Webhook URL未配置"
            logger.error(error_msg)
            return False
        
        # 添加页脚
        footer = Config.MESSAGE_FOOTER
        if footer:
            message += f"\n\n{footer}"
        
        payload = {
            "msgtype": "text",
            "text": {
                "content": message,
                "mentioned_list": ["@all"]
            }
        }
        
        response = requests.post(wecom_webhook, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.info("企业微信消息发送成功")
        return True
    except Exception as e:
        error_msg = f"【系统错误】企业微信消息发送失败: {str(e)}"
        logger.error(error_msg)
        return False
