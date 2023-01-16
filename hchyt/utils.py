import os
from functools import cache
import re
import datetime
import time
import toml
from typing import Union, List, Tuple
import datetime
import pickle
import tushare as ts
import pandas as pd

def gen_trade_calendar(
    start_date: Union[str, pd.Timestamp, datetime.date]=None, 
    end_date: Union[str, pd.Timestamp, datetime.date] = None,
    output: str=None,
    local_config: str=None) -> None:
    """
    生成交易日期的二进制文件, 
    1. 使用 local_config 中的 tushare 账户信息
    2. 入参没有指定 local_config, 会在用户主目录下的 ".config" 寻找 user_info.toml 文件
    3. 配置文件格式需要为 toml 格式，填入 tushare 的 token 信息，格式如下：
    ```
    [tushare]
    token = xxx
    ```

    Args:
        start_date: 起始时间，默认从 "1990-01-01" 开始进行查询
        end_date: 结束时间，默认为当前所属年份 12-31
        output: 交易日历本地文件保存地址，没有指定时，
            - 如果 end_date 指定时，会保存为 "~/.config/trade_cal_{start_date}_{end_date}.pickle",
            - 如果 end_date 为 None 时，会保存为 "~/.config/trade_cal.pickle"
        local_config: 本地 tushare 账户配置信息，没有指定，默认寻找 "~/.config/user_info.toml"
    Returns:
        None 
    """
    user_info = dict()

    # 1. 寻找 tushare 配置信息
    if not local_config:
        ts_configuration = os.path.expanduser("~") + os.sep + ".config" + os.sep + "user_info.toml" 
        if not os.path.exists(ts_configuration):
            raise ValueError(f"[ERROR]\t没有找到本地用户配置信息，配置地址为 {ts_configuration}")
    else:
        if not os.path.exists(local_config):
            raise ValueError(f"[ERROR]\t没有找到本地用户配置信息，配置地址为 {ts_configuration}")
        ts_configuration = local_config 

    # 2. 导入配置信息
    try:
        with open(ts_configuration, "r") as f:
            user_info = toml.load(f)
        if "tushare" not in user_info:
            raise ValueError
    except:
        raise ValueError(f"[ERROR]\t在本地用户配置 {ts_configuration} 信息中没有找到 tushare 信息")

    # 3. 设置输出文件
    if not output:
        if (not start_date) and (not end_date):
            end_date = str(datetime.date.today().year) + "1231"
            output = os.path.expanduser("~") + os.sep + ".config" + os.sep + "trade_cal.pickle"
        else:
            if not start_date:
                start_date = "19900101"
            else:
                start_date = pd.Timestamp(start_date).strftime("%Y%m%d")
            if not end_date:
                end_date = str(datetime.date.today().year) + "1231"
            else:
                end_date = pd.Timestamp(end_date).strftime("%Y%m%d")
            output = os.path.expanduser("~") + os.sep + ".config" + os.sep + f"trade_cal_{start_date}_{end_date}.pickle"

    # 4. 设置 tushare 账户信息
    ts.set_token(user_info["tushare"]["token"])
    pro = ts.pro_api()

    # 5. 查询交易日历，并以 list 形式保存到本地
    df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
    df.loc[:, 'cal_date'] = pd.to_datetime(df['cal_date'])
    if df.empty:
        try:
            time.sleep(seconds=60)
            df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
        except:
            if df.empty:
                raise ValueError("[ERRROR]\t从 tushare 查询交易日期，返回 None")

    with open(output, "wb") as f:
        pickle.dump(sorted(list(set(df.loc[df.is_open == 1, 'cal_date'].to_list()))), f)


@cache
def load_trade_cal(local_cal: str=None) -> List[str]:
    """
    导入交易日历，本地日历文件不存在则进行网络全量获取

    Args:
        local_cal: 本地日历文件，默认为 ~/.config/trade_cal.pickle
    """
    if not local_cal:
        local_cal = os.path.expanduser("~") + os.sep + ".config" + os.sep + "trade_cal.pickle"

    if not os.path.exists(local_cal):
        gen_trade_calendar(output=local_cal)
    
    with open(local_cal, "rb") as f:
        return pickle.load(f)


def get_real_trade_date(
    cursor_date: Union[str, datetime.date, pd.Timestamp] = None, 
    direction: int=-1,
    trade_cal_file: str=None) -> str:
    """
    获取指定日期附近真正的交易日
    - 如果指定日期为交易日则返回指定日期，否则根据指定方向查询真实股票交易日
    - 如果没有指定日期，默认以当日作为输入，往历史回溯作为方向

    Args:
        cursor_date: 指定日期
        direction: 查询方式，默认为 -1， 即往历史回溯
        trade_cal_file: 交易日历存放地址，默认为 None，从用户主目录下的 ".config/trade_cal.pickle" 读取
    """
    # 1. 输入日期格式化处理
    if not cursor_date:
        cursor_date = pd.Timestamp(datetime.date.today())
    else:
        cursor_date = pd.Timestamp(pd.Timestamp(cursor_date).date())

    # 2. 交易日历获取
    if not trade_cal_file:
        trade_calendar = load_trade_cal()
    else:
        trade_calendar = load_trade_cal(trade_cal_file)

    # 3. 交易日期查询
    if cursor_date in trade_calendar:
        return pd.Timestamp(cursor_date).strftime("%Y-%m-%d")
    else:
        if (cursor_date < trade_calendar[0]) or (cursor_date > trade_calendar[-1]):
            raise ValueError(f"[ERROR]\t交易日期 {cursor_date} 小于交易日历最小值 {trade_calendar[0]} 或大于交易日历最大值 {trade_calendar[-1]}")
        elif direction == -1:
            while cursor_date not in trade_calendar:
                cursor_date = cursor_date - pd.Timedelta(days=1)
                if cursor_date in trade_calendar:
                    return cursor_date.strftime('%Y-%m-%d')
                elif cursor_date < trade_calendar[0]:
                    raise ValueError(f"[ERROR]\t交易日期 {cursor_date} 小于交易日历最小值 {trade_calendar[0]}")
        elif direction == 1:
            while cursor_date not in trade_calendar:
               cursor_date = cursor_date + pd.Timedelta(days=1)
               if cursor_date in trade_calendar:
                   return cursor_date.strftime('%Y-%m-%d')
               elif cursor_date > trade_calendar[-1]:
                   raise ValueError(f"[ERROR]\t交易日期 {cursor_date} 大于交易日历最小值 {trade_calendar[-1]}")
       

def get_pre_trade_date(
    cursor_date: Union[str, pd.Timestamp, datetime.date]=None, 
    n: int=1,
    inclusive: bool=False):
    """
    获取指定日期回溯 N 日的历史交易日，当不指定日期时，从当前日期往历史回溯，注意：
    - 当 inclusive 为 True 的时候，如果 cursor_date 为交易日，往前回溯一个交易日即为 cursor_date
    - 当 inclusive 为 False 的时候
      - 如果 cursor_date 为交易日，往前回溯一个交易日为 cursor_date 往历史继续回溯一个交易日
      - 如果 cursor_date 为非交易日，往前回溯一个交易日即为往历史看，距离当前最近的交易日

    Args:
        cursor_date: 指定日期，默认为空，即当前日期往历史回溯 (默认不包含当前日期)
        n: 往历史回溯的交易日数目，注意 n 必须为正整数
        inclusive: 当 cursor_date 为交易日的时候，是否包含 cursor_date

    Return:
        "YYYY-mm-dd" 格式的交易日
    """
    assert isinstance(n, int) and n > 0
    # 1. 获取交易日历
    trade_calendar = load_trade_cal()

    # 2. 处理 cursor_date
    if not cursor_date:
        cursor_date = pd.Timestamp(datetime.date.today())
    else:
        cursor_date = pd.Timestamp(pd.Timestamp(cursor_date).date())

    if (cursor_date in trade_calendar) and inclusive:
        return trade_calendar[trade_calendar.index(cursor_date) - n + 1].strftime("%Y-%m-%d")
    elif (cursor_date in trade_calendar):
        return trade_calendar[trade_calendar.index(cursor_date) - n].strftime("%Y-%m-%d")
    else:
        return trade_calendar[trade_calendar.index(pd.Timestamp(get_real_trade_date(cursor_date))) - n + 1].strftime("%Y-%m-%d")


def get_next_trade_date(
    cursor_date: Union[str, pd.Timestamp, datetime.date]=None, 
    n: int=1,
    inclusive: bool=False):
    """
    获取指定日期未来 N 日的交易日，当不指定日期时，从当前日期往未来推演，注意：
    - 当 inclusive 为 True 的时候，如果 cursor_date 为交易日，往前推演一个交易日即为 cursor_date
    - 当 inclusive 为 False 的时候
      - 如果 cursor_date 为交易日，往前推演一个交易日为 cursor_date 往未来继续推演一个交易日
      - 如果 cursor_date 为非交易日，往前推演一个交易日即为往未来看，距离当前最近的交易日

    Args:
        cursor_date: 指定日期，默认为空，即当前日期往未来推演 (默认不包含当前日期)
        n: 往未来推演的交易日数目，注意 n 必须为正整数
        inclusive: 当 cursor_date 为交易日的时候，是否包含 cursor_date

    Return:
        "YYYY-mm-dd" 格式的交易日
    """
    assert isinstance(n, int) and n > 0
    # 1. 获取交易日历
    trade_calendar = load_trade_cal()

    # 2. 处理 cursor_date
    if not cursor_date:
        cursor_date = pd.Timestamp(datetime.date.today())
    else:
        cursor_date = pd.Timestamp(pd.Timestamp(cursor_date).date())

    if (cursor_date in trade_calendar) and inclusive:
        return trade_calendar[trade_calendar.index(cursor_date) + n - 1].strftime("%Y-%m-%d")
    elif (cursor_date in trade_calendar):
        return trade_calendar[trade_calendar.index(cursor_date) + n].strftime("%Y-%m-%d")
    else:
        return trade_calendar[trade_calendar.index(pd.Timestamp(get_real_trade_date(cursor_date, direction=1))) + n - 1].strftime("%Y-%m-%d")


def fmt_symbols(
    symbols: Union[str, List[str], Tuple[str]],
    style: str=None
) -> Union[str, List[str]]:
    """
    根据输入的股票或股票列表，和相应需要格式化的风格，实现标的代码的格式化
    注意：
        1. 如果输入的是单个标的，即 str 格式，返回的是 str，其他情况返回格式化标的列表
        2. 只支持沪深交所，不支持北交所

    Args:
        symbols: 标的代码或标的代码列表
        style: 标的编码风格，默认为 None，即只输出标的数字编号，支持 
            - 'gm' 或 'goldminer'(掘金), 
            - 'ts' 或 'tushare' (tushare)
            - 'jq' 或 'joinquant' (聚宽), 
            - 'wd' 或 'wind' (万得)

    Returns:
        格式化后的标的代码或标的列表
    """
    # 1. 编码中的数字规则
    digit_pat = re.compile(r"\d+")

    # 2. 处理输入标的编码
    output_str_flag = False
    if isinstance(symbols, str):
        symbols = [symbols]
        output_str_flag = True
    symbols = ",".join(symbols)

    # 3. 搜索所有数字编码
    digit_symbols = re.findall(digit_pat, symbols)

    # 4. 根据指定风格处理标的编码
    if not style:
        symbols = digit_symbols
    elif style in ['gm', 'goldminer']:
        symbols = list(map(lambda x: "SHSE." + x if x[0] == '6' else "SZSE."+x, digit_symbols))
    elif style in ['ts', 'tushare', 'wd', 'wind']:
        symbols = list(map(lambda x: x+".SH" if x[0] == '6' else x+".SZ", digit_symbols))
    elif style in ['jq', 'joinquant']:
        symbols = list(map(lambda x: x+".XSHG" if x[0] == '6' else x+".XSHE", digit_symbols))
    else:
        raise ValueError("[ERROR]\t不支持的编码风格")

    # 5. 返回结果
    if output_str_flag:
        return symbols[0]
    return symbols 
    

def get_trade_time_type(
    cursor_time: Union[str, datetime.datetime, datetime.time, pd.Timestamp] = None,
) -> str:
    """
    查询指定时间所处交易时间段
        - "auction1": 9:15 ~ 9:20 可挂可撤
        - "auction2": 9:20 ~ 9:25 可挂不可撤
        - "auction3": 9:25 ~ 9:30 接收挂单，实际开盘价在 9:25
        - "auction4": 14:57 ~ 15:00 可挂不可撤
        - "continuous": 9:30 ~ 11:30/13:00 ~ 14:57 连续竞价
        - "others": 其他非交易时间

    Args:
        time_str: 指定时间, 默认为当前时间

    Returns:
        str: 当前时间所处交易时间段
    """
    specific_times = [
        datetime.time(9, 15), # 0
        datetime.time(9, 20), # 1
        datetime.time(9, 25), # 2
        datetime.time(9, 30), # 3
        datetime.time(11, 30), # 4
        datetime.time(13, 00), # 5
        datetime.time(14, 57), # 6
        datetime.time(15, 0), # 7
    ]
    # 1. 指定时间格式化处理
    if not cursor_time:
        cursor_time = datetime.datetime.now().time()
        cursor_date = pd.Timestamp(datetime.date.today()) 
    elif isinstance(cursor_time, datetime.time):
        cursor_date = pd.Timestamp(datetime.date.today())
    else:
        cursor_time = pd.Timestamp(cursor_time) 
        cursor_date = pd.Timestamp(cursor_time.date())
        cursor_time = cursor_time.time()

    # 2. 判断时间所处时间段
    trade_calendar = load_trade_cal()

    if (
        (cursor_date not in trade_calendar) 
        or 
        (cursor_time < specific_times[0]) 
        or 
        (cursor_time > specific_times[-1])
        or
        (cursor_time > specific_times[4] and cursor_time < specific_times[5])
    ):
        return "others"
    elif (cursor_time >= specific_times[0] and cursor_time < specific_times[1]):
        # 9:15 ~ 9:20
        return "auction1"
    elif (cursor_time >= specific_times[1] and cursor_time < specific_times[2]):
        # 9:20 ~ 9:25
        return "auction2"
    elif (cursor_time >= specific_times[2] and cursor_time < specific_times[3]):
        # 9:25 ~ 9:30
        return "auction3"
    elif (
        (cursor_time >= specific_times[3] and cursor_time < specific_times[4])
        or
        (cursor_time >= specific_times[5] and cursor_time < specific_times[6])
    ):
        # 9:30 ~ 11:30
        # 13:00 ~ 14:57
        return "continuous"
    elif (cursor_time >= specific_times[6] and cursor_time < specific_times[7]):
        # 14:57 ~ 15:00
        return "auction4" 
    else:
        return "others"