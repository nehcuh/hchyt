import pandas as pd
import os
import sys

sys.path.append("../hchyt")

from utils import (
    gen_trade_calendar, 
    load_trade_cal, 
    get_real_trade_date, 
    get_pre_trade_date, 
    get_next_trade_date,
    fmt_symbols,
    get_trade_time_type
    )

def test_gen_trade_calendar():
    """
    测试本地保存交易日历 
    """
    # 1. 测试输入起始时间，结束时间
    gen_trade_calendar(start_date="2020-03-01", end_date="2023-01-01")
    assert os.path.exists(os.path.expanduser("~")+os.sep+".config"+os.sep+"trade_cal_20200301_20230101.pickle")
    # 2. 测试指定目录
    gen_trade_calendar(start_date="2020-03-01", end_date="2023-01-01", output="./test.pickle")
    assert os.path.exists("test.pickle")
    # 3. 测试默认参数
    gen_trade_calendar()
    assert os.path.exists(os.path.expanduser("~")+os.sep+".config"+os.sep+"trade_cal.pickle")

def test_load_trade_cal():
    """
    测试导入交易日历 
    """
    assert pd.Timestamp("2022-12-30") in load_trade_cal()

def test_get_real_trade_date():
    """
    测试获取真实交易日 
    """
    assert "2022-12-30" == get_real_trade_date("2022-12-31")
    assert "2023-01-03" == get_real_trade_date("2022-12-31", direction=1)
    # assert "2023-01-13" == get_real_trade_date()
    # assert "2023-01-16" == get_real_trade_date()

def test_get_pre_trade_date():
    """
    测试获取历史交易日 
    """
    assert "2023-01-13" == get_pre_trade_date("2023-01-15")
    assert "2023-01-13" == get_pre_trade_date("2023-01-13", inclusive=True)
    assert "2023-01-13" != get_pre_trade_date("2023-01-13", inclusive=False)
    assert "2023-01-13" != get_pre_trade_date("2023-01-13")
    assert "2023-01-12" == get_pre_trade_date("2023-01-15", n=2)

def test_get_next_trade_date():
    """
    测试获取未来交易日 
    """
    assert "2023-01-16" == get_next_trade_date("2023-01-15")
    assert "2023-01-16" == get_next_trade_date("2023-01-16", inclusive=True)
    assert "2023-01-17" == get_next_trade_date("2023-01-16", inclusive=False)
    assert "2023-01-16" == get_next_trade_date("2023-01-13")
    assert "2023-01-17" == get_next_trade_date("2023-01-13", n=2)
    assert "2023-01-16" == get_next_trade_date("2023-01-13", n=2, inclusive=True)

def test_fmt_symbols():
    """
    测试格式化标的代码函数 
    """
    assert "SZSE.000001" == fmt_symbols("SZ000001", 'gm')
    assert "000001.SZ" == fmt_symbols("000001", 'wd')
    assert "000001.XSHE" == fmt_symbols("SZ000001", 'jq')
    assert "600000.XSHG" == fmt_symbols("600000", 'jq')
    assert ["000001.SZ", "600000.SH"] == fmt_symbols(["SZ000001", 'SH600000'], 'wd')
    assert ["000001", "600000"] == fmt_symbols(["SZ000001", 'SH600000'])

def test_get_trade_time_type():
    """
    测试交易时间判断 
    """
    assert "others" == get_trade_time_type("2023-01-15 09:30:00")
    assert "continuous" == get_trade_time_type("2023-01-16 09:30:00")
    assert "auction1" == get_trade_time_type("2023-01-16 09:17:00")
    assert "auction2" == get_trade_time_type("2023-01-16 09:23:00")
    assert "auction3" == get_trade_time_type("2023-01-16 09:25:00")
    assert "auction4" == get_trade_time_type("2023-01-16 14:58:00")