import pandas as pd
import os
from .utils import gen_trade_calendar, load_trade_cal, get_real_trade_date, get_pre_trade_date, get_next_trade_date

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