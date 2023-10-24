# This script offers functions to calculate the `alias` of future products.
from datetime import datetime, timedelta
BASE_DATE = datetime(2022, 12, 30, 0, 0)
EXP_BIAS = timedelta(hours=16) # ONLY expire at 16:00 at that day.
WEEKLY_DELTA = timedelta(days=7)
QUARTERLY_DELTA = timedelta(days=91)


def is_this_week(milli_ts: int, date_s: str) -> bool:
    date_format = "%y%m%d"
    exp_date = datetime.strptime(date_s, date_format) + EXP_BIAS
    current_date = datetime.fromtimestamp(milli_ts)
    if not (exp_date > current_date > BASE_DATE):
        raise Exception(f'exp_date {exp_date} > current_date {current_date} > BASE_DATE {BASE_DATE}')
    if exp_date - current_date > WEEKLY_DELTA:
        return False
    else:
        return True

def is_next_week(milli_ts: int, date_s: str) -> bool:
    date_format = "%y%m%d"
    exp_date = datetime.strptime(date_s, date_format) + EXP_BIAS
    current_date = datetime.fromtimestamp(milli_ts)
    assert exp_date > current_date > BASE_DATE
    if 2*WEEKLY_DELTA >= exp_date - current_date > 1*WEEKLY_DELTA:
        return True
    else:
        return False

def is_this_quarter(milli_ts: int, date_s: str) -> bool:
    date_format = "%y%m%d"
    exp_date = datetime.strptime(date_s, date_format) + EXP_BIAS
    current_date = datetime.fromtimestamp(milli_ts)
    assert exp_date > current_date > BASE_DATE
    if exp_date - current_date <= QUARTERLY_DELTA \
        and (exp_date-BASE_DATE)%QUARTERLY_DELTA == timedelta(days=0):
        return True
    else:
        return False

def is_next_quarter(milli_ts: int, date_s: str) -> bool:
    date_format = "%y%m%d"
    exp_date = datetime.strptime(date_s, date_format) + EXP_BIAS
    current_date = datetime.fromtimestamp(milli_ts)
    assert exp_date > current_date > BASE_DATE
    if 1*QUARTERLY_DELTA < exp_date - current_date <= 2*QUARTERLY_DELTA \
        and (exp_date-BASE_DATE)%QUARTERLY_DELTA == timedelta(days=0):
        return True
    else:
        return False

