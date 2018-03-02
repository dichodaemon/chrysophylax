import datetime
import pandas as pd
import pytz
import os
import time

PERIODS = {
    "1d": 1,
    "1h": 24,
    "4h": 6
}


def date2ts(date):
    return int(time.mktime(date.timetuple()) * 1e3)


def required_months(month, window_size, period):
    days = window_size / PERIODS[period]
    months = days / 28 + 2
    m = month
    for i in xrange(months):
        yield m
        m = previous_month(m)

def ongoing_month(dt):
    today = pytz.datetime.datetime.utcnow().date()
    return today.year == dt.year and today.month == dt.month

def previous_month(dt0):
    dt1 = dt0.replace(day=1)
    dt2 = dt1 - datetime.timedelta(days=1)
    dt3 = dt2.replace(day=1)
    return dt3


def full_months(start_date, end_date):
    current = previous_month(start_date)
    end_month = next_month(end_date)
    while True:
        current = next_month(current)
        if current >= end_month:
            break
        yield current


def next_month(dt0, inclusive=True):
    dt1 = (dt0.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
    if inclusive:
        return dt1
    return dt1 - datetime.timedelta(days=1)

def months(start_d, end_d):
    m1 = next_month(previous_month(start_d))
    m2 = next_month(end_d)
    while m1 < m2:
        yield m1
        m1 = next_month(m1)

def task_type(task):
    result = [type(task)]
    for p in task.param_args:
        if not isinstance(p, datetime.date):
            result.append(p)
    return tuple(result)


def input_df(task_list):
    data = {}
    for r in task_list:
        t = task_type(r)
        if t not in data:
            data[t] = []
        new_data = pd.read_csv(r.target.path, index_col=0,
                               parse_dates=True)
        data[t].append(new_data)
    result = None
    for k, v in data.items():
        new_data = pd.concat(v)
        if result is None:
            result = new_data
        else:
            result = pd.merge(result, new_data,
                              left_index=True, right_index=True)
    result.sort_index(inplace=True)
    return result

def monthly_task(t_params, task_class, start_date, end_date, **kargs):
    for k, v in kargs.items():
        t_params[k] = "{}".format(v)
    for m in full_months(start_date, end_date):
        t_params["month"] = "{:%Y-%m}".format(m)
        yield task_class.from_str_params(t_params)


def task_filename(task, ext, suffix=None, exclude=[]):
    always_exclude = ["pair", "period", "destination_path"]
    always_exclude.extend(exclude)
    keys = task.__class__.get_param_names()
    for key in always_exclude:
        keys.remove(key)
    params = ["{}_{}".format(k, task.to_str_params()[k]) for k in keys]
    params = "{}__{}".format(task.__class__.__name__, "__".join(params)).upper()
    params = params.replace("/", "-")
    params = params.replace(".", "-")
    if suffix is not None:
        result = "{}-{}.{}".format(params, suffix, ext)
    else:
        result = "{}.{}".format(params, ext)
    return os.path.join(task.period.upper(),
                        task.pair.upper().replace("/", "-"), result)
