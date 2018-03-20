import os
import time_utils as tu


OHLCV = {
    "directory": "raw/{period}/{pair}",
    "filename": "OHLCV",
    "fields": ["exchange", "month"]
}

INDICATOR = {
    "directory": "indicators/{period}/{pair}",
    "filename": "{class}",
    "fields": ["exchange", "month"]
}

WINDOWED_INDICATOR = {
    "directory": "indicators/{period}/{pair}",
    "filename": "{class}",
    "fields": ["exchange", "month", "window_size"]
}

COLUMN_STAT = {
    "directory": "indicators/{period}/{pair}",
    "filename": "{class}",
    "fields": ["exchange", "month", "input_column", "window_size"]
}


SIGNAL_THRESHOLDS = {
    "directory": "strategies/{period}/{pair}",
    "filename": "{class}",
    "fields": ["exchange", "date"]
}

SIMPLE_TURTLE_SIGNAL_THRESHOLDS = {
    "directory": "strategies/{period}/{pair}",
    "filename": "{class}",
    "fields": ["exchange", "date", "entry", "exit",
               "stop_loss_multiplier", "trailing_stop_multiplier"]
}

STUDY = {
    "directory": "studies",
    "filename": "{class}",
    "fields": ["markets", "start_date", "end_date", "start_balance",
               "risk_percentage"]
}


DEFINITIONS = {
    "OHLCV": OHLCV,
    "MaxInWindow": COLUMN_STAT,
    "MinInWindow": COLUMN_STAT,
    "TrueRange": INDICATOR,
    "AverageTrueRange": WINDOWED_INDICATOR,
    "EfficiencyRatio": WINDOWED_INDICATOR,
    "MovingAverage": WINDOWED_INDICATOR,
    "BuyAndHold": SIGNAL_THRESHOLDS,
    "SimpleTurtleSignalThresholds": SIMPLE_TURTLE_SIGNAL_THRESHOLDS,
    "Study": STUDY,
}

def adjust_month(month=None, period=None, **kargs):
    month = tu.month_from_str(month)
    result = "{:%Y-%m}".format(month)
    suffix = ""
    if tu.ongoing_month(month):
        suffix = "P{:%d_%H}".format(tu.latest_full_period(period))
    return "{}{}".format(result, suffix)

def adjust_date(date=None, period=None, **kargs):
    date = tu.date_from_str(date)
    result = "{:%Y-%m}".format(date)
    suffix = ""
    if tu.ongoing_month(date):
        suffix = "P{:%d_%H}".format(tu.latest_full_period(period))
    return "{}{}".format(result, suffix)


FILTERS = {
    "pair": lambda pair=None, **x: pair.replace("/", "-"),
    "markets": lambda markets=None, **x:
               os.path.basename(markets).split(".")[0],
    "month": adjust_month,
    "date": adjust_date
}



def path(definition, extension="csv", **kargs):
    filtered = {}
    for k, v in kargs.items():
        v = str(v)
        if k in FILTERS:
            v = FILTERS[k](**kargs)
        v = v.replace(".", "-").upper()
        v = v.replace("/", "-")
        filtered[k] = v

    arg_list = []
    for f in definition["fields"]:
        k = "-".join([v[:4] for v in f.split("_")]).upper()
        v = filtered[f]
        arg_list.append("{}_{}".format(k, v))
    args = "__".join(arg_list)
    directory = definition["directory"].format(**filtered)
    filename = definition["filename"].format(**filtered)
    filename = "{}__{}.{}".format(filename,
                                  args,
                                  extension)
    path = os.path.join(directory, filename)
    return path
