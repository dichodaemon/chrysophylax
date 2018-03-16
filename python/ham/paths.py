import os
import time_utils as tu


OHLCV = {
    "directory": "raw/{period}/{pair}",
    "filename": "OHLCV",
    "fields": ["exchange", "month"]
}


def adjust_month(month=None, period=None, **kargs):
    month = tu.month_from_str(month)
    result = "{:%Y-%m}".format(month)
    suffix = ""
    if tu.ongoing_month(month):
        suffix = "P{:%d_%H}".format(tu.latest_full_period(period))
    return "{}{}".format(result, suffix)


FILTERS = {
    "pair": lambda pair=None, **x: pair.replace("/", "-"),
    "markets": lambda markets=None, **x: os.basename(markets).split(".")[0],
    "month": adjust_month
}


def path(definition, extension="csv", **kargs):
    filtered = {}
    for k, v in kargs.items():
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
    filename = "{}__{}.{}".format(definition["filename"],
                                  args,
                                  extension)
    path = os.path.join(directory, filename)
    return path
