import matplotlib.dates as mdates


def configure_date_axis(axis, period):
    axis.tick_params(axis='y', which='major', labelsize=10)
    axis.tick_params(axis='x', which='major', labelsize=9, labelcolor="k",
                     pad=10)
    axis.tick_params(axis='x', which='minor', labelsize=8, labelcolor="k")
    axis.xaxis_date()
    if period == "1d":
        max_locator = mdates.WeekdayLocator(byweekday=0)
        max_formatter = mdates.DateFormatter("%m-%d-%Y")
        min_locator = mdates.WeekdayLocator(byweekday=[3])
        min_formatter = mdates.DateFormatter("%d")
    elif period == "1h":
        max_locator = mdates.DayLocator()
        max_formatter = mdates.DateFormatter("%m-%d\n%Y")
        min_locator = mdates.HourLocator(byhour=[6, 12, 18])
        min_formatter = mdates.DateFormatter("%H:00")
    elif period == "4h":
        max_locator = mdates.DayLocator()
        max_formatter = mdates.DateFormatter("%m-%d\n%Y")
        min_locator = mdates.HourLocator(byhour=[12])
        min_formatter = mdates.DateFormatter("%H:00")
    axis.xaxis.set_major_locator(max_locator)
    axis.xaxis.set_major_formatter(max_formatter)
    axis.xaxis.set_minor_locator(min_locator)
    axis.xaxis.set_minor_formatter(min_formatter)
