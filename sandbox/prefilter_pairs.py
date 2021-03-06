#!/usr/bin/python

import ccxt
import pandas as pd
import sys


potential_portfolio = [
    "BTC", "ETH",
    "ADA", "BCC", "BCH", "XRP", "LTC", "XLM",
    "NEO", "EOS", "MIOTA", "DASH", "XEM", "XMR",
    "LSC", "ETC", "TRX", "QTUM", "VEN", "BTG",
    "ICX", "OMG", "ZEC", "XRB", "STEEM", "BNB",
    "BCN", "PPT", "STRAT", "XVG", "SC", "DOGE",
    "SNT", "RHOC", "BTS", "MKR", "WAVES", "VERI",
    "AE", "REP", "WTC", "HSR", "KMD", "UCASH",
    "DCR", "ZCL", "ZRX", "KCS", "ARDR", "R",
    "ARK", "DRGN", "GAS", "BAT"
]

exchanges = ["kraken", "gdax", "binance", "bitstamp"]
pairs = set()
symbols = set()

for e_name in exchanges:
    exchange = ccxt.__dict__[e_name]()
    try:
        exchange.load_markets()
    except ValueError:
        pass
    if exchange.markets is not None:
        for m in exchange.markets:
            if "/" in m:
                if not m in pairs:
                    if m.split("/")[1] in ["USD", "BTC", "ETH"] \
                       and m.split("/")[0] in potential_portfolio:
                        symbols.add((e_name, m))
                        pairs.add(m)
symbols = list(symbols)
symbols.sort()
result = []
for exchange, pair in symbols:
    result.append({"exchange": exchange, "pair":pair})
result = pd.DataFrame(result)
result.to_csv(sys.argv[1])
