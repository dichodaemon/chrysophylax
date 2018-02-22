#!/usr/bin/python

import ccxt

symbols = set()

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
for e_name in ccxt.exchanges:
    if e_name in ["bitstamp", "cryptopia", "hitbtc", "gdax", "kraken",
                  "binance"]:
        exchange = ccxt.__dict__[e_name]()
        try:
            exchange.load_markets()
        except ValueError:
            pass
        if exchange.markets is not None:
            for m in exchange.markets:
                if "/" in m:
                    if m.split("/")[1] in ["USD", "BTC", "ETH"] \
                       and m.split("/")[0] in potential_portfolio:
                        symbols.add((e_name, m))
symbols = list(symbols)
symbols.sort()
print "exchange,pair"
for exchange, pair in symbols:
    print exchange, pair
