#!/usr/bin/python

import ccxt

symbols = set()
for e_name in ccxt.exchanges:
    if e_name in ["bitstamp", "cryptopia", "hitbtc", "gdax", "kraken",
                  "binance"]:
        exchange = ccxt.__dict__[e_name]()
        try:
            exchange.load_markets()
            for m in exchange.markets.keys():
                symbols.add(m)
        except Exception:
            pass
symbols = list(symbols)
symbols.sort()
for s in symbols:
    print s
