import alpaca_trade_api as tradeapi
import pandas as pd
import plotly.express as px
from config import *

STOCKS_SYMBOLS = ["TSLA", "NIO", "PLTR", "NFLX", "ZM", "FB", "GOOG", "MSFT"]

alpaca = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_API_URL)
barsets = alpaca.get_barset(STOCKS_SYMBOLS, "day", limit=1000)  # .df

results = []

for symbol in barsets:
    print(f"processing symbol {symbol}")
    for bar in barsets[symbol]:
        results.append(dict())
        results[-1]["symbol"] = symbol
        results[-1]["date"] = bar.t.date()
        results[-1]["open"] = bar.o
        results[-1]["high"] = bar.h
        results[-1]["low"] = bar.l
        results[-1]["close"] = bar.c
        results[-1]["volume"] = bar.v

results = pd.DataFrame(results)

results["symbol"] = results["symbol"].astype("str")
results["date"] = results["date"].astype("datetime64")

results.to_csv("data/barsets.csv", index=False)
# results = pd.read_csv("data/barsets.csv")

results["lag"] = results.groupby(["symbol"]).shift(1).close

df_aggregated = (
    results.assign(returns=lambda x: (x["close"] / x["lag"] - 1))
    .query("date > '2021-01-01'")
    .groupby("symbol")
    .aggregate({"returns": ["mean", "std", "count"], "date": ["max", "min"]})
    .reset_index()
)

df_aggregated.columns = ["_".join([a, b]) for a, b in df_aggregated.columns]

df_aggregated = (
    df_aggregated.rename(columns={"symbol_": "symbol"})
    # .query("returns_count > 365")
    # .query("returns_count < 1000")
    .assign(reward_metric=lambda x: x["returns_mean"] / x["returns_std"] + 100)
)

df_aggregated.pipe(
    func=px.scatter,
    x="returns_std",
    y="returns_mean",
    color="reward_metric",
    hover_data=["symbol"],
    render_mode="svg",
    template="plotly_dark",
)

results.pipe(
    func=px.line,
    x="date",
    y="close",
    color="symbol",
    facet_col="symbol",
    facet_col_wrap=2,
    render_mode="svg",
    template="plotly_dark",
).update_yaxes(matches=None, title=None).update_xaxes(title=None).update_layout(
    showlegend=True, font=dict(size=12)
).update_traces(
    line=dict(width=1)
)
