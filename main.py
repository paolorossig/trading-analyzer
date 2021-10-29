import alpaca_trade_api as tradeapi
import pandas as pd
import plotly.express as px
from config import *

alpaca = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_API_URL)

assets = alpaca.list_assets()

df_stocks = []

for asset in assets:
    if asset.status == "active" and asset.tradable and asset.symbol:
        print(f"Added a new stock {asset.symbol} {asset.name}")
        df_stocks.append(
            {
                "id": asset.id,
                "symbol": asset.symbol,
                "name": asset.name,
                "exchange": asset.exchange,
            }
        )

df_stocks = pd.DataFrame(df_stocks)
df_stocks.to_csv("data/symbols.csv", index=False)


STOCKS_SYMBOLS = df_stocks.query("exchange == 'NASDAQ'")["symbol"].tolist()

results = []
chunk_size = 100

for i in range(0, len(STOCKS_SYMBOLS), chunk_size):
    barsets = alpaca.get_barset(STOCKS_SYMBOLS[i : i + chunk_size], "day", limit=1000)

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
results.to_csv("data/barsets.csv", index=False)

results["symbol"] = results["symbol"].astype("str")
results["date"] = results["date"].astype("datetime64")


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
    .query("returns_count > 180")
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

best_stocks = (
    df_aggregated.sort_values("reward_metric", ascending=False).head(10).symbol.tolist()
)

results.set_index("symbol").loc[best_stocks, :].reset_index().pipe(
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
