import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
df_call = pd.read_csv("_option_back_0_call.csv")
df_put = pd.read_csv("_option_back_0_put.csv")
df_contract = pd.read_csv("_option_back_0.csv")
df_settlement = pd.read_csv("settlement_price.csv")
trading_table = pd.DataFrame()

df_contract = df_contract[df_contract["Time"] == 150100]
df_contract = df_contract.drop(["Time", "H", "L", "C", "Volume"], axis=1)
df_contract.columns = ["Date", "Contract_price"]

df_call = df_call[df_call["Time"] == 150100]
df_call = df_call.drop(["Time", "H", "L", "C", "Volume"], axis=1)
df_call.columns = ["Date", "Entryprice"]
df_call["c_or_p"] = "C"
df_call = df_call.merge(df_contract, on="Date")
df_call["Break_even_point"] = df_call["Contract_price"] + df_call["Entryprice"]


df_put = df_put[df_put["Time"] == 150100]
df_put = df_put.drop(["Time", "H", "L", "C", "Volume"], axis=1)
df_put.columns = ["Date", "Entryprice"]
df_put["c_or_p"] = "P"
df_put = df_put.merge(df_contract, on="Date")
df_put["Break_even_point"] = df_put["Contract_price"] - df_put["Entryprice"]

trading_table = df_call.append(df_put)
trading_table = trading_table.sort_values(["Date", "c_or_p"])
trading_table.reset_index(inplace=True)

df_settlement.columns = ["Date", "Contract", "Settlement_price"]
df_settlement["Date"] = df_settlement["Date"].apply(lambda x: x.replace('/', ''))
df_settlement["Date"] = df_settlement["Date"].astype(int)

row_iter = df_settlement.iterrows()
for idx, row in row_iter:
    trading_table.loc[(trading_table["Date"] < row[0]), "Contract"] = row[1]
    trading_table.loc[(trading_table["Date"] < row[0]), "Settlement_price"] = row[2]

trading_table.drop(["index"], inplace=True, axis=1)
trading_table.dropna(inplace=True)
trading_table = trading_table[["Date", "Contract", "Contract_price", "c_or_p", "Entryprice", "Break_even_point", "Settlement_price"]]

trading_table.loc[(trading_table["c_or_p"] == "C"), "Profit_point"] = trading_table["Break_even_point"] - trading_table["Settlement_price"]
trading_table.loc[(trading_table["c_or_p"] == "P"), "Profit_point"] = trading_table["Settlement_price"] - trading_table["Break_even_point"]

trading_table = trading_table[trading_table["c_or_p"] == "P"]

trading_table["Win_lose"] = trading_table["Profit_point"].apply(lambda x: 'W' if x >= 0 else 'L')
trading_table["Profit_point"] = trading_table.apply(lambda x: x["Profit_point"] if x["Entryprice"] >= x["Profit_point"] else x["Entryprice"], axis=1)
trading_table["Profit"] = trading_table["Profit_point"].apply(lambda x: x * 50)
trading_table["Cumulative_profit"] = trading_table["Profit"].cumsum().round(1)
trading_table["HighValue"] = trading_table["Cumulative_profit"].cummax()
trading_table["Drawdown"] = trading_table["Cumulative_profit"] - trading_table["HighValue"]

trading_table.drop(["HighValue"], axis=1, inplace=True)
# trading_table = trading_table.sort_values(["Cumulative_profit"])

win_num = trading_table[trading_table["Win_lose"] == "W"].shape[0]
lose_num = trading_table[trading_table["Win_lose"] == "L"].shape[0]
largest_win = trading_table[trading_table["Win_lose"] == "W"]["Profit"].max()
largest_lose = trading_table[trading_table["Win_lose"] == "L"]["Profit"].min()
max_drawdown = trading_table["Drawdown"].min()


print(f"Number Winning Trades: {win_num}")
print(f"Number Losing Trades: {lose_num}")
print(f"Percent Profitable: {(win_num / (win_num + lose_num)): .2f}")
print(f"Largest Winning Trade: {largest_win}")
print(f"Largest Losing Trade: {largest_lose}")
print(f"Max Drawdown: {max_drawdown}")

profit = list(trading_table["Cumulative_profit"].apply(lambda x: x + 100000))
drawdown = list(trading_table["Drawdown"])
idx = list(trading_table.index)

x = np.arange(len(idx))
fig, axes = plt.subplots(nrows=2, sharex=True)
axes[0].plot(x, profit)
axes[1].fill_between(x, 0, drawdown, color='red')
plt.suptitle("ONLY SELL PUT")
plt.show()
# trading_table.to_csv("temp.csv")
# fig.savefig("performance.png")

