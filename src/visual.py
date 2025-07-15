import matplotlib.pyplot as plt
import functions
import numpy as np

# class trade():
#     def __init__(self, open_pos, close_pos, sum, direction):
#         self.open_pos = open_pos
#         self.close_pos = close_pos
#         self.sum = sum
#         self.direction = direction

#     def execute(self):
#         if open_pos == True:

def calculate_indicators_and_plot(period='1h'):
    indicators_df = functions.main()
    df, odf = functions.read_files("2025-07-12", "2025-07-12")

    '''вход-выход из позиций'''
    rf = indicators_df['RF']
    vpin = indicators_df['VPIN']
    ofi = indicators_df['OFI']
    kl = indicators_df['Kyle_Lambda']

    #условия входов в позиции
    buy_rf     = rf <  0.5
    sell_rf    = rf >  2.0

    buy_vpin   = vpin < 0.1
    sell_vpin  = vpin > 0.3

    buy_ofi    = ofi > 0
    sell_ofi   = ofi < 0

    buy_kl     = kl <  0.05
    sell_kl    = kl >  0.2

    open_pos_long = buy_rf & buy_ofi
    close_pos_long = sell_rf & sell_ofi 

    open_pos_short = sell_rf & sell_ofi
    close_pos_sell = buy_rf & buy_ofi

    #открытие и закрытие позиций
    longs_open  = indicators_df.index[ open_pos_long.fillna(False) ]
    longs_close = indicators_df.index[ close_pos_long.fillna(False) ]

    price_series = df.set_index('recv_time')['price']
    open_prices  = price_series.reindex(longs_open,  method='nearest').values
    close_prices = price_series.reindex(longs_close, method='nearest').values

    trades = []
    i = j = 0
    while i < len(longs_open) and j < len(longs_close):
        t_open  = longs_open[i]
        t_close = longs_close[j]
        if t_close <= t_open:
            j += 1
            continue

        entry_price = open_prices[i]
        exit_price  = close_prices[j]
        profit      = (exit_price - entry_price)
        trades.append({
            'entry_time' : t_open,
            'exit_time'  : t_close,
            'entry_price': entry_price,
            'exit_price' : exit_price,
            'profit'     : profit,
            'return'     : profit / entry_price
        })

        i += 1
        j += 1

    #расчет доходности
    deposit = 10_000
    risk = 0.01
    equity = [deposit]
    times  = [trades[0]['entry_time'] if trades else df['recv_time'].iloc[0]]

    for tr in trades:
        ret = (tr['exit_price'] - tr['entry_price']) / tr['entry_price']
        pnl = equity[-1] * risk * ret
        equity.append(equity[-1] + pnl)
        times.append(tr['exit_time'])

    # 5) Расчёт buy&hold
    bh_price = price_series
    bh_equity = deposit * (bh_price / bh_price.iloc[0])

    # 6) Графики
    fig, ax = plt.subplots(2, 1, figsize=(12, 8), sharex=True,
                           gridspec_kw={'height_ratios': [3, 1]})


    '''графики'''

    indicators = [
        'volume_delta', 'cum_delta',
        'VPIN', 'RF', 'OFI', "Kyle_Lambda"
    ]
    price_metrics = [
        'POC_price', 'VAH', 'VAL',
    ]

    indicators = [col for col in indicators if col in indicators_df.columns]
    price_metrics = [c for c in price_metrics if c in indicators_df.columns]

    n_plots = 1 + len(indicators)
    fig, axes = plt.subplots(
        n_plots, 1,
        figsize=(12, 3*n_plots),
        sharex=True,
        gridspec_kw={'height_ratios': [5] + [1]*len(indicators)},
        constrained_layout=True
    )
    axes = axes if isinstance(axes, (list, np.ndarray)) else [axes]

    #цена + poc, vah, val
    ax0 = axes[0]
    ax0.plot(df['recv_time'], df['price'], label='Price', linewidth=1)
    for col in price_metrics:
        ax0.plot(indicators_df.index, indicators_df[col], label=col, linewidth=1.2)

    ax0.scatter(longs_open,  open_prices,  marker='^', color='g', s=60, label='Buy')
    ax0.scatter(longs_close, close_prices, marker='v', color='r', s=60, label='Sell')

    ax0.set_ylabel("Price / Profile")
    ax0.legend(loc='upper left')
    ax0.grid(False)

    #инликаторы
    for i, col in enumerate(indicators, start=1):
        ax = axes[i]
        ax.plot(indicators_df.index, indicators_df[col], label=col, linewidth=1)
        ax.set_ylabel(col)
        ax.legend(loc='upper left')
        ax.grid(True)

    #единая ось времени внизу
    axes[-1].set_xlabel("Time")
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    calculate_indicators_and_plot()
