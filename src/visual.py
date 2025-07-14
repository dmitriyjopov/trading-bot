import matplotlib.pyplot as plt
import functions

def plot_indicators(period='1h'):
    indicators_df = functions.main()
    df, odf = functions.read_files("2025-07-12", "2025-07-12")

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
        gridspec_kw={
            'height_ratios': [5, 1, 1, 1, 1, 1, 1]  # Относительные высоты графиков (пример для 3 графиков)
        },
        constrained_layout = True # Автоматическая подгонка отступов
    )
    if n_plots == 1:
        axes = [axes]

    #цена + poc, vah, val
    ax0 = axes[0]
    ax0.plot(df['recv_time'], df['price'], label='Price', linewidth=1)
    for col in price_metrics:
        ax0.plot(indicators_df.index, indicators_df[col], label=col, linewidth=1.2)
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
    plot_indicators()
