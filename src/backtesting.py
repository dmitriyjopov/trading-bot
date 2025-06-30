from collections import deque, defaultdict
from datetime import datetime, timezone
import pandas as pd

# FILE_NAME = "data.csv"
# STEP = 0.5#шаг цены
# PERIOD = "1h"#частота группировки

# df = pd.read_csv(FILE_NAME, parse_dates=['recv_time'], delimiter=',')

#VAH, VAL, POC
def value_area(df, period, step):
    df["bucket"] = (df["price"] / step).round() * step
    df["volume"] = df["size"]
    
    grp = df.groupby([pd.Grouper(key='recv_time', freq=period), 'bucket']).agg(
        total_volume=('volume', 'sum')
    ).reset_index()
    
    results = []
    for time_key, group in grp.groupby('recv_time'):
        group = group.sort_values('bucket').reset_index(drop=True)

        if group.empty:
            results.append({'recv_time': time_key, 'POC_price': None, 'POC_volume': None, 'VAH': None, 'VAL': None})
            continue

        total_vol = group['total_volume'].sum()
        target_vol = total_vol * 0.7
        
        # поиск POC
        poc_idx = group['total_volume'].idxmax()
        poc_price = group.at[poc_idx, 'bucket']
        poc_volume = group.at[poc_idx, 'total_volume']
        current_vol = poc_volume
        low_idx = poc_idx - 1
        high_idx = poc_idx + 1
        VAH = VAL = poc_price
        
        while current_vol < target_vol and (low_idx >= 0 or high_idx < len(group)):
            vol_low = group.at[low_idx, 'total_volume'] if low_idx >= 0 else -1
            vol_high = group.at[high_idx, 'total_volume'] if high_idx < len(group) else -1
            
            if vol_high >= vol_low and vol_high != -1:
                VAH = group.at[high_idx, 'bucket']
                current_vol += vol_high
                high_idx += 1
            elif vol_low != -1:
                VAL = group.at[low_idx, 'bucket']
                current_vol += vol_low
                low_idx -= 1
            else:
                break
                
        results.append({'recv_time': time_key, 'POC_price': poc_price, 'POC_volume': poc_volume, 'VAH': VAH, 'VAL': VAL})

    va_df = pd.DataFrame(results)
    va_df.set_index('recv_time', inplace=True)
    return va_df

#дельта объема
def volume_delta(df, period):
    if df.empty:
        return pd.Series(dtype=float)
    
    vol_df = df.pivot_table(
        index=pd.Grouper(key='recv_time', freq=period),
        columns='side',
        values='size',
        aggfunc='sum',
        fill_value=0
    )
    
    delta = vol_df.get('Buy') - vol_df.get('Sell')

    return delta.iloc[0]
    


def main():
    FILE_NAME = str(input("введите путь к файлу, нажмите [d] для дефолтного пути (data.csv)\n"))
    if FILE_NAME == "d":
        FILE_NAME = "data.csv"
    INPUT = str(input("введите шаг цены, нажмите [d] для дефолтного шага (0.5)\n"))
    if INPUT == "d":
        STEP = 0.5
    else:
        STEP = float(INPUT)
    PERIOD = str(input("введите частоту группировки, нажмите [d] для дефолтной частоты (1h)\n"))
    if PERIOD == "d":
        PERIOD = "1h"

    df = pd.read_csv(FILE_NAME, parse_dates=['recv_time'], delimiter=',')

    #ФОРМАТИРОВАНИЯ ВЫВОДА НАЧАЛО
    va_df = value_area(df.copy(), PERIOD, STEP)
    va_df = va_df.astype({
        'POC_price': float,
        'POC_volume': float,
        'VAH': float,
        'VAL': float
    })
    print("Результаты анализа:")
    print(va_df)
    
    # Для вывода последней записи
    last_record = va_df.iloc[-1]
    print("\nПоследний период:")
    print(f"POC: {last_record['POC_price']} (объем: {last_record['POC_volume']})")
    print(f"VAH: {last_record['VAH']}, VAL: {last_record['VAL']}")
    print(f"Дельта объема: {volume_delta(df.copy(), PERIOD)}")
    
    #ФОРМАТИРОВАНИЯ ВЫВОДА КОНЕЦ

if __name__ == "__main__":
    main()