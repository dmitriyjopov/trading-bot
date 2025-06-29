from collections import deque, defaultdict
from datetime import datetime, timezone
import pandas as pd

FILE_NAME = "data.csv"
STEP = 0.5#шаг цены
PERIOD = "1h"#частота группировки

df = pd.read_csv(FILE_NAME, parse_dates=['recv_time'], delimiter=',')
  
def POC(df, period):
    df["bucket"] = (df["price"] / STEP).round() * STEP#создаю бакеты
    df["volume"] = df["size"]
    grp = df.groupby([pd.Grouper(key='recv_time', freq=period), 'bucket']).agg(total_volume=('volume', 'sum')).reset_index()
    idx = grp.groupby("recv_time")["total_volume"].idxmax()
    poc_per_hour = grp.loc[idx, ["recv_time", "bucket", 'total_volume']].set_index("recv_time")

    return poc_per_hour

def value_area(df, period):
    df = df.copy()
    df["bucket"] = (df["price"] / STEP).round() * STEP
    df["volume"] = df["size"]
    
    grp = df.groupby([pd.Grouper(key='recv_time', freq=period), 'bucket']).agg(
        total_volume=('volume', 'sum')
    ).reset_index()
    
    results = []
    for time_key, group in grp.groupby('recv_time'):
        group = group.sort_values('bucket').reset_index(drop=True)

        if group.empty:
            results.append({'recv_time': time_key, 'VAH': None, 'VAL': None})
            continue

        total_vol = group['total_volume'].sum()
        target_vol = total_vol * 0.7
        
        #поиск POC
        poc_idx = group['total_volume'].idxmax()
        current_vol = group.at[poc_idx, 'total_volume']
        low_idx = poc_idx - 1
        high_idx = poc_idx + 1
        VAH = VAL = group.at[poc_idx, 'bucket']
        
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
                
        results.append({'recv_time': time_key, 'VAH': VAH, 'VAL': VAL})

    va_df = pd.DataFrame(results)
    va_df.set_index('recv_time', inplace=True)
    return va_df
   
def main():
    poc_df = POC(df.copy(), PERIOD)
    va_df = value_area(df.copy(), PERIOD)
    
    # Объединяем результаты
    result_df = pd.concat([poc_df, va_df], axis=1)
    result_df.columns = ['POC_price', 'POC_volume', 'VAH', 'VAL']
    
    # Конвертируем numpy-типы в стандартные float
    result_df = result_df.astype({
        'POC_price': float,
        'POC_volume': float,
        'VAH': float,
        'VAL': float
    })
    
    print("Результаты анализа:")
    print(result_df)
    
    # Для вывода последней записи
    last_record = result_df.iloc[-1]
    print("\nПоследний период:")
    print(f"POC: {last_record['POC_price']} (объем: {last_record['POC_volume']})")
    print(f"VAH: {last_record['VAH']}, VAL: {last_record['VAL']}")

if __name__ == "__main__":
    main()