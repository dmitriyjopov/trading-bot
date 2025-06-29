from collections import deque, defaultdict
from datetime import datetime, timezone
import pandas as pd
import time, os, csv

FILE_NAME = "data.csv"
STEP = 0.5

df = pd.read_csv(FILE_NAME, parse_dates=['recv_time'], delimiter=',')
  
def POC(df):
    df["bucket"] = (df["price"] / STEP).round() * STEP#создаю бакеты
    df["volume"] = df["size"]
    grp = df.groupby([pd.Grouper(key='recv_time', freq='1h'), 'bucket']).agg(total_volume=('volume', 'sum')).reset_index()
    idx = grp.groupby("recv_time")["total_volume"].idxmax()
    poc_per_hour = grp.loc[idx, ["recv_time", "bucket", 'total_volume']].set_index("recv_time")
    return poc_per_hour

print(POC(df))

def value_area(df):
    df["bucket"] = (df["price"] / STEP).round() * STEP  # создаю бакеты
    df["volume"] = df["size"]
    
    grp = df.groupby([pd.Grouper(key='recv_time', freq='1h'), 'bucket']).agg(total_volume=('volume', 'sum')).reset_index()
    grp = grp.sort_values('bucket')
    
    total_volume = grp['total_volume'].sum()
    target_volume = total_volume * 0.7
    
    # Находим POC (Point of Control) - бакет с максимальным объемом
    poc_bucket = grp.loc[grp['total_volume'].idxmax(), 'bucket']
    
    # Инициализируем переменные для Value Area
    va_high = poc_bucket
    va_low = poc_bucket
    current_volume = grp[grp['bucket'] == poc_bucket]['total_volume'].iloc[0]
    
    # Индексы POC в отсортированном DataFrame
    poc_idx = grp[grp['bucket'] == poc_bucket].index[0]
    
    # Расширяем Value Area вверх и вниз от POC
    upper_idx = poc_idx + 1
    lower_idx = poc_idx - 1
    
    while current_volume < target_volume:
        upper_volume = 0
        lower_volume = 0
        
        # Проверяем объем выше
        if upper_idx < len(grp):
            upper_volume = grp.iloc[upper_idx]['total_volume']
        
        # Проверяем объем ниже
        if lower_idx >= 0:
            lower_volume = grp.iloc[lower_idx]['total_volume']
        
        # Выбираем направление с большим объемом
        if upper_volume >= lower_volume and upper_volume > 0:
            current_volume += upper_volume
            va_high = grp.iloc[upper_idx]['bucket']
            upper_idx += 1
        elif lower_volume > 0:
            current_volume += lower_volume
            va_low = grp.iloc[lower_idx]['bucket']
            lower_idx -= 1
        else:
            break  # Больше нет данных для расширения
    
    # Вычисляем объемы в Value Area и за её пределами
    va_volume = current_volume
    above_va_volume = grp[grp['bucket'] > va_high]['total_volume'].sum()
    below_va_volume = grp[grp['bucket'] < va_low]['total_volume'].sum()
    
    # Создаем результат
    result = {
        'poc_price': poc_bucket,
        'va_high': va_high,
        'va_low': va_low,
        'va_volume': va_volume,
        'va_volume_pct': (va_volume / total_volume) * 100,
        'above_va_volume': above_va_volume,
        'below_va_volume': below_va_volume,
        'total_volume': total_volume
    }
    
    return result

print("\n" + "="*50)
print("VALUE AREA ANALYSIS")
print("="*50)
va_result = value_area(df)
print(f"POC Price: {va_result['poc_price']}")
print(f"Value Area High: {va_result['va_high']}")
print(f"Value Area Low: {va_result['va_low']}")
print(f"Value Area Volume: {va_result['va_volume']:.2f} ({va_result['va_volume_pct']:.1f}%)")
print(f"Volume Above VA: {va_result['above_va_volume']:.2f}")
print(f"Volume Below VA: {va_result['below_va_volume']:.2f}")
print(f"Total Volume: {va_result['total_volume']:.2f}")
    