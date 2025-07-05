from collections import deque, defaultdict
from datetime import datetime, timezone
from shlex import join
import pandas as pd
import math, pyarrow.parquet as pq
from tqdm import tqdm
'''
BUCKET_SIZE, WINDOW_LENGTH - ДЛЯ VPIN
'''
BUCKET_SIZE = 1000
WINDOW_LENGTH = 10

def read_parquet_with_progress(path, columns=None):
    df = pd.read_parquet(path, columns=columns)
    print(f"Loaded {len(df)} rows from {path}")
    return df

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

    return delta

def RF(va_df):
    rf = (va_df['VAH'] - va_df['POC_price']) / (va_df['POC_price'] - va_df['VAL'])
    return rf.rename('RF')

#Volume‑Synchronised Probability of Informed Trading    
def VPIN(df, period, bucket_size, window_length):

    df['signed_size'] = df['size'] * df['side'].map({'Buy':1, 'Sell':-1})

    df['cum_vol']   = df['size'].abs().cumsum()
    df['bucket_id'] = (df['cum_vol'] // bucket_size).astype(int)

    agg = df.groupby('bucket_id')['signed_size'].agg(
        buy_vol  = lambda x: x[x>0].sum(),
        sell_vol = lambda x: -x[x<0].sum()
    )
    agg['imbalance'] = (agg['buy_vol'] - agg['sell_vol']).abs()/bucket_size

    # присваиваем каждому тиковому времени imbalance текущего bucket_id
    df = df.join(agg['imbalance'], on='bucket_id')

    # ресемплируем по времени: усредняем imbalance за период
    return df.resample(period, on='recv_time')['imbalance'].mean().rename('VPIN')

def order_flow_imbalance(ob_df, period):
    """
    Рассчитывает Order Flow Imbalance (OFI) на основе данных стакана
    
    Параметры:
    ob_df (DataFrame): DataFrame с данными стакана
    period (str): частота группировки (например, '1h')
    
    Возвращает:
    Series: OFI для каждого временного интервала
    """
    if ob_df.empty:
        return pd.Series(dtype=float, name='OFI')
    
    # Подготовка данных
    ob_df['price'] = ob_df['price'].astype(float)
    ob_df['size'] = ob_df['size'].astype(float)
    ob_df['recv_time'] = pd.to_datetime(ob_df['recv_time'])
    
    # Сортировка по времени и идентификатору обновления (u)
    ob_df = ob_df.sort_values(['recv_time', 'u'])
    
    # Инициализация состояния
    current_bids = {}
    current_asks = {}
    ofi_accumulator = 0.0
    results = {}
    current_interval = None
    
    # Группировка по событиям (используем 'u' как идентификатор события)
    grouped = ob_df.groupby(['recv_time', 'u', 'type'])
    
    for (recv_time, u, event_type), group in grouped:
        # Определение начала интервала
        interval_start = recv_time.floor(period)
        
        # Инициализация интервала
        if current_interval is None:
            current_interval = interval_start
        
        # Сохранение результатов при смене интервала
        if interval_start != current_interval:
            results[current_interval] = ofi_accumulator
            ofi_accumulator = 0.0
            current_interval = interval_start
        
        # Обработка снимка стакана
        if event_type == 'snapshot':
            current_bids = {}
            current_asks = {}
            
            # Разделение на биды и аски
            bids = group[group['side'] == 'Buy']
            asks = group[group['side'] == 'Sell']
            
            #заполняет словари текущими значениями
            for _, row in bids.iterrows():
                current_bids[row['price']] = row['size']
                
            for _, row in asks.iterrows():
                current_asks[row['price']] = row['size']
        
        # Обработка изменений (delta)
        elif event_type == 'delta':
            for _, row in group.iterrows():
                price = row['price']
                size = row['size']
                side = row['side']
                
                if side == 'Buy':  # Изменение бида
                    old_size = current_bids.get(price, 0.0)
                    delta_v = old_size - size
                    
                    # Учитываем только уменьшение объема
                    if delta_v > 0:
                        ofi_accumulator -= delta_v * price
                    
                    # Обновление стакана
                    if size > 0:
                        current_bids[price] = size
                    elif price in current_bids:
                        del current_bids[price]
                        
                elif side == 'Sell':  # Изменение аска
                    old_size = current_asks.get(price, 0.0)
                    delta_v = old_size - size
                    
                    if delta_v > 0:
                        ofi_accumulator += delta_v * price
                    
                    if size > 0:
                        current_asks[price] = size
                    elif price in current_asks:
                        del current_asks[price]
    
    # Добавление последнего интервала
    if current_interval is not None:
        results[current_interval] = round(ofi_accumulator, 5)
    
    # Создание временного ряда
    ofi_series = pd.Series(results, name='OFI')
    ofi_series.index = pd.to_datetime(ofi_series.index)
    return ofi_series

def result_output(va_df, delta, vpin, rf, ofi):
    return pd.concat([va_df, delta, vpin, rf, ofi], axis=1)

def main():
    FILE_NAME = str(input("введите путь к файлу, нажмите [d] для дефолтного пути (data.csv)\n"))
    if FILE_NAME == "d":
        FILE_NAME = "/Users/a11111/trading-bot/data.parquet"
        OB_FILE_NAME = "/Users/a11111/trading-bot/orderbook.parquet"
    INPUT = str(input("введите шаг цены, нажмите [d] для дефолтного шага (0.5)\n"))
    if INPUT == "d":
        STEP = 0.5
    else:
        STEP = float(INPUT)
    PERIOD = str(input("введите частоту группировки, нажмите [d] для дефолтной частоты (1h)\n"))
    if PERIOD == "d":
        PERIOD = "1h"

    # Индикатор загрузки данных
    print("Загрузка данных...")
    df = read_parquet_with_progress(FILE_NAME)
    odf = read_parquet_with_progress(OB_FILE_NAME)

    odf['recv_time'] = pd.to_datetime(odf['recv_time'])

    print("Orderbook columns:", odf.columns.tolist())
    print(odf.head())

    print("✓ Данные загружены\n")

    # Список задач для отслеживания прогресса
    start_time = datetime.now()
    tasks = [
        ("Расчет Value Area", lambda: value_area(df, PERIOD, STEP)),
        ("Расчет Volume Delta", lambda: volume_delta(df, PERIOD)),
        ("Расчет VPIN", lambda: VPIN(df.copy(), PERIOD, BUCKET_SIZE, WINDOW_LENGTH)),
        # ("Расчет RF", lambda: RF(va_df)),
        ("Расчет Order Flow Imbalance", lambda: order_flow_imbalance(odf, PERIOD))
    ]
    end_time = datetime.now()
    print(f"Время выполнения: {end_time - start_time}")

    results = {}
    progress_bar = tqdm(tasks, desc="Прогресс расчета", unit="задача", dynamic_ncols=True)

    # Выполняем задачи с отображением прогресса
    for task_name, task_func in progress_bar:
        progress_bar.set_description(f"Выполняется {task_name}")
        try:
            result = task_func()
            results[task_name] = result
            progress_bar.set_postfix_str("✓ Успешно")
        except Exception as e:
            progress_bar.set_postfix_str(f"⚠ Ошибка: {str(e)}")
    
    # Рассчитываем RF после получения Value Area
    if "Расчет Value Area" in results:
        try:
            results["Расчет RF"] = RF(results["Расчет Value Area"])
            print("✓ RF рассчитан")
        except Exception as e:
            print(f"⚠ Ошибка при расчете RF: {str(e)}")
            results["Расчет RF"] = pd.Series(dtype=float, name='RF')

    # Извлекаем результаты
    va_df = results["Расчет Value Area"]
    delta = results["Расчет Volume Delta"]
    vpin = results["Расчет VPIN"]
    rf = results["Расчет RF"]
    ofi = results["Расчет Order Flow Imbalance"]

    # Дополнительные преобразования
    print("\nФинальные преобразования данных...")
    va_df = va_df.astype({
        'POC_price': float,
        'POC_volume': float,
        'VAH': float,
        'VAL': float
    })
    delta = delta.rename('volume_delta')
    delta.index.name = 'recv_time'

    # Сборка результатов
    print("Формирование итоговой таблицы...")
    results_df = pd.concat([va_df, delta, vpin, rf, ofi], axis=1)
    
    print("\n✓ Расчет завершен! Результаты:")
    print(results_df)


if __name__ == "__main__":
    main()