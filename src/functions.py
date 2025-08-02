from collections import deque, defaultdict
from datetime import datetime, timezone
from shlex import join
import pandas as pd
import math, pyarrow.parquet as pq, os, glob
from tqdm import tqdm
import numpy as np
from numba import njit, types
from numba.typed import Dict
'''
BUCKET_SIZE, WINDOW_LENGTH - ДЛЯ VPIN
'''
BUCKET_SIZE = 1000
WINDOW_LENGTH = 10

def read_parquet_by_day(base_dir, subfolder, start_date=None, end_date=None, columns=None, dtypes=None):
    """
    Построчно читает parquet-файлы по одному дню, сразу приводит к нужным типам и отбирает столбцы.
    Возвращает список DataFrame, по одному на каждый день.
    """
    pattern = os.path.join(base_dir, subfolder, "year=*", "month=*", "day=*", "day=*.parquet")
    files = sorted(glob.glob(pattern))
    out = []
    for file in files:
        # извлечь дату из имени
        day = os.path.basename(file).split('=')[1].replace('.parquet','')
        if start_date and day < start_date:   continue
        if end_date   and day > end_date:     continue

        # читаем ровно этот день
        df = pd.read_parquet(file, columns=columns)

        # приводим типы, если нужно
        if dtypes:
            for col, dtype in dtypes.items():
                df[col] = df[col].astype(dtype)

        out.append(df)
        del df   # сразу освобождаем после добавления
    return out

def cummulutive_delta(df, period):
    df['signed_size'] = df['size'] * df['side'].astype(str).map({'Buy':1,'Sell':-1})#размечаю с учетом знака
    
    bar_delta = (
        df.set_index('recv_time')['signed_size'].resample(period).sum().fillna(0)
    )

    return bar_delta.cumsum().rename('cum_delta')

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
    rf.iloc[0] = np.nan
    return rf.rename('RF').clip(upper = 10)

#Volume‑Synchronised Probability of Informed Trading    
def VPIN(df, period, bucket_size, window_length):

    df['signed_size'] = df['size'] * df['side'].astype(str).map({'Buy':1,'Sell':-1})

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

@njit
def _ofi_numba(ts_ns, typ, side, price, size, t0, period_ns, max_bins):
    # используем Numba‑dict для текущего стакана
    bids = Dict.empty(key_type=types.float64, value_type=types.float64)
    asks = Dict.empty(key_type=types.float64, value_type=types.float64)
    # ofi по бинам
    ofi = np.zeros(max_bins, dtype=np.float64)
    for i in range(ts_ns.shape[0]):
        t = ts_ns[i]
        bin_idx = (t - t0) // period_ns
        if typ[i] == 0:  # snapshot
            bids.clear()
            asks.clear()
        else:            # delta
            p = price[i]
            s = size[i]
            if side[i] == 0:  # Buy‑level
                old = bids.get(p, 0.0)
                dv  = old - s
                if dv > 0.0:
                    if dv > 0.0:
                        ofi[bin_idx] -= dv * price[i]
                if s > 0.0:
                    bids[p] = s
                else:
                    # удаляем, если size=0
                    if p in bids: del bids[p]
            else:             # Sell‑level
                old = asks.get(p, 0.0)
                dv  = old - s
                if dv > 0.0:
                    ofi[bin_idx] += dv * price[i]
                if s > 0.0:
                    asks[p] = s
                else:
                    if p in asks: del asks[p]
    return ofi

def process_order_flow_imbalance(odf: pd.DataFrame, period: str) -> pd.Series:
    #сортировка и подготовка
    df = odf.sort_values('recv_time')
    df['recv_time'] = pd.to_datetime(df['recv_time'])
    
    #базовая метка t0 и период
    t0 = df['recv_time'].dt.floor(period).astype('int64').iat[0]
    period_ns = pd.to_timedelta(period).value
    #вычисляем количество бинов
    t_last = df['recv_time'].astype('int64').iat[-1]
    max_bins = int((t_last - t0) // period_ns) + 1
    
    ts_ns   = df['recv_time'].astype('int64').to_numpy()
    typ     = (df['type']=='delta').to_numpy(np.int8)
    side    = (df['side']=='Sell').to_numpy(np.int8)
    price   = df['price'].to_numpy(np.float64)
    size    = df['size'].to_numpy(np.float64)
    
    ofi_vals = _ofi_numba(ts_ns, typ, side, price, size, t0, period_ns, max_bins)
    
    bin_times = pd.to_datetime(t0 + np.arange(max_bins, dtype=np.int64)*period_ns)
    return pd.Series(ofi_vals, index=bin_times, name='OFI')
#KYLE LAMBDA
def kyle_lambda_vectorized(prices, volumes, sides, timestamps, window_size_ns, first_ts):
    """
    Векторизованный расчет Kyle's Lambda с использованием Numba.
    
    Параметры:
        prices: np.array[float] - цены сделок
        volumes: np.array[float] - объемы сделок
        sides: np.array[int] - направления сделок (1 для покупок, -1 для продаж)
        timestamps: np.array[int64] - временные метки в наносекундах
        window_size_ns: int64 - размер окна в наносекундах
        
    Возвращает:
        tuple: (window_starts, lambda_values) - временные метки и значения λ
    """
    n = len(prices)
    if n < 2:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)
    
    #вычисляем знаковые объемы и изменения цен
    signed_volumes = volumes * sides
    price_changes = np.zeros(n, dtype=np.float64)
    price_changes[1:] = prices[1:] - prices[:-1]
    
    #определяем границы окон
    frst_ts = first_ts#справа та которую передали в функцию
    last_ts = timestamps[-1]
    n_windows = int((last_ts - frst_ts) // window_size_ns) + 1
    window_starts = frst_ts + np.arange(n_windows) * window_size_ns
    
    #инициализация результатов
    lambda_values = np.full(n_windows, np.nan, dtype=np.float64)
    
    for i in range(n_windows):
        window_start = window_starts[i]
        window_end = window_start + window_size_ns
        
        #находим сделки в текущем окне
        mask = (timestamps >= window_start) & (timestamps < window_end)
        if not mask.any():
            continue
            
        window_price_changes = price_changes[mask]
        window_signed_volumes = signed_volumes[mask]
        
        #удаляем NaN значения
        valid_mask = ~np.isnan(window_price_changes) & ~np.isnan(window_signed_volumes)
        window_price_changes = window_price_changes[valid_mask]
        window_signed_volumes = window_signed_volumes[valid_mask]
        
        if len(window_price_changes) < 2:
            continue
            
        #вычисляем ковариацию и дисперсию
        cov_matrix = np.cov(window_price_changes, window_signed_volumes)
        cov = cov_matrix[0, 1]
        var_volume = np.var(window_signed_volumes)
        
        if var_volume > 0:
            lambda_values[i] = cov / var_volume
            
    return window_starts, lambda_values

def vectorized_kyle_lambda(df, period='1h'):
    """
    Векторизованный расчет Kyle's Lambda для DataFrame.
    
    Параметры:
        df: pd.DataFrame - DataFrame с тиковыми данными
        period: str - период агрегации (например, '1h')
        
    Возвращает:
        pd.Series - ряд значений Kyle's Lambda с временным индексом
    """
    if df.empty:
        return pd.Series(dtype=float)
    
    #в numpy массивы
    timestamps = df['recv_time'].astype(np.int64).to_numpy()  # Преобразуем в наносекунды
    prices = df['price'].to_numpy()
    volumes = df['size'].to_numpy()
    sides = np.where(df['side'] == 'Buy', 1, -1)
    
    window_size_ns = pd.to_timedelta(period).value

    first_ts = df['recv_time'].dt.floor(period).astype('int64').iat[0]
    
    window_starts, lambda_values = kyle_lambda_vectorized(
        prices, volumes, sides, timestamps, window_size_ns, first_ts
    )
    
    index = pd.to_datetime(window_starts)
    return pd.Series(lambda_values, index=index, name='Kyle_Lambda')

def read_files_by_day(start_date, end_date):
    # первые – тики
    ticks_by_day = read_parquet_by_day(
        './data', 'ticks', start_date, end_date,
        columns=['recv_time','symbol','price','size','side'],
        dtypes={'price':'float32','size':'float32','side':'category'}
    )
    # потом стакан
    ob_by_day = read_parquet_by_day(
        './data', 'orderbook', start_date, end_date,
        columns=['recv_time','symbol','type','seq','side','price','size'],
        dtypes={'price':'float32','size':'float32','side':'category','type':'category'}
    )
    return ticks_by_day, ob_by_day

def result_output(va_df, delta, vpin, rf, ofi):
    return pd.concat([va_df, delta, vpin, rf, ofi], axis=1)

def main():
    INPUT = str(input("введите шаг цены, нажмите [d] для дефолтного шага (0.5)\n"))
    if INPUT == "d":
        STEP = 0.5
    else:
        STEP = float(INPUT)
    PERIOD = str(input("введите частоту группировки, нажмите [d] для дефолтной частоты (1h)\n"))
    if PERIOD == "d":
        PERIOD = "1h"

    #индикатор загрузки данныхˆ
    print("Введите диапазон дат в формате 01-01-2001 или введите [d] для анализа всех данных")
    if (input() == 'd'):
        start_date = "2025-07-12"
        end_date = "2025-07-17"
    else:
        start_date = str(input("Начальная дата: "))
        end_date = str(input("Конечная дата: "))

    print('Загрузка данных...')
    df, odf = read_files(start_date, end_date)

    odf['recv_time'] = pd.to_datetime(odf['recv_time'])

    print("✓ Данные загружены\n")

    #список задач для отслеживания прогресса
    start_time = datetime.now()
    tasks = [
        ("Расчет Cummulative Delta", lambda: cummulutive_delta(df.copy(), PERIOD)),
        ("Расчет Value Area", lambda: value_area(df.copy(), PERIOD, STEP)),
        ("Расчет Volume Delta", lambda: volume_delta(df.copy(), PERIOD)),
        ("Расчет VPIN", lambda: VPIN(df.copy(), PERIOD, BUCKET_SIZE, WINDOW_LENGTH)),
        ("Расчет Order Flow Imbalance", lambda: process_order_flow_imbalance(odf, PERIOD)),
        ("Расчет Kyle's Lambda", lambda: vectorized_kyle_lambda(df.copy(), PERIOD))
    ]

    results = {}
    task_times = {}
    progress_bar = tqdm(tasks, desc="Прогресс расчета", unit="задача", dynamic_ncols=True)

    for task_name, task_func in progress_bar:
        progress_bar.set_description(f"Выполняется {task_name}")
        try:
            t_start = datetime.now()
            result = task_func()
            t_end = datetime.now()
            elapsed = t_end - t_start
            results[task_name] = result
            task_times[task_name] = elapsed
            progress_bar.set_postfix_str(f"✓ Успешно за {elapsed}")
            print(f"{task_name} выполнена за {elapsed}")
        except Exception as e:
            progress_bar.set_postfix_str(f"⚠ Ошибка: {str(e)}")
            print(f"{task_name} ошибка: {str(e)}")

    end_time = datetime.now()
    
    if "Расчет Value Area" in results:
        try:
            results["Расчет RF"] = RF(results["Расчет Value Area"])
            print("✓ RF рассчитан")
        except Exception as e:
            print(f"⚠ Ошибка при расчете RF: {str(e)}")
            results["Расчет RF"] = pd.Series(dtype=float, name='RF')

    va_df = results["Расчет Value Area"]
    vol_delta = results["Расчет Volume Delta"]
    cum_delta = results["Расчет Cummulative Delta"]
    vpin = results["Расчет VPIN"]
    rf = results["Расчет RF"]
    ofi = results["Расчет Order Flow Imbalance"]
    kyle_lambda = results["Расчет Kyle's Lambda"]

    print("\nФинальные преобразования данных...")
    va_df = va_df.astype({
        'POC_price': float,
        'POC_volume': float,
        'VAH': float,
        'VAL': float
    })
    vol_delta = vol_delta.rename('volume_delta')
    vol_delta.index.name = 'recv_time'
    cum_delta = cum_delta.rename('cum_delta')
    cum_delta.index.name = 'recv_time'

    print("Формирование итоговой таблицы...")
    results_df = pd.concat([va_df, vol_delta, cum_delta, vpin, rf, ofi, kyle_lambda], axis=1)

    print("\n✓ Расчет завершен! Результаты:")
    print(results_df)

    hourly_close = (df.copy()
        .set_index('recv_time')['price']
        .resample('1h')
        .last()
        .shift(-1)  # «закрытие» следующего часа
        .rename('next_close')
    )

    check = results_df[['POC_price','RF']].join(hourly_close)

    check['move_pct'] = (check['next_close'] - check['POC_price']) / check['POC_price'] * 100

    print(check)

    print(f"Время выполнения расчета: {end_time - start_time}")
    print(f"\nВремя выполнения по задачам:")
    for task_name, elapsed in task_times.items():
        print(f"{task_name}: {elapsed}")
    
    return results_df
    

if __name__ == "__main__":
    main()