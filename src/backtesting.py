from collections import deque, defaultdict
from datetime import datetime, timezone
from shlex import join
import pandas as pd
import math, pyarrow.parquet as pq, os, glob
from tqdm import tqdm
import numpy as np
from numba import njit
# import keyboard
'''
BUCKET_SIZE, WINDOW_LENGTH - ДЛЯ VPIN
'''
BUCKET_SIZE = 1000
WINDOW_LENGTH = 10

def read_parquet_range(base_dir, subfolder, start_date=None, end_date=None):
    """
    Читает все parquet-файлы из поддиректории (ticks/orderbook) за указанный диапазон дат.
    Если даты не указаны — читает все файлы.
    """
    pattern = os.path.join(
        base_dir, subfolder, "year=*", "month=*", "day=*", "day=*.parquet"
    )
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files found in {pattern}")
    
    dfs = []
    for file in files:
        fname = os.path.basename(file)
        date_str = fname.replace("day=", "").replace(".parquet", "")
        if start_date or end_date:
            file_date = pd.to_datetime(date_str)
            if start_date and file_date < pd.to_datetime(start_date):
                continue
            if end_date and file_date > pd.to_datetime(end_date):
                continue
        dfs.append(pd.read_parquet(file))
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

def cummulutive_delta(df, period):
    df['signed_size'] = df['size'] * df['side'].map({'Buy':1, 'Sell':-1})#размечаю с учетом знака
    
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

@njit
def _ofi_numba(ts_ns, typ, side, price, size, t0, period_ns, max_bins):
    # используем Numba‑dict для текущего стакана
    bids = dict()
    asks = dict()
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
    # 1) Сортировка и подготовка
    df = odf.sort_values('recv_time')
    df['recv_time'] = pd.to_datetime(df['recv_time'])
    
    # Базовая метка t0 и период
    t0 = df['recv_time'].view('int64').iat[0]
    period_ns = pd.to_timedelta(period).value
    # Вычисляем количество бинов
    t_last = df['recv_time'].view('int64').iat[-1]
    max_bins = int((t_last - t0) // period_ns) + 1
    
    # 2) К массивам
    ts_ns   = df['recv_time'].view('int64').to_numpy()
    typ     = (df['type']=='delta').to_numpy(np.int8)
    side    = (df['side']=='Sell').to_numpy(np.int8)
    price   = df['price'].to_numpy(np.float64)
    size    = df['size'].to_numpy(np.float64)
    
    # 3) Запуск Numba‑функции
    ofi_vals = _ofi_numba(ts_ns, typ, side, price, size, t0, period_ns, max_bins)
    
    # 4) Время каждой бины
    bin_times = pd.to_datetime(t0 + np.arange(max_bins, dtype=np.int64)*period_ns)
    return pd.Series(ofi_vals, index=bin_times, name='OFI')


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

    # Индикатор загрузки данныхˆ
    print("Введите диапазон дат в формате 01-01-2001 или введите [d] для анализа всех данных")
    if (input() == 'd'):
        start_date = None
        end_date = None
    else:
        start_date = str(input("Начальная дата: "))
        end_date = str(input("Конечная дата: "))

    print('Загрузка данных...')
    df = read_parquet_range('./data', 'ticks', start_date, end_date)
    odf = read_parquet_range('./data', 'orderbook', start_date, end_date)

    odf['recv_time'] = pd.to_datetime(odf['recv_time'])

    print("✓ Данные загружены\n")

    # Список задач для отслеживания прогресса
    start_time = datetime.now()
    tasks = [
        ("Расчет Cummulative Delta", lambda: cummulutive_delta(df.copy(), PERIOD)),
        ("Расчет Value Area", lambda: value_area(df.copy(), PERIOD, STEP)),
        ("Расчет Volume Delta", lambda: volume_delta(df.copy(), PERIOD)),
        ("Расчет VPIN", lambda: VPIN(df.copy(), PERIOD, BUCKET_SIZE, WINDOW_LENGTH)),
        # ("Расчет RF", lambda: RF(va_df)),
        ("Расчет Order Flow Imbalance", lambda: process_order_flow_imbalance(odf, PERIOD))
    ]

    results = {}
    task_times = {}
    progress_bar = tqdm(tasks, desc="Прогресс расчета", unit="задача", dynamic_ncols=True)

    # Выполняем задачи с отображением прогресса
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
    vol_delta = results["Расчет Volume Delta"]
    cum_delta = results["Расчет Cummulative Delta"]
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
    vol_delta = vol_delta.rename('volume_delta')
    vol_delta.index.name = 'recv_time'
    cum_delta = cum_delta.rename('cum_delta')
    cum_delta.index.name = 'recv_time'

    # Сборка результатов
    print("Формирование итоговой таблицы...")
    results_df = pd.concat([va_df, vol_delta, cum_delta, vpin, rf, ofi], axis=1)
    
    print("\n✓ Расчет завершен! Результаты:")
    print(results_df)

    print(f"Время выполнения расчета: {end_time - start_time}")
    print(f"\nВремя выполнения по задачам:")
    for task_name, elapsed in task_times.items():
        print(f"{task_name}: {elapsed}")


if __name__ == "__main__":
    main()