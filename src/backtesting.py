from collections import deque, defaultdict
from datetime import datetime, timezone
import pandas as pd
import time, os, csv

FILE_NAME = "data.csv"
STEP = 0.5
  
def POC():
    df = pd.read_csv(FILE_NAME, parse_dates=['recv_time'], delimiter=',')
    df["bucket"] = (df["price"] / STEP).round() * STEP#создаю бакеты
    df["volume"] = df["size"]
    grp = df.groupby([pd.Grouper(key='recv_time', freq='1h'), 'bucket']).agg(total_volume=('volume', 'sum')).reset_index()
    idx = grp.groupby("recv_time")["total_volume"].idxmax()
    poc_per_hour = grp.loc[idx, ["recv_time", "bucket", 'total_volume']].set_index("recv_time")
    return poc_per_hour

print(POC())