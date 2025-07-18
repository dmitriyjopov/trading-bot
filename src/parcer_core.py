import csv, os, time, logging, threading, signal, atexit
import websocket, json
from datetime import datetime, timezone
from collections import deque
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd



# Настройки
SYMBOL        = "BTCUSDT"
BUFFER_SIZE   = 100
FLUSH_INTERVAL= 2            # сек
DEPTH         = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- Буферизованный Parquet writer ---
class BufferedParquetWriter:
    def __init__(self, base_dir, subfolder, header, schema):
        self.base_dir = base_dir
        self.subfolder = subfolder
        self.current_date   = None
        self.current_writer = None
        self.current_path   = None
        self.header   = header
        self.buffer   = deque()
        self.lock     = threading.Lock()
        self.stop_evt = threading.Event()
        self.last_flush = time.time()
        # Схема из header:
        self.schema = schema
        self.thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.thread.start()

    def make_path(self, dt):
        d = dt.date()
        out_dir = os.path.join(
            os.path.expanduser(self.base_dir),
            self.subfolder,
            f"year={d.year}",
            f"month={d.month:02d}",
            f"day={d.day:02d}"
        )
        os.makedirs(out_dir, exist_ok=True)
        # фиксированное имя: один файл на день
        return os.path.join(out_dir, f"day={d}.parquet")

    def write_row(self, row):
        with self.lock:
            self.buffer.append(row)
            if len(self.buffer) >= BUFFER_SIZE:
                self._flush()

    def _flush(self):
        df = pd.DataFrame(self.buffer, columns=self.header)
        # ns -> ms
        df['recv_time'] = df['recv_time'].astype('datetime64[ms]')
        if 'timestamp' in df.columns:
            df['timestamp'] = df['timestamp'].astype('datetime64[ms]')

        # float64 -> float32, int64 -> int32
        df['price'] = df['price'].astype('float32')
        df['size'] = df['size'].astype('float32')
        if 'seq' in df.columns:
            df['seq'] = df['seq'].astype('int32')
        if 'u' in df.columns:
            df['u'] = df['u'].astype('int32')

        table = pa.Table.from_pandas(df, schema=self.schema)
        today = df['recv_time'].dt.date.iloc[0]

        # Если ещё нет writer-а или дата сменилась — создаём новый
        if self.current_writer is None or self.current_date != today:
            # Закрываем старый (если был)
            if self.current_writer is not None:
                self.current_writer.close()

            self.current_date = today
            self.current_path = self.make_path(df['recv_time'].iloc[0])

            self.current_writer = pq.ParquetWriter(
                self.current_path,
                self.schema,
                compression='zstd'
            )

        assert self.current_writer is not None, "ParquetWriter не инициализирован!"

        self.current_writer.write_table(table)
        self.buffer.clear()
        self.last_flush = time.time()

    def _flush_loop(self):
        while not self.stop_evt.is_set():
            time.sleep(1)
            if time.time() - self.last_flush >= FLUSH_INTERVAL:
                with self.lock:
                    if self.buffer:
                        self._flush()

    def flush(self):
        with self.lock:
            if self.buffer:
                self._flush()

    def close(self):
        self.stop_evt.set()
        self.thread.join(timeout=2)
        with self.lock:
            if self.buffer:
                self._flush()
        if self.current_writer is not None:
            self.current_writer.close()

# --- Глобальные объекты и очистка ---
ticks_schema = pa.schema([
    pa.field('recv_time', pa.timestamp('ns'), False),
    pa.field('timestamp', pa.timestamp('ns'), False),
    pa.field('symbol',    pa.string(),        False),
    pa.field('price',     pa.float64(),       False),
    pa.field('size',      pa.float64(),       False),
    pa.field('side',      pa.string(),        False),
])
ob_schema = pa.schema([
    pa.field('recv_time', pa.timestamp('ns'), False),
    pa.field('symbol',    pa.string(),        False),
    pa.field('type',      pa.string(),        False),
    pa.field('seq',       pa.int64(),         True),
    pa.field('u',         pa.int64(),         True),
    pa.field('side',      pa.string(),        False),
    pa.field('price',     pa.float64(),       False),
    pa.field('size',      pa.float64(),       False),
    pa.field('ts',        pa.int64(),         False),
])
def init_writers():
    ticks_writer = BufferedParquetWriter(
        base_dir="./data",  # сделай папку в проекте и вставь ее название сюда
        subfolder="ticks",
        header=["recv_time","timestamp","symbol","price","size","side"],
        schema=ticks_schema
    )
    ob_writer = BufferedParquetWriter(
        base_dir="./data",  # сделай папку в проекте и вставь ее название сюда
        subfolder="orderbook",
        header=["recv_time","symbol","type","seq","u","side","price","size","ts"],
        schema=ob_schema
    )

    return ticks_writer, ob_writer

ticks_writer, ob_writer = init_writers()
ws           = None
# last_msg_time= time.time() #добавить позже

def cleanup():
    logging.info("Exiting websocket...")
    if ws:
        try: ws.exit()
        except: pass

    # сначала пробуем flush без schema‑конфликтов
    try:
        ticks_writer.flush()
    except Exception as e:
        logging.error("ticks_writer.flush() failed: %s", e)
    try:
        ob_writer.flush()
    except Exception as e:
        logging.error("ob_writer.flush() failed: %s", e)

    # Закрываем writers
    try:
        ticks_writer.close()
    except Exception as e:
        logging.error("ticks_writer.close() failed: %s", e)
    try:
        ob_writer.close()
    except Exception as e:
        logging.error("ob_writer.close() failed: %s", e)

# --- Обработчик сообщений ---
def handle_msg(msg):
    global last_msg_time
    last_msg_time = time.time()  #добавить позже
    recv_dt = datetime.now(timezone.utc).replace(tzinfo=None) #убрал .isoformat()
    topic = msg.get("topic","")

    if topic == f"publicTrade.{SYMBOL}":
        for t in msg.get("data",[]):
            try:
                ts    = int(t["T"])
                price = float(t["p"])
                size  = float(t["v"])
                side  = t["S"]
            except:
                continue
            dt = datetime.fromtimestamp(ts/1000,timezone.utc).replace(tzinfo=None)
            ticks_writer.write_row([recv_dt, dt, SYMBOL, price, size, side])

    elif topic == f"orderbook.{DEPTH}.{SYMBOL}":
        typ  = msg.get("type","")
        data = msg.get("data",{}) or {}
        seq  = data.get("seq"); u = data.get("u"); ts_s = msg.get("ts") or msg.get("cts")
        for side_flag, items in (("Buy", data.get("b",[])), ("Sell", data.get("a",[]))):
            for price_, size_ in items:
                try:
                    price = float(price_); size = float(size_)
                    print(f'added: price: {price}, size: {size}')
                except:
                    continue
                ob_writer.write_row([recv_dt, SYMBOL, typ, seq, u, side_flag, price, size, ts_s])