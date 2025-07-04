import csv, os, time, logging, threading, signal, atexit
from datetime import datetime, timezone
from collections import deque
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# Настройки
SYMBOL        = "BTCUSDT"
TICKS_FILE    = os.path.expanduser("data.parquet")
OB_FILE       = os.path.expanduser("orderbook.parquet")
MSG_TIMEOUT   = 30           # сек
BUFFER_SIZE   = 100
FLUSH_INTERVAL= 2            # сек
DEPTH         = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- Буферизованный Parquet writer ---
class BufferedParquetWriter:
    def __init__(self, filename, header):
        self.filename = filename
        self.header   = header
        self.buffer   = deque()
        self.lock     = threading.Lock()
        self.last_flush = time.time()
        self.stop_evt = threading.Event()
        self._init_file()
        self.thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.thread.start()

    def _init_file(self):
        os.makedirs(os.path.dirname(self.filename) or ".", exist_ok=True)
        # Если файл не существует, создаём новый пустой DataFrame и сохраняем его как Parquet
        if not os.path.exists(self.filename) or os.path.getsize(self.filename) == 0:
            df = pd.DataFrame(columns=self.header)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, self.filename)

    def write_row(self, row):
        with self.lock:
            self.buffer.append(row)
            if len(self.buffer) >= BUFFER_SIZE:
                self._flush()

    def _flush(self):
        # Преобразуем буфер в DataFrame и записываем его в файл Parquet
        df = pd.DataFrame(self.buffer, columns=self.header)
        table = pa.Table.from_pandas(df)
        with open(self.filename, 'ab') as f:  # 'ab' — append binary mode
            pq.write_table(table, f)
        self.buffer.clear()  # Очистить буфер
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
        self.flush()

# --- Глобальные объекты и очистка ---
ticks_writer = BufferedParquetWriter(TICKS_FILE, ["recv_time","timestamp","symbol","price","size","side"])
ob_writer    = BufferedParquetWriter(OB_FILE,    ["recv_time","symbol","type","seq","u","side","price","size","ts"])
ws           = None
last_msg_time= time.time()

def cleanup():
    logging.info("Cleaning up...")
    if ws:
        try: ws.exit()
        except: pass
    ticks_writer.close()
    ob_writer.close()

# --- Обработчик сообщений ---
def handle_msg(msg):
    global last_msg_time
    last_msg_time = time.time()  # <<< обновляем таймер при любом сообщении
    recv_dt = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    topic   = msg.get("topic","")

    if topic == f"publicTrade.{SYMBOL}":
        for t in msg.get("data",[]):
            try:
                ts    = int(t["T"])
                price = float(t["p"])
                size  = float(t["v"])
                side  = t["S"]
            except:
                continue
            dt = datetime.fromtimestamp(ts/1000,timezone.utc).replace(tzinfo=None).isoformat()
            ticks_writer.write_row([recv_dt, dt, SYMBOL, price, size, side])

    elif topic == f"orderbook.{DEPTH}.{SYMBOL}":
        typ  = msg.get("type","")
        data = msg.get("data",{}) or {}
        seq  = data.get("seq"); u = data.get("u"); ts_s = msg.get("ts") or msg.get("cts")
        for side_flag, items in (("Buy", data.get("b",[])), ("Sell", data.get("a",[]))):
            for price_, size_ in items:
                try:
                    price = float(price_); size = float(size_)
                except:
                    continue
                ob_writer.write_row([recv_dt, SYMBOL, typ, seq, u, side_flag, price, size, ts_s])

# --- Основной reconnect-цикл ---

# while True:
#     try:
#         # 1) Создаём WS и подписываемся
#         ws = WebSocket(testnet=False, channel_type="linear")
#         ws.trade_stream(symbol=SYMBOL,         callback=handle_message)
#         ws.orderbook_stream(symbol=SYMBOL,     depth=DEPTH, callback=handle_message)

#         # Сразу после подписки сбрасываем таймер и backoff
#         last_msg_time = time.time()
#         backoff       = 1

#         # 2) Мониторим таймаут ping/pong
#         stop_evt = threading.Event()
#         def monitor():
#             logging.info("Monitor started")
#             while not stop_evt.is_set():
#                 if time.time() - last_msg_time > MSG_TIMEOUT:
#                     logging.warning("No messages for %s s, restarting WS", MSG_TIMEOUT)
#                     try: 
#                         if ws: ws.exit()
#                     except: pass
#                     stop_evt.set()
#                     break
#                 time.sleep(1)
#             logging.info("Monitor exiting")

#         mon_t = threading.Thread(target=monitor, daemon=True)
#         mon_t.start()

#         # 3) Ждём сигнала от монитора
#         logging.info("Main waiting...")
#         while not stop_evt.is_set():
#             time.sleep(1)
#         mon_t.join(timeout=2)

#     except KeyboardInterrupt:
#         logging.info("Interrupted by user")
#         cleanup()
#         break

#     except Exception:
#         logging.exception("Error, reconnecting in %s s", backoff)
#         ticks_writer.flush()
#         ob_writer.flush()
#         time.sleep(backoff)
#         backoff = min(backoff*2, 60)
#         continue
