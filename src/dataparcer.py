from pybit.unified_trading import WebSocket
import csv, os, time, logging, threading, sys, signal, atexit
from datetime import datetime, timezone
from io import StringIO
from collections import deque

os.system("")# иногда помогает
# Убираем проблемные строки с reconfigure
# sys.stdout.reconfigure(encoding="utf-8")
# sys.stderr.reconfigure(encoding="utf-8")

SYMBOL = "BTCUSDT"
TICKS_FILE = os.path.expanduser("data.csv")
OB_FILE = os.path.expanduser("orderbook.csv")
MSG_TIMEOUT =  30

# Настройки буферизации
BUFFER_SIZE = 100  # Уменьшаем размер буфера для более частой записи
FLUSH_INTERVAL = 2  # Уменьшаем интервал для более частой записи

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Глобальные переменные для writers
ticks_writer = None
ob_writer = None
ws = None

# Класс для буферизованной записи в CSV
class BufferedCSVWriter:
    def __init__(self, filename, header, buffer_size=BUFFER_SIZE, flush_interval=FLUSH_INTERVAL):
        self.filename = filename
        self.header = header
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.buffer = deque()
        self.last_flush_time = time.time()
        self.lock = threading.Lock()
        self.is_closed = False
        
        # Инициализация файла
        self._init_file()
        
        # Запуск фонового потока для периодической записи
        self.stop_event = threading.Event()
        self.flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
        self.flush_thread.start()
    
    def _init_file(self):
        if not os.path.exists(self.filename) or os.path.getsize(self.filename) == 0:
            dirpath = os.path.dirname(self.filename)
            if dirpath and not os.path.exists(dirpath):
                os.makedirs(dirpath, exist_ok=True)
            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(self.header)
            print(f"CSV инициализирован: записан заголовок в {self.filename}")
        else:
            print(f"CSV существует и не пустой: {self.filename}, пропускаем инициализацию.")
    
    def write_row(self, row):
        if self.is_closed:
            return
        with self.lock:
            self.buffer.append(row)
            if len(self.buffer) >= self.buffer_size:
                self._flush_buffer()
    
    def _flush_buffer(self):
        if not self.buffer or self.is_closed:
            return
            
        try:
            with open(self.filename, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                while self.buffer:
                    writer.writerow(self.buffer.popleft())
            
            self.last_flush_time = time.time()
            logging.debug(f"Записано {self.buffer_size} записей в {self.filename}")
        except Exception as e:
            logging.error(f"Ошибка записи в файл {self.filename}: {e}")
    
    def _periodic_flush(self):
        while not self.stop_event.is_set():
            time.sleep(1)
            if time.time() - self.last_flush_time > self.flush_interval:
                with self.lock:
                    if self.buffer and not self.is_closed:
                        self._flush_buffer()
    
    def flush(self):
        """Принудительная запись всех данных из буфера"""
        if self.is_closed:
            return
        with self.lock:
            self._flush_buffer()
    
    def close(self):
        """Закрытие writer с записью оставшихся данных"""
        if self.is_closed:
            return
        self.is_closed = True
        self.stop_event.set()
        self.flush()
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)
        logging.info(f"Writer для {self.filename} закрыт")

def cleanup_on_exit():
    """Функция очистки при завершении программы"""
    global ticks_writer, ob_writer, ws
    logging.info("Выполняется очистка при завершении...")
    
    if ws:
        try:
            ws.exit()
        except:
            pass
    
    if ticks_writer:
        ticks_writer.close()
    
    if ob_writer:
        ob_writer.close()
    
    logging.info("Очистка завершена")

def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    logging.info(f"Получен сигнал {signum}, завершаем работу...")
    cleanup_on_exit()
    sys.exit(0)

# Регистрируем обработчики
atexit.register(cleanup_on_exit)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Создаем буферизованные writers
ticks_writer = BufferedCSVWriter(TICKS_FILE, ["recv_time", "timestamp", "symbol", "price", "size", "side"])
ob_writer = BufferedCSVWriter(OB_FILE, ["recv_time", "symbol", "type", "seq", "u", "side", "price", "size", "ts"])

DEPTH = 50
TOPIC_OB = f"orderbook.{DEPTH}.{SYMBOL}"

backoff = 1
last_msg_time = time.time()

# парсинг
def handle_message(msg):
    global ticks_writer, ob_writer
    # используем timezone-aware, но убираем tzinfo, чтобы формат совпадал с utc.isoformat()
    recv_dt = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    topic = msg.get("topic", "")
    if topic == f"publicTrade.{SYMBOL}":
        for trade in msg.get("data", []):
            try:
                ts = int(trade.get("T"))
                price = float(trade.get("p", 0))
                size = float(trade.get("v", 0))
                side = trade.get("S", "")
            except Exception as e:
                print("Skipping trade due to error:", e)
                continue
            # UTC from timestamp, same format
            dt = datetime.fromtimestamp(ts/1000, timezone.utc).replace(tzinfo=None).isoformat()
            if ticks_writer:
                ticks_writer.write_row([recv_dt, dt, SYMBOL, price, size, side])
    elif topic == TOPIC_OB:
        msg_type = msg.get("type", "")
        data = msg.get("data", {}) or {}
        seq = data.get("seq")
        u = data.get("u")
        ts_server = data.get("ts")
        bids = data.get("b", [])
        asks = data.get("a", [])
        
        if msg_type == "snapshot":
            for bid in bids:
                try:
                    price = float(bid[0]); size = float(bid[1])
                except:
                    continue
                if ob_writer:
                    ob_writer.write_row([recv_dt, SYMBOL, "snapshot", seq, u, "Buy", price, size, ts_server])
            for ask in asks:
                try:
                    price = float(ask[0]); size = float(ask[1])
                except:
                    continue
                if ob_writer:
                    ob_writer.write_row([recv_dt, SYMBOL, "snapshot", seq, u, "Sell", price, size, ts_server])
        elif msg_type == "delta":
            for bid in bids:
                try:
                    price = float(bid[0]); size = float(bid[1])
                except:
                    continue
                if ob_writer:
                    ob_writer.write_row([recv_dt, SYMBOL, "delta", seq, u, "Buy", price, size, ts_server])
            for ask in asks:
                try:
                    price = float(ask[0]); size = float(ask[1])
                except:
                    continue
                if ob_writer:
                    ob_writer.write_row([recv_dt, SYMBOL, "delta", seq, u, "Sell", price, size, ts_server])
        else:
            print("Unknown orderbook msg type:", msg_type)

try:
    while True:
        try:
            last_msg_time = time.time()
            ws = WebSocket(testnet=False, channel_type="linear")
            ws.trade_stream(symbol=SYMBOL, callback=handle_message)
            ws.orderbook_stream(symbol=SYMBOL, depth=DEPTH, callback=handle_message)

            stop_event = threading.Event()
            def monitor():
                logging.info("Монитор запущен")
                while not stop_event.is_set():
                    if time.time() - last_msg_time > MSG_TIMEOUT:
                        logging.warning("Нет сообщений %s сек — закрываем", MSG_TIMEOUT)
                        if ws:
                            ws.exit()
                        stop_event.set()
                        break
                    time.sleep(1)
                logging.info("Монитор завершён")

            monitor_thread = threading.Thread(target=monitor, daemon=True)
            monitor_thread.start()

            logging.info("Главный поток — вход в wait-loop")
            while not stop_event.is_set():
                time.sleep(1)
            logging.info("Главный поток — выход из wait-loop")

            monitor_thread.join(timeout=2)
            backoff = 1

        except KeyboardInterrupt:
            logging.info("ручное завершение программы")
            break

        except Exception:
            logging.exception("Ошибка — reconnect через %s сек", backoff)
            # Записываем данные перед переподключением
            if ticks_writer:
                ticks_writer.flush()
            if ob_writer:
                ob_writer.flush()
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

finally:
    cleanup_on_exit()
    
    