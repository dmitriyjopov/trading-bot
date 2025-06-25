from pybit.unified_trading import WebSocket
import csv, os, time, logging, threading, sys
from datetime import datetime, timezone

os.system("")# иногда помогает
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

SYMBOL = "BTCUSDT"
TICKS_FILE = os.path.expanduser("C:/Users/382he/PycharmProjects/dataparcer/data.csv")
OB_FILE = os.path.expanduser("C:/Users/382he/PycharmProjects/dataparcer/orderbook.csv")
MSG_TIMEOUT =  30

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def init_csv(filename, header):
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        dirpath = os.path.dirname(filename)
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
        print(f"CSV инициализирован: записан заголовок в {filename}")
    else:
        print(f"CSV существует и не пустой: {filename}, пропускаем инициализацию.")

init_csv(TICKS_FILE, ["recv_time", "timestamp", "symbol", "price", "size", "side"])
init_csv(OB_FILE, ["recv_time", "symbol", "type", "seq", "u", "side", "price", "size", "ts"])

DEPTH = 50
TOPIC_OB = f"orderbook.{DEPTH}.{SYMBOL}"

backoff = 1
last_msg_time = time.time()

# парсинг
def handle_message(msg):
    # используем timezone-aware, но убираем tzinfo, чтобы формат совпадал с utc.isoformat()
    recv_dt = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    topic = msg.get("topic", "")
    if topic == f"publicTrade.{SYMBOL}":
        with open(TICKS_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
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
                writer.writerow([recv_dt, dt, SYMBOL, price, size, side])
    elif topic == TOPIC_OB:
        msg_type = msg.get("type", "")
        data = msg.get("data", {}) or {}
        seq = data.get("seq")
        u = data.get("u")
        ts_server = data.get("ts")
        bids = data.get("b", [])
        asks = data.get("a", [])
        with open(OB_FILE, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if msg_type == "snapshot":
                for bid in bids:
                    try:
                        price = float(bid[0]); size = float(bid[1])
                    except:
                        continue
                    writer.writerow([recv_dt, SYMBOL, "snapshot", seq, u, "Buy", price, size, ts_server])
                for ask in asks:
                    try:
                        price = float(ask[0]); size = float(ask[1])
                    except:
                        continue
                    writer.writerow([recv_dt, SYMBOL, "snapshot", seq, u, "Sell", price, size, ts_server])
            elif msg_type == "delta":
                for bid in bids:
                    try:
                        price = float(bid[0]); size = float(bid[1])
                    except:
                        continue
                    writer.writerow([recv_dt, SYMBOL, "delta", seq, u, "Buy", price, size, ts_server])
                for ask in asks:
                    try:
                        price = float(ask[0]); size = float(ask[1])
                    except:
                        continue
                    writer.writerow([recv_dt, SYMBOL, "delta", seq, u, "Sell", price, size, ts_server])
            else:
                print("Unknown orderbook msg type:", msg_type)

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
        ws.exit()
        break

    except Exception:
        logging.exception("Ошибка — reconnect через %s сек", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)
        continue
    
    
