import websocket, json, threading, time, signal, atexit, logging
import parcer_core  # только функции и писатели
from datetime import datetime, timezone

WS_URL = "wss://stream.bybit.com/v5/public/linear"
SYMBOL = parcer_core.SYMBOL   # "ETHUSDT"
DEPTH  = parcer_core.DEPTH    # 50
MSG_TIMEOUT   = 30           # сек


SUBSCRIBE_TOPICS = [
    f"publicTrade.{SYMBOL}",
    f"orderbook.{DEPTH}.{SYMBOL}"
]

parcer_core.init_writers()

def on_open(ws):
    print(f"[{datetime.now()}] WS opened, subscribing {SUBSCRIBE_TOPICS}")
    ws.send(json.dumps({"op":"subscribe","args":SUBSCRIBE_TOPICS}))

def on_message(ws, message):
    msg = json.loads(message)
    # Для отладки:
    topic = msg.get("topic","")
    if topic.startswith("orderbook"):
        print(f"[{datetime.now()}][RAW OB] type={msg.get('type')} seq={msg.get('data',{}).get('seq')}")
    # Передаём в парсер
    parcer_core.handle_msg(msg)

def on_error(ws, error):
    print("WS error:", error)

def on_close(ws, code, reason):
    print("WS closed:", code, reason)
    ws.keep_running = False

def cleanup_and_exit(signum=None, frame=None):
    print("Cleaning up…")
    parcer_core.cleanup()
    exit(0)

# Регистрируем обработчики сигнала
signal.signal(signal.SIGINT, cleanup_and_exit)
signal.signal(signal.SIGTERM, cleanup_and_exit)
atexit.register(parcer_core.cleanup)

def run_reconnect_loop():
    backoff = 1
    while True:
        # Запуск WS
        ws_app = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        parcer_core.last_msg_time = time.time()
        
        # 2) Мониторим таймаут ping/pong
        stop_evt = threading.Event()
        def monitor():
            logging.info("Monitor started")
            while not stop_evt.is_set():
                if time.time() - parcer_core.last_msg_time > MSG_TIMEOUT:
                    logging.warning("No messages for %s s, restarting WS", MSG_TIMEOUT)
                    ws_app.close()
                    break
                time.sleep(1)
            logging.info("Monitor exiting")

        mon_t = threading.Thread(target=monitor, daemon=True)
        mon_t.start()

        try:
            ws_app.run_forever(ping_interval=MSG_TIMEOUT, ping_timeout=MSG_TIMEOUT/2)
            # 3) Ждём сигнала от монитора
            logging.info("Main waiting...")
            while not stop_evt.is_set():
                time.sleep(1)
            mon_t.join(timeout=2)

        except KeyboardInterrupt:
            logging.info("Interrupted by user")
            parcer_core.cleanup()
            break

        except Exception:
            logging.exception("Error, reconnecting in %s s", backoff)
            parcer_core.ticks_writer.flush()
            parcer_core.ob_writer.flush()
            time.sleep(backoff)
            backoff = min(backoff*2, 60)
            continue

if __name__ == "__main__":
    run_reconnect_loop()

