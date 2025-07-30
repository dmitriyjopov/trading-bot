import websocket, json, threading, time, signal, atexit, logging
import parcer_core
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
    #для отладки:
    topic = msg.get("topic","")
    if topic.startswith("orderbook"):
        print(f"[{datetime.now()}][RAW OB] type={msg.get('type')} seq={msg.get('data',{}).get('seq')}")
    #передаем в парсер
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

def monitor(stop_evt, ws_app):
    logging.info("Monitor started")
    while not stop_evt.is_set():
        if time.time() - parcer_core.last_msg_time > MSG_TIMEOUT:
            logging.warning(f"No messages for {MSG_TIMEOUT}s, restarting WS")
            try:
                ws_app.close()
            except:
                pass
            break
        time.sleep(1)
    logging.info("Monitor exiting")

#регистрируем обработчики сигнала
signal.signal(signal.SIGINT, cleanup_and_exit)
signal.signal(signal.SIGTERM, cleanup_and_exit)
atexit.register(parcer_core.cleanup)

def run_reconnect_loop():
    backoff = 1
    while True:
        #запуск WS
        ws_app = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        parcer_core.last_msg_time = time.time()
        
        # 2)мониторим таймаут ping/pong
        stop_evt = threading.Event()
        mon_t = threading.Thread(target=monitor, args=(stop_evt, ws_app))
        mon_t.daemon = True
        mon_t.start()

        try:
            logging.info("Starting WebSocket connection...")
            ws_app.run_forever(
                ping_interval=MSG_TIMEOUT, 
                ping_timeout=MSG_TIMEOUT//2
            )

        except Exception as e:
            logging.error(f"WebSocket error: {e}")

        finally:
            #останавливаем монитор
            stop_evt.set()
            mon_t.join(timeout=2.0)
            if mon_t.is_alive():
                logging.warning("Monitor thread did not exit gracefully")
            
            #принудительно сбрасываем состояние
            ws_app.keep_running = False

        logging.info(f"Reconnecting in {backoff} seconds...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)  # Экспоненциальная задержка

if __name__ == "__main__":
    run_reconnect_loop()

