import websocket, json, threading, time, signal, atexit
import parcer_core  # только функции и писатели
from datetime import datetime, timezone

WS_URL = "wss://stream.bybit.com/v5/public/linear"
SYMBOL = parcer_core.SYMBOL   # "ETHUSDT"
DEPTH  = parcer_core.DEPTH    # 50


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
    # Обновляем время последнего сообщения
    parcer_core.last_msg_time = time.time()

def on_error(ws, error):
    print("WS error:", error)

def on_close(ws, code, reason):
    print("WS closed:", code, reason)

def cleanup_and_exit(signum=None, frame=None):
    print("Cleaning up…")
    parcer_core.cleanup()
    exit(0)

# Регистрируем обработчики сигнала
signal.signal(signal.SIGINT, cleanup_and_exit)
signal.signal(signal.SIGTERM, cleanup_and_exit)
atexit.register(parcer_core.cleanup)

# Запуск WS
ws_app = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)
ws_app.run_forever()


parcer_core.run_reconnect_loop()