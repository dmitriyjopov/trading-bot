from pybit.unified_trading import WebSocket
import csv
from time import sleep
import os
from datetime import datetime

SYMBOL = "BTCUSDT"
CSV_FILE = os.path.expanduser("C:/Users/382he/PycharmProjects/dataparcer/src/data.csv")
filename = 'data.csv'


ws = WebSocket(
    testnet=False,
    channel_type="linear",
)

def init_csv(filename, header):
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
        print(f"CSV инициализирован: записан заголовок в {filename}")
    else:
        print(f"CSV существует и не пустой: {filename}, пропускаем инициализацию.")

init_csv(filename, ["timestamp", "symbol", "price", "size", "side"])

def handle_message(msg):
        print("Received:", msg)
        topic = msg.get("topic", "")
        if topic == f"publicTrade.{SYMBOL}":
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                for trade in msg.get("data", []):
                    try:
                        ts = int(trade.get("T"))
                        price = float(trade.get("p", 0))
                        size = float(trade.get("v", 0))
                        side = trade.get("S", "")
                    except Exception as e:
                    # Если какое-то поле отсутствует или неверного формата, пропускаем
                        print("Skipping trade due to error:", e)
                        continue
                    # Преобразуем timestamp в ISO-строку (UTC). Можно хранить просто ts, если нужен ms.
                    dt = datetime.utcfromtimestamp(ts/1000).isoformat()
                    writer.writerow([dt, SYMBOL, price, size, side])                   

ws = WebSocket(
    testnet=False,
    channel_type="linear",
)

ws.trade_stream(
    symbol=SYMBOL,
    callback=handle_message
)

# Бесконечный цикл для поддержания работы скрипта
while True:
    try:
        # Просто поддерживаем соединение активным
        time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        break