from pybit.unified_trading import WebSocket
import csv
from time import sleep

SYMBOL = "BTCUSDT"

ws = WebSocket(
    testnet=False,
    channel_type="linear",
)
def handle_message(msg):
        topic = msg.get("topic", "")
        data = list()
        if topic == f"publicTrade.{SYMBOL}":
            for i in range(8):
                data[i] = msg.get("data", ""[i])
            with open('data.csv', 'w', newline='') as csvfile:
                parse = csv.writer(csvfile, delimiter=' ',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)

                parse.writerow([data[i]] * 5 + ['Baked Beans'])
                parse.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])

ws.trade_stream(
    symbol="BTCUSDT",
    callback=handle_message
)
while True:
    sleep(1)