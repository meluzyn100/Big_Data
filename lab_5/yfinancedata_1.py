from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import yfinance as yf
import socket
import time
import threading
import random
import os
from dotenv import load_dotenv
load_dotenv()
period = os.getenv("PERIOD")
interval = os.getenv("INTERVAL")

def fetch_stock_data(stock_symbol):
    stock = yf.Ticker(stock_symbol)
    data = stock.history(period=period, interval=interval)
    return data

def send_data_to_socket(stock_symbol, host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Listening on port {port}")

    conn, addr = server_socket.accept()
    print(f"Connection from {addr}")

    data = fetch_stock_data(stock_symbol)
    if not data.empty:
        for index, row in data.iterrows():
            message = f"{index}, {row['Close']}\n"
            conn.send(message.encode())
            time.sleep(max(random.normalvariate(0.1, 0.1), 0.01))
    conn.close()
    server_socket.close()

def main():
    stock_symbol = "AAPL"  
    host = "localhost"
    port = int(os.getenv("PORT"))
    send_data_to_socket(stock_symbol, host, port)

if __name__ == "__main__":
    main()