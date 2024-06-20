from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import yfinance as yf
import socket
import time
import threading
import random
import os
from dotenv import load_dotenv

load_dotenv()
move_k  = int(os.getenv("MOVE_K"))
save_prices = []
dates = []
def compute_moving_average(rdd):
    global save_prices
    global dates
    if not rdd.isEmpty():
        data = rdd.map(lambda x: x.split(','))
        date = data.map(lambda x: x[0]).collect()
        prices = data.map(lambda x: float(x[1])).collect()
        # prices = rdd.map(lambda x: float(x.split(',')[1])).collect()

        save_prices = save_prices + prices
        dates = dates + date
        if prices:
            with open("stock_prices.csv", "a") as f:
                for i in range(0, len(prices)-move_k+1):
                    moving_average = sum(prices[i: i + move_k]) / move_k
                    f.write(f"{date[i]},{moving_average}\n")
            save_prices = save_prices[-move_k:]
            dates = date[-move_k:]
def main():
    with open("stock_prices.csv", "w") as f:
        f.write("Date,Moving Average\n")
    host = "localhost"
    port = int(os.getenv("PORT"))

    conf = SparkConf().setAppName("StockPriceMovingAverage").setMaster("local[2]")
    conf.set("spark.replication", "1")
    conf.set("spark.default.parallelism", "1")
    conf.set("spark.streaming.concurrentJobs", "1")
    conf.set("spark.streaming.blockInterval", "200ms")
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "false")
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.eventLog.enabled", "false")
    
    sc = SparkContext(conf=conf)    
    # sc = SparkContext("local[2]", "StockPriceMovingAverage")
    ssc = StreamingContext(sc, 10)
    # ssc.checkpoint("checkpoint")

    lines = ssc.socketTextStream(host, port)
    lines.pprint()
    # lines.flatMap(lambda x: float(x.split(',')[1])).pprint()
    lines.foreachRDD(compute_moving_average)

    ssc.start()
    ssc.awaitTermination()

    # try:
    #     ssc.awaitTermination(10)
    # except KeyboardInterrupt:
    #     ssc.stop(stopSparkContext=True, stopGraceFully=True)
    #     print("Streaming context stopped gracefully")

if __name__ == "__main__":
    main()

