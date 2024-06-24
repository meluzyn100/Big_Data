from pyspark.sql import SparkSession


file_path = "file:///home/olek/studia/semestr_3_AM/Big_Data/Big_Data/projekt/data/charts.csv"
file_path = "/home/olek/studia/semestr_3_AM/Big_Data/Big_Data/projekt/data/charts.csv"


# Tworzenie sesji Spark
spark = SparkSession.builder \
    .appName("Read CSV File") \
    .getOrCreate()


df = spark.read.csv(file_path, header=True, inferSchema=True)

df.show()

df.printSchema()

spark.stop()