from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName('test') \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.cores", "1").getOrCreate()

merchant_df = spark.read.csv('merchants.csv', header = True)
user_df = spark.read.csv('users.csv', header = True)
transaction_df = spark.read.csv('transactions.csv', header = True)

merchant_df.createOrReplaceTempView('merchants')
user_df.createOrReplaceTempView('users')
transaction_df.createOrReplaceTempView('transactions')
