query = "select a.merchant_id, count(distinct b.user_id) unique_customers, round(sum(b.transaction_amount), 2) total_amount_received \
        from merchants a join transactions b \
        on a.merchant_id = b.merchant_id \
        group by a.merchant_id \
        having count(distinct b.user_id) >= 90"

spark.sql(query).show()

from pyspark.sql.functions import round, sum, col, avg, count, countDistinct

df1 = merchant_df.alias('a')
df2 = transaction_df.alias('b')

df = df1.join(df2, col('a.merchant_id') == col('b.merchant_id')) \
        .groupBy(col('a.merchant_id')) \
        .agg(countDistinct(col('b.user_id')).alias('unique_customers'),
            round(sum(col('b.transaction_amount')), 2).alias('total_amount_received')) \
        .filter(col('unique_customers') >= 90)
#df.show(20)
