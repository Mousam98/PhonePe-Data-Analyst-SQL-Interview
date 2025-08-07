query = """with temp_transactions as 
            (select a.merchant_id, b.city, sum(a.transaction_amount) total_transaction_amount
            from transactions a join merchants b
            on a.merchant_id = b.merchant_id
            group by 1, 2),

            merchant_avg_transaction_by_city as 
            (select city, avg(total_transaction_amount) avg_transaction_amount
            from temp_transactions
            group by 1)
            
            select b.merchant_id, a.city, a.avg_transaction_amount, b.total_transaction_amount
            from merchant_avg_transaction_by_city a join temp_transactions b
            on a.city = b.city
            where b.total_transaction_amount > a.avg_transaction_amount
            order by 2, 1"""

spark.sql(query).show(20)

# same query in pyspark dataframe
from pyspark.sql.functions import avg, col, sum
a = transaction_df.alias('a')
b = merchant_df.alias('b')

df1 = a.join(b, on = (col('a.merchant_id') == col('b.merchant_id')), how = 'inner') \
                .groupBy(col('b.merchant_id'), col('b.city')) \
                .agg(sum(col('a.transaction_amount')).alias('total_transaction_amount')).alias('x')

df2 = df1.groupBy(col('city')) \
        .agg(avg(col('total_transaction_amount')).alias('avg_transaction_amount')).alias('y')

df = df1.join(df2, on = (col('x.city') == col('y.city')), how = 'inner') \
        .filter(col('x.total_transaction_amount') > col('y.avg_transaction_amount')) \
        .select(col('x.merchant_id'), col('x.city'), col('y.avg_transaction_amount'), col('x.total_transaction_amount')) \
        .orderBy(col('x.city'), col('x.merchant_id'))

df.show(20)
