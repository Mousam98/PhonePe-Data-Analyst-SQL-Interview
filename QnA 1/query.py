query = "select a.user_name, count(b.transaction_id) total_transactions, round(sum(b.transaction_amount), 2) total_amount_paid\
        from users a join transactions b \
        on a.user_id = b.user_id \
        where b.transaction_status = 'Success' and add_months(b.transaction_date, 2) <= CURRENT_DATE()\
        group by a.user_name \
        order by total_amount_paid desc"

spark.sql(query).show()

# same query using pyspark dataframe
from pyspark.sql.functions import desc, col, count, round, sum, add_months, current_date

df1 = user_df.alias('a')
df2 = transaction_df.alias('b')

df = df1.join(df2, col('a.user_id') == col('b.user_id'), how = 'inner') \
        .filter((col('b.transaction_status') == 'Success') & (add_months(col('b.transaction_date'), 2) <= current_date())) \
        .groupBy(col('a.user_name')) \
        .agg(count(col('b.transaction_id')).alias('total_transactions'), round(sum(col('b.transaction_amount')), 2).alias('total_amount_paid')) \
        .orderBy(desc(col('total_amount_paid')))

df.show(5)
