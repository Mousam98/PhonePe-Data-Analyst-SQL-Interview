query = "select user_id, avg(transactions) last_3_month_avg_transactions from  \
        (select user_id, count(*) transactions from transactions\
        where transaction_date between add_months(current_date, -3) and current_date \
        group by user_id) \
        group by user_id \
        order by last_3_month_avg_transactions desc"
spark.sql(query).show(10)

# same query in spark sql
from pyspark.sql.functions import add_months, col, avg, count, desc

sub_df = transaction_df.filter(current_date() <= add_months(col('transaction_date'), 3)) \
                        .groupBy(col('user_id')) \
                        .agg(count('*').alias('transaction_count'))
df = sub_df.groupBy(col('user_id')) \
            .agg(avg(col('transaction_count')).alias('last_3_month_avg_transactions')) \
            .orderBy(desc(col('last_3_month_avg_transactions')))

df.show(10)
