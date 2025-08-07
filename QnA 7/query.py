query = "select user_id, count(*) transaction_count_above_10K from (select distinct user_id, transaction_amount from transactions \
        where transaction_amount > 10000) \
        group by user_id \
        having count(*) >= 5"

spark.sql(query).show(5)

# same query in pyspark dataframe

from pyspark.sql.functions import count, col

sub_df = transaction_df.filter(col('transaction_amount') > 10000)

df = sub_df.groupBy(col('user_id')) \
            .agg(count('*').alias('transaction_count_above_10K')) \
            .filter(col('transaction_count_above_10K') >= 5)

df.show(5)
