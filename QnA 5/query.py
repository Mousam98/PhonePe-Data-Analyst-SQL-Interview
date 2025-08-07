query = "select user_id, count(distinct date(transaction_date)) day_count \
        from transactions \
        where month(transaction_date) = month(current_date) -1 and year(transaction_date) = year(current_date) \
        and transaction_status = 'Success' \
        group by 1 \
        having day_count >= 20 \
        order by day_count desc"

spark.sql(query).show(5)

# same query in pyspark dataframe
from pyspark.sql.functions import month, count, year, desc, countDistinct, to_date

df = transaction_df.filter((month(col('transaction_date')) == month(current_date())-1) & \
                           (year(col('transaction_date')) == year(current_date())) & \
                            (col('transaction_status') == 'Success')) \
                    .groupBy(col('user_id')) \
                    .agg(countDistinct(to_date(col('transaction_date'))).alias('day_count')) \
                    .filter((col('day_count') >= 20))\
                    .orderBy(desc(col('day_count')))

df.show(5)
