query = """with weekly_transactions_per_user as
            (select user_id, 
            case
                when (weekofyear(current_date) - weekofyear(transaction_date)) = 1 then 'last_week'
                when (weekofyear(current_date) - weekofyear(transaction_date)) = 2 then '2nd_last_week'
                when (weekofyear(current_date) - weekofyear(transaction_date)) = 3 then '3rd_last_week'
                when (weekofyear(current_date) - weekofyear(transaction_date)) = 4 then '4th_last_week'
                when (weekofyear(current_date) - weekofyear(transaction_date)) = 5 then '5th_last_week'
                when (weekofyear(current_date) - weekofyear(transaction_date)) = 6 then '6th_last_week'
            end
            as week_no,
            count(*) as payment_count
            from transactions
            group by 1, 2
            having week_no is not NULL
            order by user_id)
            
            select week_no, concat_ws(', ', collect_set(user_id)) as user_group, count(*) count_of_users
            from weekly_transactions_per_user 
            group by 1
            order by
                case when week_no = 'last_week' then 0
                    when week_no = '2nd_last_week' then 1
                    when week_no = '3rd_last_week' then 2
                    when week_no = '4th_last_week' then 3
                    when week_no = '5th_last_week' then 4
                    else 5
                end
            """

spark.sql(query).show(20)

#same query in pyspark dataframe
from pyspark.sql.functions import when, col, count, concat_ws, collect_set, weekofyear, current_date
a = transaction_df.alias('a')

df1 = a.withColumn('week_no', 
                  when(weekofyear(current_date()) - weekofyear(col('transaction_date')) == 1, 'last_week')
                  .when(weekofyear(current_date()) - weekofyear(col('transaction_date')) == 2, '2nd_last_week')
                   .when(weekofyear(current_date()) - weekofyear(col('transaction_date')) == 3, '3rd_last_week')
                   .when(weekofyear(current_date()) - weekofyear(col('transaction_date')) == 4, '4th_last_week')
                   .when(weekofyear(current_date()) - weekofyear(col('transaction_date')) == 5, '5th_last_week')
                   .when(weekofyear(current_date()) - weekofyear(col('transaction_date')) == 6, '6th_last_week')
                   .otherwise('NULL')
                    ) \
        .groupBy(col('user_id'), col('week_no')) \
        .agg(count('*').alias('payment_count')) \
        .filter(col('week_no') != 'NULL') \
        .orderBy(col('user_id'))

df = df1.groupBy(col('week_no')) \
        .agg(count('*').alias('count_of_users'), 
            concat_ws(', ', collect_set(col('user_id'))).alias('user_group')) \
        .orderBy(when(col('week_no') == 'last_week', 0)
                 .when(col('week_no') == '2nd_last_week', 1)
                 .when(col('week_no') == '3rd_last_week', 2)
                 .when(col('week_no') == '4th_last_week', 3)
                 .when(col('week_no') == '5th_last_week', 4)
                 .otherwise(5)
                )
df.show()
