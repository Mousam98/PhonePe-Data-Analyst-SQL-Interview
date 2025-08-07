query = "select distinct user_id, transaction_status \
        from transactions \
        where user_id not in (select user_id from transactions where transaction_status = 'Failed')"

spark.sql(query).show()

# same query using pyspark dataframe
df1 = transaction_df.filter(col('transaction_status') == 'Failed') \
                    .select('user_id', 'transaction_status').distinct() \
                    .alias('a')

df2 = transaction_df.filter(col('transaction_status') == 'Success') \
                    .select('user_id', 'transaction_status').distinct() \
                    .alias('b')


df = df2.join(df1, on = 'user_id', how = 'left_anti')
df.show()
