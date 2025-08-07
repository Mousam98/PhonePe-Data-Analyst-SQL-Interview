query = "select * from (select city, bank_name, transaction_count, row_number() over(partition by city order by transaction_count desc) ranks from \
        (select a.city, b.bank_name, count(*) transaction_count from users a join transactions b \
        on a.user_id = b.user_id \
        group by 1, 2) )\
        where ranks <= 5"

#query = "select distinct city from users"

spark.sql(query).show()

# same code in pyspark dataframe

from pyspark.sql.functions import desc, count, col, row_number
from pyspark.sql.window import Window

df1 = user_df.alias('a')
df2 = transaction_df.alias('b')

sub_df = df1.join(df2, 
                  on = (col('a.user_id') == col('b.user_id')), how = 'inner') \
            .groupBy(col('a.city'), col('b.bank_name')) \
            .agg(count('*').alias('transaction_count'))

window_def = Window.partitionBy('city').orderBy(desc('transaction_count'))

df = sub_df.withColumn('ranks',
                              row_number().over(window_def)) \
            .select(col('city'), col('bank_name'), col('transaction_count'), col('ranks'))

df.show()
