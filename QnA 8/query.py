query = """with first_payment_info as
        (select merchant_id, first_transaction_amount, first_payment_date from
        (select merchant_id, transaction_amount first_transaction_amount, transaction_date first_payment_date,
        row_number() over (partition by merchant_id order by transaction_date) as rank from transactions 
        where transaction_status = 'Success'
        ) where rank = 1), 
        
        latest_payment_info as
        (select merchant_id, latest_transaction_amount, latest_payment_date from
        (select merchant_id, transaction_amount latest_transaction_amount, transaction_date latest_payment_date,
        row_number() over (partition by merchant_id order by transaction_date desc) as rank from transactions
        where transaction_status = 'Success'
        )
        where rank = 1)

        select a.merchant_id, a.first_payment_date, a.first_transaction_amount,
                b.latest_payment_date, b.latest_transaction_amount
        from first_payment_info a join latest_payment_info b
        on a.merchant_id = b.merchant_id
        """

spark.sql(query).show(20)

# same query in pyspark dataframe

from pyspark.sql.functions import col, min, max, desc, row_number
from pyspark.sql.window import Window

window_def_first = Window.partitionBy('merchant_id').orderBy('transaction_date')
window_def_latest = Window.partitionBy('merchant_id').orderBy(desc('transaction_date'))

first_payment_df = transaction_df.filter(col('transaction_status') == 'Success') \
                                .withColumn('rank', 
                                            row_number().over(window_def_first)) \
                                .filter(col('rank') == 1) \
                                .select('merchant_id', 'transaction_amount', 'transaction_date').alias('a')

latest_payment_df = transaction_df.filter(col('transaction_status') == 'Success') \
                                    .withColumn('rank', 
                                            row_number().over(window_def_latest)) \
                                .filter(col('rank') == 1) \
                                .select('merchant_id', 'transaction_amount', 'transaction_date').alias('b')

df = first_payment_df.join(latest_payment_df, on = (col('a.merchant_id') == col('b.merchant_id'))) \
                    .select(col('a.merchant_id'), col('a.transaction_date').alias('first_transaction_date'), 
                            col('a.transaction_amount').alias('first_transaction_amount'), col('b.transaction_date').alias('latest_transaction_date'),
                           col('b.transaction_amount').alias('latest_transaction_amount')) \
                    .orderBy(col('a.merchant_id'))

df.show(20)
