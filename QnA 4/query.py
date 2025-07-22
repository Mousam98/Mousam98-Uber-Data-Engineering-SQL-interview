import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'schema')))
from spark import df, spark

df.createOrReplaceTempView('uber')

query = "with bf as ( \
            select driver_id, WEEKOFYEAR(trip_date) as week_num, count(*) as current_week_rides \
            from uber group by 1, 2), \
        \
        gf as (select driver_id, week_num, current_week_rides, \
        LAG(current_week_rides, 1, 0) over(partition by driver_id order by week_num) as prev_week_rides \
        from bf) \
        \
        select driver_id, week_num, current_week_rides, prev_week_rides, \
        (current_week_rides - prev_week_rides) as ride_diff \
        from gf order by driver_id"

spark.sql(query).show()

# same query using pyspark dataframe
from pyspark.sql.functions import weekofyear, count, lag
from pyspark.sql.window import Window
df1 = df.withColumn('week_num', 
                    weekofyear(col('trip_date'))) \
        .groupBy('driver_id', 'week_num') \
        .agg(count('*').alias('current_week_rides'))

window_def = Window.partitionBy('driver_id').orderBy('week_num')
df2 = df1.withColumn('prev_week_rides',
                    lag(col('current_week_rides'), 1, 0).over(window_def)) \
        .withColumn('ride_diff', col('current_week_rides')-col('prev_week_rides')) \
        .select(col('driver_id'), col('week_num'), col('current_week_rides'), col('prev_week_rides'), col('ride_diff'))

df2.show()
