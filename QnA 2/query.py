df.createOrReplaceTempView('uber')

query = 'select a.user_id, CONCAT_WS(", ", COLLECT_SET(a.driver_id)) as drivers, avg(b.rating) rated_on_avg \
            from uber a, uber b \
            where a.ride_id <> b.ride_id and a.user_id = b.user_id and \
            abs(UNIX_TIMESTAMP(a.trip_date)-UNIX_TIMESTAMP(b.trip_date)) / 60 <= 10 \
            group by a.user_id \
            order by a.user_id'

spark.sql(query).show()

#same query using pyspark dataframe
from pyspark.sql.functions import col, unix_timestamp, abs, concat_ws, collect_set, avg

df1 = df.alias('a')
df2 = df.alias('b')

joined_df = df1.join(df2,
                    ((col("a.ride_id") != col("b.ride_id")) & (col("a.user_id") == col("b.user_id"))), \
                    how = "inner") \
            .withColumn("min_diff",
                        abs(unix_timestamp("a.trip_date") - unix_timestamp("b.trip_date")) / 60) \
            .filter(col("min_diff") <= 10) \
            .groupBy(col("a.user_id")) \
            .agg(concat_ws(',', collect_set(col("b.driver_id"))).alias("drivers"), 
                avg(col("a.rating")).alias("rated_on_avg")) \
            .orderBy(col("a.user_id"))

joined_df.show()
