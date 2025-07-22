df.createOrReplaceTempView('uber')
query = 'select driver_id, sum(trip_time) total_trip_time, \
        avg(rating) avg_rating, count(*) total_trips\
        from uber \
        group by driver_id \
        having count(*) > 15 \
        order by avg_rating desc \
        limit 3'

spark.sql(query).show()

#same query using pyspark dataframe
#same query in pyspark dataframe
result_df = df.groupBy(col("driver_id")) \
            .agg(sum(col("trip_time")).alias("total_trip_time"),
                 avg(col("rating")).alias("avg_rating"),
                count('*').alias('total_trips'))\
            .filter(col("total_trips") > 15) \
            .orderBy(desc(col("avg_rating"))) \
            .limit(3)

result_df.show()
