import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'schema')))
from spark import df

df.createOrReplaceTempView('uber')

query = "select pickup, drop, count(*) route_count, max(trip_date) last_trip_time from uber \
        where date_add(trip_date, 30) >= current_date() \
        group by pickup, drop \
        order by count(*) desc\
        limit 5"

spark.sql(query).show()

# same query using pyspark dataframe
# same query in pyspark dataframe
from pyspark.sql.functions import count, max, desc

result_df = df.groupBy(col("pickup"), col("drop")) \
            .agg(count('*').alias('route_count'), 
                max(col("trip_date")).alias("last_trip_time")) \
            .orderBy(desc(count('*'))) \
            .limit(5)
result_df.show()
