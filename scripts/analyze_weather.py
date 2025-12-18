from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, round as spark_round, desc, asc

# Create Spark session
spark = SparkSession.builder \
    .appName("Weather Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("WEATHER DATA ANALYSIS WITH SPARK")
print("=" * 60)

# Load JSON data
input_file = "/opt/spark/data/input/weather_data.json"
df = spark.read.option("multiLine", "true").json(input_file)

print(f"\nNumber of cities: {df.count()}")
print("\nData preview:")
df.show(5, truncate=False)

# === 1. GLOBAL STATISTICS ===
print("\n" + "=" * 60)
print("1. GLOBAL STATISTICS")
print("=" * 60)

stats = df.select(
    spark_round(avg("temperature"), 2).alias("avg_temperature"),
    spark_round(max("temperature"), 2).alias("max_temperature"),
    spark_round(min("temperature"), 2).alias("min_temperature"),
    spark_round(avg("humidity"), 2).alias("avg_humidity"),
    spark_round(avg("pressure"), 2).alias("avg_pressure")
)

stats.show()

# Save results
stats.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/opt/spark/data/output/global_stats")
print("Saved to data/output/global_stats/")

# === 2. CITY RANKINGS ===
print("\n" + "=" * 60)
print("2. CITY RANKINGS")
print("=" * 60)

print("\nTop 5 hottest cities:")
hottest = df.select("city", "country", "temperature") \
    .orderBy(desc("temperature")) \
    .limit(5)
hottest.show()

print("Top 5 coldest cities:")
coldest = df.select("city", "country", "temperature") \
    .orderBy(asc("temperature")) \
    .limit(5)
coldest.show()

print("Top 5 most humid cities:")
humid = df.select("city", "country", "humidity") \
    .orderBy(desc("humidity")) \
    .limit(5)
humid.show()

# Combine all rankings
hottest_df = hottest.selectExpr("'Hottest' as type", "city", "country", "temperature as value")
coldest_df = coldest.selectExpr("'Coldest' as type", "city", "country", "temperature as value")
humid_df = humid.selectExpr("'Most Humid' as type", "city", "country", "humidity as value")

all_rankings = hottest_df.union(coldest_df).union(humid_df)
all_rankings.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/opt/spark/data/output/city_rankings")
print("Saved to data/output/city_rankings/")

# === 3. COUNTRY STATISTICS ===
print("\n" + "=" * 60)
print("3. STATISTICS BY COUNTRY")
print("=" * 60)

country_stats = df.groupBy("country") \
    .agg(
        count("city").alias("num_cities"),
        spark_round(avg("temperature"), 2).alias("avg_temperature"),
        spark_round(avg("humidity"), 2).alias("avg_humidity")
    ) \
    .orderBy(desc("avg_temperature"))

country_stats.show(20)

country_stats.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/opt/spark/data/output/country_stats")
print("Saved to data/output/country_stats/")

# === 4. CORRELATIONS ===
print("\n" + "=" * 60)
print("4. CORRELATION ANALYSIS")
print("=" * 60)

# Temperature vs Humidity correlation
corr_temp_hum = df.stat.corr("temperature", "humidity")
print(f"\nTemperature vs Humidity: {corr_temp_hum:.4f}")

# Latitude vs Temperature correlation
corr_lat_temp = df.stat.corr("latitude", "temperature")
print(f"Latitude vs Temperature: {corr_lat_temp:.4f}")

# Save correlations
correlations = [
    ("Temperature vs Humidity", round(corr_temp_hum, 4)),
    ("Latitude vs Temperature", round(corr_lat_temp, 4))
]

corr_df = spark.createDataFrame(correlations, ["metric_pair", "correlation"])
corr_df.show()

corr_df.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/opt/spark/data/output/correlations")
print("Saved to data/output/correlations/")

# === 5. WEATHER DISTRIBUTION ===
print("\n" + "=" * 60)
print("5. WEATHER TYPES DISTRIBUTION")
print("=" * 60)

weather_types = df.groupBy("weather") \
    .agg(count("city").alias("count")) \
    .orderBy(desc("count"))

weather_types.show()

print("\n" + "=" * 60)
print("ANALYSIS COMPLETED")
print("=" * 60)
print("\nOutput files:")
print("  - data/output/global_stats/")
print("  - data/output/city_rankings/")
print("  - data/output/country_stats/")
print("  - data/output/correlations/")
print("=" * 60)

spark.stop()
