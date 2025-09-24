from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace

# إنشاء SparkSession
spark = SparkSession.builder \
    .appName("KafkaWeatherStreaming") \
    .getOrCreate()

# ----- قراءة البيانات من Kafka -----
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# تحويل البيانات من bytes إلى string
df_string = df.selectExpr("CAST(value AS STRING)")

# تقسيم السطر إلى أعمدة منفصلة
split_col = split(df_string['value'], ' \| ')

weather_df = df_string.withColumn("timestamp", split_col.getItem(0)) \
    .withColumn("city", split_col.getItem(1)) \
    .withColumn("temperature", regexp_replace(split_col.getItem(2), "Temp:|°C", "")) \
    .withColumn("humidity", regexp_replace(split_col.getItem(3), "Humidity:|%", "")) \
    .withColumn("wind_speed", regexp_replace(split_col.getItem(4), "Wind:|km/h", "")) \
    .select("timestamp", "city", "temperature", "humidity", "wind_speed")

# ----- كتابة البيانات إلى Parquet بشكل مستمر -----
query = weather_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "spark_weather_parquet") \
    .option("checkpointLocation", "checkpoint_parquet") \
    .start()

query.awaitTermination()
