from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from psycopg2 import connect, Error
from logging import basicConfig, error, DEBUG


class InsertPostgres:
    def process(self, row):
        StartTime = str(row.StartTime)
        EndTime = str(row.EndTime)
        Device_Name = row.Device_Name
        Temperature = row.Avg_Temperature
        Humidity = row.Avg_Humidity
        try:
            connection = connect(user="postgres",
                                 password="kader",
                                 host="localhost",
                                 port="5432",
                                 database="iot_database")
            cursor = connection.cursor()
            sql_insert_query = """ INSERT INTO iot_data (starttime, endtime, device_name, avg_temperature, avg_humidity)
                               VALUES ('%s', '%s', '%s', %d, %d) """ % (
            StartTime, EndTime, Device_Name, Temperature, Humidity)
            cursor.execute(sql_insert_query)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into table")
        except Exception as e:
            error("Failed inserting record into table {}".format(e.args))


spark = SparkSession.builder.appName("BigData Iot 1").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "2")
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe",
                                                                                                       "grogu").load()
kafka_df_1 = kafka_df.selectExpr("cast(value as string)")

kafka_df_2 = kafka_df_1.withColumn("body", regexp_replace(kafka_df_1.value, "\"", ""))
kafka_df_3 = kafka_df_2.withColumn("body_1", split(kafka_df_2.body, ","))
kafka_df_4 = kafka_df_3.withColumn("Timestamp", col("body_1").getItem(0)) \
    .withColumn("Device_Name", col("body_1").getItem(1)) \
    .withColumn("Temperature", col("body_1").getItem(2).cast("Double")) \
    .withColumn("Humidity", col("body_1").getItem(3).cast("Double")).drop("body_1", "body", "value")
kafka_df_4.printSchema()

df_window = kafka_df_4.groupBy(window(kafka_df_4.Timestamp, "10 seconds", "5 seconds"), "Device_Name").mean().orderBy(
    "window")
df_window.printSchema()
df_window_1 = df_window.select(col("window.start").alias("StartTime"), col("window.end").alias("EndTime"),
                               "Device_Name",
                               col("avg(Temperature)").alias("Avg_Temperature"),
                               col("avg(Humidity)").alias("Avg_Humidity"))

df_window_1.printSchema()

df_window_1.writeStream.format("console").outputMode("complete").option("truncate", "false").start()
df_window_1.writeStream.outputMode("complete").foreach(InsertPostgres()).start()
kafka_df_4.writeStream.format('console').outputMode("update").option("truncate", "false").start().awaitTermination()
