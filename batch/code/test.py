from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def extractYearFromStartTimeColumn(val):
  return val.substr(0,4)

conf = SparkConf().setAppName("uni").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)


schemaString =  "ID Source TMC Severity Start_Time End_Time Start_Lat Start_Lng End_Lat End_Lng Distance(mi) Description Number Street Side City \
                County State Zipcode Country Timezone Airport_Code Weather_Timestamp Temperature(F) Wind_Chill(F) Humidity(%) Pressure(in) \
                Visibility(mi) Wind_Direction Wind_Speed(mph) Precipitation(in) Weather_Condition Amenity Bump Crossing Give_Way Junction \
                No_Exit Railway Roundabout Station Stop Traffic_Calming Traffic_Signal Turning_Loop Sunrise_Sunset Civil_Twilight \
                Nautical_Twilight Astronomical_Twilight" 


fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.read.csv("hdfs://namenode:9000/user/test/data.csv.tmp", header=True, mode="DROPMALFORMED", schema=schema)
df = df.withColumn("Year", extractYearFromStartTimeColumn(col("Start_Time")))


# df = df.withColumn("TimeOfDay", hour(unix_timestamp("Start_Time", "yyyy-MM-dd HH:mm:ss").cast("double").cast("timestamp")))
df = df.withColumn("TimeOfDay", hour("Start_Time"))




print("Severity")
print('-------------------------------')
df.groupBy("Severity").count().sort(col("count").desc()).show()
print('-------------------------------')

print("State")
print('-------------------------------')
df.groupBy("State").count().sort(col("count").desc()).show()
print('-------------------------------')


print("Weather_Condition")
print('-------------------------------')
df.groupBy("Weather_Condition").count().sort(col("count").desc()).show()
print('-------------------------------')


print("Year")
print('-------------------------------')
df.groupBy("Year").count().sort(col("count").desc()).show()
print('-------------------------------')

print("TimeOfDay")
print('-------------------------------')
df.groupBy("TimeOfDay").count().sort(col("count").desc()).show()
print('-------------------------------')