from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql import HiveContext 

sc = SparkContext(appName = "spark")
sqlContext = SQLContext(sc)

#Loading text file and convert each line to a Row
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadings = lines.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], temp=float(p[3]), quality=p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

schema_temp = schemaTempReadings.filter( (schemaTempReadings.year >= 1950) & (schemaTempReadings.year <= 2014) & (schemaTempReadings.temp > 10) ).select('station', 'year', 'month').distinct()

#Get Max
schema_temp_count = schema_temp.groupBy('year','month').agg(F.count('station').alias('count')).orderBy('count',ascending=False).show()

