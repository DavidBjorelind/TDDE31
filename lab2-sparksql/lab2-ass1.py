from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql import HiveContext 

sc = SparkContext(appName = "spark")
sqlContext = SQLContext(sc)

#Loading text file and convert each line to a Row
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadings = lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], time=p[2], temp=float(p[3]), quality=p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

schema_temp = schemaTempReadings.filter( (schemaTempReadings.year >= 1950) & (schemaTempReadings.year <= 2014))

#Get Max
schema_temp_max = schema_temp.groupBy('year').agg(F.max('temp').alias('temp')) 
schema_temp_max = schema_temp_max.join(schema_temp, ['year','temp'], 'inner').\
                  select('year', 'station', 'temp').orderBy('temp', ascending=False).show()

#Get Min
schema_temp_min = schema_temp.groupBy('year').agg(F.min('temp').alias('temp')) 
schema_temp_min = schema_temp_min.join(schema_temp, ['year','temp'], 'inner').\
                  select('year', 'station', 'temp').orderBy('temp', ascending=False).show()



