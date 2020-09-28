from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql import HiveContext 
sc = SparkContext(appName = "spark")
sqlContext = SQLContext(sc)

#--RAIN READ--#
pre_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines = pre_file.map(lambda line: line.split(";"))
rainReadings = lines.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], date=p[1], rain=float(p[3]), quality=p[4]))
schemaRainReadings = sqlContext.createDataFrame(rainReadings)
schemaRainReadings.registerTempTable("rainReadings")

#--STATION READ--#
ost_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines = ost_file.map(lambda line: line.split(";"))
ostReadings = lines.map(lambda p: Row(station=p[0]))
schemaOstReadings = sqlContext.createDataFrame(ostReadings)
schemaOstReadings.registerTempTable("ostReadings")

# Get MONTHLY observations of rain and then filtering
schemaRainReadings = schemaRainReadings.filter( (schemaRainReadings.year >= 1993) & (schemaRainReadings.year <= 2016) )
schemaRainMonth = schemaRainReadings.groupBy('year', 'month', 'station').sum('rain')
schemaRainMonth = schemaRainMonth.join(schemaRainReadings, ['year', 'month'], 'inner')

# Only Ost-stations
schemaOstRain = schemaRainMonth.join(schemaOstReadings, ['station'], 'inner')
schemaOstRain = schemaOstRain.groupBy('year', 'month').avg('sum(rain)')
schemaOstRain.select('year', 'month', 'avg(sum(rain))').orderBy('avg(sum(rain))', ascending=False).show()

