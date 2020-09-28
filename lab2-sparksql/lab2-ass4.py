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

#--TEMP READ--#
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
tempReadings = lines.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month=p[1].split("-")[1], time=p[2], temp=float(p[3]), quality=p[4]))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

#Get max temp for each station within the temperature interval
schema_temp_max = schemaTempReadings.groupBy('station').agg(F.max('temp').alias('temp'))
schema_temp = schema_temp_max.filter( (schema_temp_max.temp >= 25) & (schema_temp_max.temp <= 30))
schema_temp = schema_temp.join(schemaTempReadings, ['station','temp'], 'inner').\
                  select('year', 'station', 'temp')

# Get daily observations of rain and then filtering
schemaRainDaily = schemaRainReadings.groupBy('date').sum('rain')
schemaRainDaily.show()
schemaRainDaily = schemaRainDaily.join(schemaRainReadings, ['date'], 'inner')
schemaRainMax = schemaRainDaily.groupBy('station').agg(F.max('sum(rain)').alias('sumrain'))
schema_rain = schemaRainMax.filter ( (schemaRainMax.sumrain >= 100) & (schemaRainMax.sumrain <= 200) )

# JOIN TEMP AND RAIN
schema_tot = schema_rain.join(schema_temp, ['station'], 'inner').\
             select('station', 'temp', 'sumrain').orderBy('station', acsending=False).show()
