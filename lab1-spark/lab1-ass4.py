from pyspark import SparkContext

sc = SparkContext(appName = "spark")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
per_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_temp = temperature_file.map(lambda line: line.split(";"))
lines_per = per_file.map(lambda line: line.split(";"))

def max(a, b):
    if a>=b:
        return a
    else:
        return b

# (key, value) = (station, temperature)
#stat_temperature = lines_temp.map(lambda x: ((x[0], x[1]), float(x[3])))
stat_temperature = lines_temp.map(lambda x: (x[0], float(x[3])))
# (key, value) = (station, percipitation)
stat_per = lines_per.map(lambda x: ((x[0], x[1]), float(x[3])))
# Getting daily reading for each station (instead of hourly)
stat_per = stat_per.reduceByKey(lambda x, y: x+y)
stat_per = stat_per.map(lambda x: (x[0][0], x[1]))

#get max temp for each station and day
max_temp = stat_temperature.reduceByKey(max)
#filter
some_temperature = max_temp.filter(lambda x: int(x[1]>=25) and int(x[1]<=30))

#max rain for each station and day
max_rain = stat_per.reduceByKey(max)
#filter rain
some_rain = max_rain.filter(lambda x: int(x[1]<=200) and int(x[1]>=100))

#join 
data=some_rain.join(some_temperature)

data.saveAsTextFile("BDA/output")
