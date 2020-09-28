from pyspark import SparkContext

sc = SparkContext(appName = "spark")

station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
per_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_per = per_file.map(lambda line: line.split(";"))
lines_ost = station_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
per_rain = lines_per.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]))))
# (key, value) = (station, temperature)
station_list = lines_ost.map(lambda x: x[0]).collect()

#filter to right stations
per_rain = per_rain.filter(lambda x: x[0][0] in station_list)

#filter
per_rain = per_rain.filter(lambda x: int(x[0][1][0:4])>=1993 and int(x[0][1][0:4])<=2016)
per_rain = per_rain.reduceByKey(lambda v1, v2: (v1+v2))
per_rain = per_rain.map(lambda x: (x[0][1][0:7], (x[1], 1)))

#Get average rain for each month and station
count = per_rain.reduceByKey(lambda v1, v2:(v1[0]+v2[0], v1[1]+v2[1]))
average  = count.mapValues(lambda v:(v[0]/v[1], 0))
average = average.map(lambda x: (x[0],x[1][0]))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
average.saveAsTextFile("BDA/output")
