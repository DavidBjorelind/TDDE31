from pyspark import SparkContext

sc = SparkContext(appName = "spark")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))


# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]), 1)))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][1][0:4])>=1960 or int(x[0][1][0:4])<=2014)

#Get number of measurements for each month and calculating average
count = year_temperature.reduceByKey(lambda v1, v2:(v1[0]+v2[0], v1[1]+v2[1]))
average  = count.mapValues(lambda v:(v[0]/v[1], 0))
average = average.map(lambda x: (x[0],x[1][0]))
average_sort = average.sortBy(ascending = False, keyfunc=lambda k: k[1])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
average_sort.saveAsTextFile("BDA/output")
