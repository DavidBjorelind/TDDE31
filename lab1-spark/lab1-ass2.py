from pyspark import SparkContext

sc = SparkContext(appName = "spark")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

def over_10(temp):
    if temp>10:
        return 1
    else:
        return 0

# (key, value) = (year,temperature)
# Made modifications to find distinct values for each station
year_temperature = lines.map(lambda x: (x[1][0:7], (x[0], over_10(float(x[3]))))).distinct()

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0:4])>=1950 or int(x[0][0:4])<=2014)

# Additional map to get rid of station
year_temperature = year_temperature.map(lambda x: (x[0], x[1][1]))

#Get number of measurements for each month
count = year_temperature.reduceByKey(lambda v1, v2: v1+v2)
count_sort = count.sortBy(ascending = False, keyfunc=lambda k: k[1])

count_sort.saveAsTextFile("BDA/output")
