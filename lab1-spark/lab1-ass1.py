from pyspark import SparkContext

sc = SparkContext(appName = "spark")

def max_temp(a, b):
    if a>=b:
        return a
    else:
        return b

def min_temp(a, b):
    if a>=b:
        return b
    else:
        return a

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 or int(x[0])<=2014)

#Get max and low temp
max_temp = year_temperature.reduceByKey(max_temp)
min_temp = year_temperature.reduceByKey(min_temp)

# Merge max and min temperatures
temperatures = max_temp.join(min_temp)
temperatures = temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

temperatures.saveAsTextFile("BDA/output")
