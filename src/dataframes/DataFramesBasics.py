from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructType, StringType, StructField, DoubleType, Row

print("started creating the sparksession")
spark = SparkSession.builder.appName("DataframeBasics").master("local").getOrCreate()
print("created the spark session")

# inferring schema is performance decorating function as it infers schema for each and every row
firstDF = spark.read.format("json").option("inferSchema", "true").load(
    "C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/cars.json")
# to display the dataframe
firstDF.show()

# to print the schema

firstDF.printSchema()
# this way we can print the each rows
# scala equivalent : firstDF.foreach(println)

for x in firstDF.take(10):
    print(x)

# pyspark datatypes
# we can create this schema for multiple ways. but , preferred way in prod is creating a json schema.
# we can also create multiple other types like arrayType , MapType etc
# reference : https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/
carsSchema = StructType([
    StructField("Name", StringType()),
    StructField("Miles_per_Gallon", DoubleType()),
    StructField("Cylinders", LongType()),
    StructField("Displacement", DoubleType()),
    StructField("Horsepower", LongType()),
    StructField("Weight_in_lbs", LongType()),
    StructField("Acceleration", DoubleType()),
    StructField("Year", StringType()),
    StructField("Origin", StringType())
])

# passing out own schema

df2 = spark.read.format("json").schema(carsSchema).load(
    "C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/cars.json")

# creating rows

row = ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")

# creating from list of tuples
rows = [("buick skylark 320", 15.0, 8, 350.0, 165, 3693, 11.5, "1970-01-01", "USA"),
        ("plymouth satellite", 18.0, 8, 318.0, 150, 3436, 11.0, "1970-01-01", "USA"),
        ("amc rebel sst", 16.0, 8, 304.0, 150, 3433, 12.0, "1970-01-01", "USA")]

df3 = spark.createDataFrame(rows)
df3.printSchema()

df3.show()

# schemas applies for dataframes not to the rows

# creating dataframe with implicits

# create a dataframe and use toDF method with column names

df3.toDF()

# create a rdd and use toDF()

# not advised to use the conversions as it can badly impact the performance.

'''
Exercise: 1) Create a manual DF describing smartphones
make
model
screen dimension
camera megapixels
2) Read another file from the data/ folder, e.g. movies.json
print its schema
count the number of rows, call count()

'''

cars = [('tesla', 'model3', 12, 13),
        ('tesla', 'models', 12, 13),
        ('tesla', 'modelx', 12, 13)]

newCarSchema = StructType([
    StructField('brand', StringType(), nullable=True),
    StructField('modelname', StringType()),
    StructField('displayinche', LongType()),
    StructField('camera', LongType())
])

# carsDF = spark.createDataFrame(cars, newCarSchema)

moviesDF = spark.read.format('json').option('inferSchema', 'true').load(
    "C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/movies.json")
moviesDF.printSchema()
print(moviesDF.count())

# Dataframes are distributed collection of rows confirming to certain schema
# schema - list describing the column names and types
#  known to the spark at the compile time
# all rows should be confined to the same schema
# immutable
# create a new transformation
# narrow - one input partiton to one output partiton
# wide - one input to multiple output partiton
# in wide shuffling hapends - which is massive performance topic
