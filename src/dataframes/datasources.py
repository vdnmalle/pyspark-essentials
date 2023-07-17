from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType

spark = SparkSession.builder.appName("datasource").master("local").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
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

'''
Reading a DF

- format
- inferSchema or Schema
- one or more options
- mode:
   - failFast : fails if any one records is not confined to the schema
   - dropMalformed - drops all the records which are not confined to the schema
   - permissive - sets other fields to null when it meets a corrupted record, and puts the malformed string into a new field configured by columnNameOfCorruptRecord .
   

'''

carsDF = spark.read.format("json").schema(carsSchema). \
    option('mode', 'failFast') \
    .option('path', "C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/cars.json") \
    .load()
carsDF.printSchema()

carsDF.show()

# reading using a options

carsDF2 = spark.read.format("json").schema(carsSchema). \
    options(
    mode='failFast',
    path='C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/cars.json'
).load()

carsDF2.printSchema()
carsDF2.show()

# writing a df

'''
format
saveMode = overwrite, append , ignore , error(defaultmode)

'''

# while writing we have many options like sorting , bucketing , partitioning which are pretty advanced

carsDF.write. \
    format("json") \
    .mode('append').save("C:/Users/91909/Desktop/project/pyspark-essentials/resources/test")

# above can create only one file . but , if the dataframe size is large it can create multiple files as part of
# partition

'''
json flags


dateformat only works with schema . because otherwise spark doesn't know which fields to be inferred with dates
dates should be formatted correctly taking data into the consideration. otherwise if the dates
are not confined to the schema then they might inferred as nulls by the spark

-- also we can pass the timestampformat also specific to the times that are precision to milliseconds


-- compression techniques

# bzip2 , gzip, lz4, snappy , deflate
'''

carsSchema2 = StructType([
    StructField("Name", StringType()),
    StructField("Miles_per_Gallon", DoubleType()),
    StructField("Cylinders", LongType()),
    StructField("Displacement", DoubleType()),
    StructField("Horsepower", LongType()),
    StructField("Weight_in_lbs", LongType()),
    StructField("Acceleration", DoubleType()),
    StructField("Year", DateType()),
    StructField("Origin", StringType())
])

# reference : https://spark.apache.org/docs/latest/sql-data-sources-json.html
carsDF3 = spark.read.format("json") \
    .option('schema', carsSchema2) \
    .option('dateFormat', 'YYYY-MM-dd') \
    .option('allowSingleQuotes', 'true') \
    .option('compression', 'uncompressed') \
    .option('path', 'C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/cars.json') \
    .load()

carsDF3.printSchema()
carsDF3.show()

# reference : https://spark.apache.org/docs/latest/sql-data-sources-csv.html

stockSchema = StructType(
    [
        StructField('symbol', StringType()),
        StructField('date', DateType()),
        StructField('price', DoubleType())

    ]
)

# refer the below for the datetime patterns post spark 3.0 version
# use spark 3.3.2
# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
# if the dates are not parsed , please set the dateparser to the legacy, refer the above for the same

# csv options - https://spark.apache.org/docs/latest/sql-data-sources-csv.html
stocksDF = spark.read.format('csv') \
    .schema(stockSchema) \
    .options(header='true', dateFormat='MMM dd YYY', sep=',', nullValues='') \
    .load('C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/stocks.csv')

stocksDF.show()
stocksDF.printSchema()

# parquet

# default format
# binary compressed
# very predictable and don't need many options for spark to infer
# very higher compression compared to the standard formats

stocksDF.write.format('parquet').mode('append').save('C:/Users/91909/Desktop/project/pyspark-essentials/resources'
                                                    '/data/test')

# reading from a database
# below are the mandatory options
'''
format : jdbc
driver : jdbc driver
url : jdbc url
user : username
password : password
dbtable : tablename

'''

'''

Exercise: 
read the movies DF, 
then write it as 
- tab-separated values file 
- snappy Parquet 
- table "public.movies" in the Postgres DB

'''

moviesDF = spark.read.format("json").options(inferSchema='true', path='C:/Users/91909/Desktop/project/pyspark'
                                                                      '-essentials/resources/data/movies.json') \
    .load()

moviesDF.write.format('csv') \
    .option('sep', '\t') \
    .option('path', 'C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/test') \
    .option('mode', 'overwrite') \
    .save()
