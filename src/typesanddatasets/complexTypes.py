from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("complextypes").master("local").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

moviesDF = spark.read.format("json").option("inferSchema", "true").load("C:/Users/91909/Desktop/project/pyspark"
                                                                        "-essentials/resources/data/movies.json")

# we can parse the dates while we are reading the dataframe by providing the option "dateFormat" as one option. or if
# we read the dates as strings without parsing and later wanted to convert them to meaningful dates. below is the
# approch need to be used.

moviesDF.select("Title", to_date(col("Release_Date"), "dd-MMM-yy")).show()

movieswithreleasedates = moviesDF.select("Title", to_date(col("Release_Date"), "dd-MMM-yy").alias("actual_release")) \
    .withColumn("Today", current_date()) \
    .withColumn("Right_now", current_timestamp()) \
    .withColumn("Movie_age", datediff(col("Today"), col("actual_release"))) \
 \
    # when spark is not able to parse the dates , it will not crash . Actually , it will parse the values as null
movieswithreleasedates.select("*").where(col("actual_release").isNull()).show()

# if the dates are in mixed format . we have to use multiple formats while parsing.

'''
1. how do you deal with multiple formats.
2. parse the dates of the stock dataframe
'''

stocksdf = spark.read.option("inferSchema", "true").option("header", "true").csv(
    "C:/Users/91909/Desktop/project/pyspark"
    "-essentials/resources/data/stocks.csv")

stocksdf.select(to_date(col("date"), "MMM dd yyyy").alias("stock_date")).show()

# Structures

# with column operators

moviesDF.select(struct(col("US_Gross"), col("Worldwide_Gross"), col("US_DVD_Sales")).alias("profit")) \
    .select(col("profit").getField("US_Gross").alias("us_profit")).show()

# with expressions

moviesDF.selectExpr("Title", "(US_Gross,Worldwide_Gross,US_DVD_Sales) as profit") \
    .selectExpr("Title", "profit.US_Gross") \
    .show()

# Arrays

moviesDF.select(split(col("Title"), " ").alias("title_words")) \
    .selectExpr("title_words[0]").show()
