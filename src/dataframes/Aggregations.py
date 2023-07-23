from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

print("creating the spark session")
spark = SparkSession.builder.appName("Aggregations").master("local").getOrCreate()
print("Spark session is created")
moviesDF = spark.read.format("json").option("inferSchema","true").load("C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/movies.json")


# counting

# if you select by column name then count will ignore the null values
# if you select all using '*' then count will include the null values as well

moviesDF.select(count(col("Major_Genre"))).show()

moviesDF.selectExpr("count(*)").show()


moviesDF.select(countDistinct("Major_Genre")).show()

moviesDF.select(approx_count_distinct("Major_Genre")).show()

moviesDF.select(approxCountDistinct("Major_Genre")).show()

moviesDF.select(max("IMDB_Votes")).show()

moviesDF.selectExpr("min(IMDB_Votes)").show()

moviesDF.select(sum("Worldwide_Gross")).show()

moviesDF.selectExpr("avg(Worldwide_Gross)").show()

moviesDF.select(mean("Worldwide_Gross"), stddev("Worldwide_Gross")).show()

print("Grouping operation")

# grouping
# group by also treats null as one value
# reference : https://sparkbyexamples.com/pyspark/pyspark-groupby-explained-with-example/?expand_article=1
groupingDF = moviesDF.groupBy("Major_Genre")

groupingDF.count().show()

groupingDF.max("Production_Budget").show()

groupingDF.agg(
    count("*").alias("count_prod_budget"),
    avg("Production_Budget").alias("avg_prod_budget"),
    max("Production_Budget").alias("max_prod_budget"),
    min("Production_Budget").alias("min_prod_budget"),
    mean("Production_Budget").alias("mean_prod_budget"),
    stddev("Production_Budget").alias("stddev_prod_budget")

).orderBy("stddev_prod_budget").show()


'''
  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */
'''


profitsSum = moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).alias("total_sales")).show()

distinctDirectors = moviesDF.select(countDistinct("Director")).show()

meannstddevusgross = moviesDF.select(mean("US_Gross").alias("usgrossmean"), stddev("US_Gross").alias("usgrossstdeev")).show()

avgimdbngrossperdirector = moviesDF.groupBy("Director").agg(
    avg("IMDB_Rating").alias("avg_imdb_rating"),
    avg("US_Gross").alias("avgusgross")
).show()
