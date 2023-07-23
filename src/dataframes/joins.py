# joins are wide transformations which means one partition to multiple partitions.
# for every join data will be scanned across all the clusters and will be joined.

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("joins").master("local").getOrCreate()

guitarsDF = spark.read.format("json").option("inferSchema", "true").load(
    "C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/guitars.json")

guitarPlayersDF = spark.read.format("json").option("inferSchema", "true").load(
    "C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/guitarPlayers.json")

bandsDF = spark.read.format("json").option("inferSchema", "true").load("C:/Users/91909/Desktop/project/pyspark"
                                                                       "-essentials/resources/data/bands.json")
# inner join

joincondition = guitarPlayersDF.band == bandsDF.id
guitarisBandsDF = guitarPlayersDF.join(bandsDF, joincondition, "inner").show()

# left outer
# everything in inner + all rows in left with nulls for corresponding right values
guitarPlayersDF.join(bandsDF, joincondition, "left_outer").show()

# right outer
# everything in inner + all rows in right with nulls for corresponding left values
guitarPlayersDF.join(bandsDF, joincondition, "right_outer").show()

# left outer
# everything in inner  and all the values from the left and right table
# basically outer join = inner + left_outer + right_outer
guitarPlayersDF.join(bandsDF, joincondition, "outer").show()

# semi - joins

# left_semi
# everything from the left table which has a matching condition from the right table
# basically we will take the inner join and cut out the rows from the right table
guitarPlayersDF.join(bandsDF, joincondition, "left_semi").show()

# anti-joins

# left_anti
# everything from the left table which there is  a no matching condition from the right table
# and only from the left table
guitarPlayersDF.join(bandsDF, joincondition, "left_anti").show()

# things to bear in mindb
# if there are multiple columns with same name and we need to join them

# option 1

# we can rename the columns which we are joining

guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band").show()

# option 2 -- drop the duplicate columns
# option 3 - rename the column and keep the data

guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars,guitarId)")).show()