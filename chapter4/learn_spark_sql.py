# In Python
from pyspark.sql import SparkSession
# Create a SparkSession
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())

# In Python
schema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING"         

# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("chapter4/departuredelays.csv", schema=schema))
df.createOrReplaceTempView("us_delay_flights_tbl")


spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)

