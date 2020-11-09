# In Python, define a schema
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    # Create a SparkSession
    spark = (SparkSession
             .builder
             .appName("use_data_frame_read_write")
             .getOrCreate())

    # Programmatic way to define a schema
    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                            StructField('UnitID', StringType(), True),
                            StructField('IncidentNumber', IntegerType(), True),
                            StructField('CallType', StringType(), True),
                            StructField('CallDate', StringType(), True),
                            StructField('WatchDate', StringType(), True),
                            StructField('CallFinalDisposition', StringType(), True),
                            StructField('AvailableDtTm', StringType(), True),
                            StructField('Address', StringType(), True),
                            StructField('City', StringType(), True),
                            StructField('Zipcode', IntegerType(), True),
                            StructField('Battalion', StringType(), True),
                            StructField('StationArea', StringType(), True),
                            StructField('Box', StringType(), True),
                            StructField('OriginalPriority', StringType(), True),
                            StructField('Priority', StringType(), True),
                            StructField('FinalPriority', IntegerType(), True),
                            StructField('ALSUnit', BooleanType(), True),
                            StructField('CallTypeGroup', StringType(), True),
                            StructField('NumAlarms', IntegerType(), True),
                            StructField('UnitType', StringType(), True),
                            StructField('UnitSequenceInCallDispatch',IntegerType(), True),
                            StructField('FirePreventionDistrict',StringType(), True),
                            StructField('SupervisorDistrict',StringType(), True),
                            StructField('Neighborhood', StringType(), True),
                            StructField('Location', StringType(), True),
                            StructField('RowID', StringType(), True),
                            StructField('Delay', FloatType(), True)])



    fire_df = spark.read.csv("chapter3/sf-fire-calls.csv", header=True, schema=fire_schema)                          

    ## Save as parquet format

    parquet_path = "chapter3/sf-fire-calls-parquet"
    fire_df.write.format("parquet").save(parquet_path)

    parquet_table = "`fire_call`" # name of the table
    fire_df.write.format("parquet").saveAsTable(parquet_table)


(fire_ts_df.select(year('IncidentDate')).distinct().orderBy(year('IncidentDate')).show())
(fire_ts_df.select(year("IncidentDate").distinct().orderBy(year('IncidentDate')).show()))
