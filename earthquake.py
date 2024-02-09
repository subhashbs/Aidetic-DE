from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import SparkSession
import geopy.distance
from delta.tables import *

#creating spark session
spark = SparkSession.builder.appName("Aidetic DE Assignment").getOrCreate()    

#********************************************************************************************************

"""1.Load the dataset into a PySpark DataFrame."""
#load the CSV dataset into dataframe
df_input_data = spark.read.option("header",True).format("csv").load("gs://sistic-subhash/subhash_test/database.csv")


#********************************************************************************************************


"""2.Convert the Date and Time columns into a timestamp column named Timestamp"""
#add one more column timestamp which is concat of date and time.
df_input_data_with_timestamp = df_input_data.withColumn("Timestamp", 
                                                   to_timestamp(concat(col("Date"),lit(" "),col("Time")),format="MM/dd/yyyy HH:mm:ss"))
                

#********************************************************************************************************


"""3.Filter the dataset to include only earthquakes with a magnitude greater than 5.0."""                
# Create one temp view from the dataframe.
df_input_data_with_timestamp.createOrReplaceTempView("input_data_view")

#Filter the dataset with only earthquake with a magnitude > 5.0

#PLEASE NOTE: upon analysing  the dataset, I came to know that magintude is greater than 5.0 for all the records, hence the where condition CAST(MAGNITUDE AS DOUBLE) > 5 is not required in this case, But still I am keeping it assuming there will be records coming in future which will have magnitude < 5

df_input_data_with_timestamp_filtered = spark.sql("""select * from input_data_view where cast(magnitude as float) > 5""")

#********************************************************************************************************


"""4.Calculate the average depth and magnitude of earthquakes for each earthquake type."""
#Calculating AVG of depth and magintude for each earthquake type.

df_avg_depth_magnitude = spark.sql("""select 
            type ,
            avg(cast(depth as float)) as avg_depth,
            avg(cast(magnitude as float)) as avg_magnitude 
        from input_data_view 
        group by 1"""
        )
        
df_avg_depth_magnitude.show(truncate=False)

#********************************************************************************************************


"""5.Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based
on their magnitudes."""
  
# Categorize earthquake into low, moderate and high based on magnitude value.
# In the given dataset, there are 64 disitinct magnitude values ranges from 5.5 from 9.1.
# I have made the following assumptions to categorize the earthquake as low, medium and high.
# magnitude < 6.0 - Low
# magnitude between 6.0 and 7.0 Medium
# magnitude > 7.0 - High

# I have added a new column as intensity to store these values.

#Below is UDF to categorize the earthquake.
@udf
def earthquake_intensity(magnitude_value):
    if float(magnitude_value) < 6.0:
        return 'Low'
    elif float(magnitude_value)>= 6.0 and float(magnitude_value) < 7.0:
        return 'Moderate'
    else:
        return 'High'

df_input_data_timestamp_filtered_intensity = df_input_data_with_timestamp_filtered.withColumn("intensity", 
                                                   earthquake_intensity(col("Magnitude")))


#********************************************************************************************************

"""6.Calculate the distance of each earthquake from a reference location (e.g., (0, 0))."""

#Calculating distance of each earthquake from a given location.
def measure_distance(loc_1,loc_2):
    #loc_1 = loc_1
    #loc_2 = loc_2
    return geopy.distance.geodesic(loc_1, loc_2).km
    
#Function Calling: Calculating distance of each earthquake from reference point (0,0)
#adding column eq_distance to store the value.

#Here I am using one intermediate spark delta lake table test_1_delta to update the distance of each earthquake from reference point (0,0)

df_input_data_timestamp_filtered_intensity.createOrReplaceTempView("input_data_final_view")

sql_insert_delta_table = """insert overwrite cassandra_db.test_1_delta select  *,'0' from input_data_final_view"""
spark.sql(sql_insert_delta_table)

df_final_res = spark.sql("select *,row_number() over(order by ID) as rnk from input_data_final_view")

deltaTable = DeltaTable.forPath(spark, "gs://sistic-subhash/test_1_delta")

def fun_distance(lower_limit,upper_limit):
#Create one empty DataFrame df_merge with ID and res.
    schema = StructType([
      StructField('ID', StringType(), True),
      StructField('res', StringType(), True),
      ])
    df_merge = spark.createDataFrame([],schema)   
    
    #calculate the distance between the two co-ordinates
    for i in df_final_res.filter((df_final_res["rnk"]>=lower_limit) & (df_final_res["rnk"]<upper_limit)).collect():
        coord_lat = float(i["Latitude"])
        coord__long = float(i["Longitude"])
        coord_1 = (0,0)
        coord_2 = (coord_lat,coord__long)
        res = str(measure_distance(coord_1,coord_2))
        #print(res)
        sql = f"""select '{i["ID"]}' as ID,'{res}' as res"""
        df = spark.sql(sql)
        df_merge = df_merge.union(df)
    
    #Updating the value of res in delta-lake table  test_1_delta with the actual distance.   
    deltaTable.alias("oldData").merge(df_merge.alias("newData"),
                                               "oldData.ID = newData.ID") \
        .whenMatchedUpdate(set={"eq_distance": "newData.res"}) \
        .execute()
    
    
#Process 1000 records at a time for fast processing, since adding entire dataset into dataframe will slow down the process.

#The below limit will determine the upper limit for FOR loop.
limit = df_final_res.count() + 1
for i in range(0,limit,1000):
    start = i
    end = start + 1000
    print("start",start,"\t\t end",end)
    #print("start:",start)
    #print("end:",end) 
    fun_distance(start,end)


#Writing output csv file into output.csv file in the specified target location.
spark.sql("Select * from cassandra_db.test_1_delta").repartition(1).toPandas().to_csv("gs://sistic-subhash/subhash_test/output.csv", header=True, index = False)

spark.sql("drop view input_data_final_view")
spark.sql("drop view input_data_view")

spark.stop()


