
#Following modules have been used to run the script earthquake.py


from pyspark.sql.functions import *  				            // Used for running SQL Scripts inside pyspark
from datetime import datetime 						             // Used for working with DATE/TIME.
from pyspark.sql import SparkSession 			    	      // Used to create sparksession
import geopy.distance 								               // Used while calculating distance
from delta.tables import * 							            // Used while working with spark delta-lake tables.



install the libraries with pip install command.
For Example, pip install geopy


"""Run below command to execute the script."""

spark-submit \
--packages io.delta:delta-core_2.12:2.3.0 \
--conf     "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf     "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
gs://sistic-subhash/subhash_test/earthquake.py

In the above spark-submit command, the packages and conf details are required while working with spark delta-lake tables and the application file .py is present in the specified GCS location.


Files Information:
1) database.csv - Input Dataset.
2) earthquake.py - PySpark Application File
3) output.csv - Final result CSV after all necessary transformations and filtering.
4) README - README file which contains general instructions about the code and how to run it.
