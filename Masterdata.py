from pyspark.sql import sparksession
from pyspark import sparkconf, sparkcontext
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell'
os.environ['JAVA_HOME'] = r'c:\programFiles\java\jdk1.8.0_202

conf = sparkconf().setAppName("pyspark").setMaster("local[*]")
sc = sparkcontext(conf = conf)


spark = ( sparksession.builder
                      .appName("Masterdata")
                      .getOrCreate()
        )


deltadf = spark.read.format("parquet").load("s3://skbuck"/dest/deltadata")
snowdf = spark.read.load("s3://skbuck/dest/site_count")
tmdf = spark.read.load("s3://skbuck/total_amount_data")      
joindf = deltadf.join(snowdf,["username"],"left").join(tmdf,["username"],"left")

 joindf.write.mode("overwrite").save("s3://skbuck/dest/masterdata") 

print()
print("=== Master data Written ===")
print()      

      
