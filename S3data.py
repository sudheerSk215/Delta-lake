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
                      .appName("S3data")
                      .getOrCreate()
        )

s3df = spark.read.load("s3://skbuck/src")
s3df.show()

aggamt = s3df.groupBy("username").agg(sum("amount").alias("total")).withcolumn("total",expr("cast(total as decimal(18,2))"))
aggamt.show()

aggamt.write.mode("overwrite").save("s3://skbuck/dest/total_amount_data")

