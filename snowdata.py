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
                      .appName("snowdata")
                      .getOrCreate()
        )
snowdf = (
  spark.read
       .format("snowflake")
       .option("sURL","https://cjihmo-oz13001.snowflakecomputing.com")
       .option("sfAccount","cjihmo")
       .option("sfUser","zeyosnow")
       .option("sfPassword","sudheersk")
       .option("sfDatabase","zeyodb")
       .option("sfSchema","zeyoschema")
       .option("sfRole","ACCOUNTADMIN")
       .option("sfWarehouse","COMPUTE_WH")
       .option("dbtable","srctab")
       .load()
)

snowdf.show()



