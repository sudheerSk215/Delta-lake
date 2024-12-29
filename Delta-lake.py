from pyspark.sql import sparksession
import sys
import urllib.request
from pyspark import sparkconf, sparkcontext
from delta.tables import *
import ssl
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell'
os.environ['JAVA_HOME'] = r'c:\programFiles\java\jdk1.8.0_202

conf = sparkconf().setAppName("pyspark").setMaster("local[*]")
sc = sparkcontext(conf = conf)

spark = ( sparksession.builder
                      .appName("DeltaLakeExample")
                      .config("spark.sql.extensions","io.delta.sql.DeltasparksessionExtension")
                      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.deltacatalog")
                      .getOrCreate()
        )

urldata = (
urllib.request
.urlopen( "https://randomuser.me.api/0.8/?/results=200",context=ssl._create_unverified_context()
        )
.read()
.decode('utf-8')
)

df = spark.read.json(sc.parallelize([urldata]))

resultexp = df.withcolumn("results",expr("explode(results)"))

resultexp.show()
finalflatten = resultexp.select(
"nationality",
"results.user.cell",
"results.user.dob",
"results.user.email",
"results.user.gender",
"results.user.location.city",
"results.user.location.state",
"results.user.location.street",
"results.user.location.zip",
"results.user.md5",
"results.user.name.first",
"results.user.name.last",
"results.user.name.title",
"results.user.password",
"results.user.phone",
"results.user.picture.large"
).withcolumn("username",regexp_replace(col("username"),"([0-9])","")).select("username","city","state","zip").drop_duplicates(["username"])

finalflatten.show()

import subprocess 
path_exists= subprocess.run(["aws","s3","ls","s3://skbuck/dest/deltadata/"]),stdout=subprocess.PPE, stderr = subprocess.PIPE).returncode == 0
print(path_exists)

if not path_exists : #FALSE
  print("======= DATA DOES NOT EXISTS ========")
  #Write the initial data to the s3 path in delta format 
finalflatten.write.format("delta").mode("overwrite").save("s3://skbuck/dest/deltadata")

else:
  print()
  target=spark.read.load("s3://skbuck/dest/deltadata")
  source = finalflatten
  print("===== UPDATES ====")
  print()
  updates = target.join(source,["username"],"inner")
  updates.show()
  print()
  print("==== NEW ====")
  
  
