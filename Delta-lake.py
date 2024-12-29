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
"results.user.location",






)