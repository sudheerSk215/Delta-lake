from pyspark.sql import sparksession

deltadf = spark.read.format("parquet").load("s3://skbuck"/dest/deltadata")
snowdf = spark.read.load("s3://skbuck/dest/site_count")
joindf = deltadf.join(snowdf,["username"],"left")

 joindf.write.mode("overwrite").save("s3://skbuck/dest/masterdata") 

      
