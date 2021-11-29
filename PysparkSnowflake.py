from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("sf_con")
sc = SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()



sf={}
sf["URL"] = "https://teksystemspartner.us-east-1.snowflakecomputing.com/console/login#/"
sf["User"] = "skotla@teksystems.com"
sf["Password"] = ""
sf["Database"] = "TEK_PRACTICE"
sf["Schema"] = "PRACTICE"
sf["Role"] = ""
sf["SNOWFLAKE_SOURCE_NAME"] = "net.snowflake.spark.snowflake"

df=spark.read.format("net.snowflake.spark.snowflake").options(sf).option("dbtable","").load()
print(df.printSchema())