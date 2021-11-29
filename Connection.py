import cx_Oracle

from pyspark.sql import SparkSession



dsn = cx_Oracle.makedsn(
    10.188.193.183, 1521, "INFADB"
)

conn = cx_Oracle.connect(
    user='dw_user',
    password='Welcome1!',
    dsn=dsn)

print('Connection Successful')
session = SparkSession.builder.appName("RddToDataframe").master("local[*]").getOrCreate()
c=conn.cursor()
tb = c.execute('SELECT PRODUCT_ID,PRODUCT_NAME FROM PRODUCT_OLD WHERE ROWNUM <= 20 ')
rdd=session.sparkContext.parallelize(tb)
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()