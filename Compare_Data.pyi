import cx_Oracle
import sys
from pyspark.sql import SparkSession

localhost = input("Enter the Localhost")
port = input("Enter the port")
Service = input("service")

dsn = cx_Oracle.makedsn(
    localhost, port, Service
)

conn = cx_Oracle.connect(
    user='dw_user',
    password='Welcome1!',
    dsn=dsn)

print('Connection Successful')
session = SparkSession.builder.appName("DataValidation").master("local[*]").getOrCreate()
c=conn.cursor()

def sort_tb:
    tb = c.execute('ALTER TABLE tablename ORDER BY columnname ASC;. ')
    rdd=session.sparkContext.parallelize(tb)
    df1 = rdd.toDF()
    df1.printSchema()
    tb2 = c.execute('SELECT * FROM PRODUCT_OLD_BKP WHERE ROWNUM <= 20 ')
    rdd2=session.sparkContext.parallelize(tb2)
    df2 = rdd2.toDF()
    df2.printSchema()
