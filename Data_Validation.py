import cx_Oracle
import sys
from pyspark.sql import SparkSession



dsn = cx_Oracle.makedsn(
    '10.188.193.183','1521', "INFADB"
)

conn = cx_Oracle.connect(
    user='dw_user',
    password='Welcome1!',
    dsn=dsn)

print('Connection Successful')
session = SparkSession.builder.appName("DataValidation").master("local[*]").getOrCreate()
c=conn.cursor()
tb = c.execute('SELECT * FROM TEST_EMP1')
for i in  tb:
    print(i)
'''rdd=session.sparkContext.parallelize(tb)
df1 = rdd.toDF()
df1.p'''
"""tb2 = c.execute('SELECT * FROM PRODUCT_OLD_BKP WHERE ROWNUM <= 20 ')
rdd2=session.sparkContext.parallelize(tb2)
df2 = rdd2.toDF()
df2.printSchema()"""
