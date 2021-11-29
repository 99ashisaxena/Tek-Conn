import findspark
from pyspark.sql import SparkSession


findspark.init()
spark=SparkSession.builder.appName('Spark_Oracle_Connection').getOrCreate()
driver='oracle.jdbc.OracleDriver'
url='jdbc:oracle:thin:@10.188.193.183:1521/INFADB'
user='dw_user'
password='Welcome1!'
query="(select count(*) from user_tab_columns where table_name = 'TEST_EMP1' MINUS select count(*) from user_tab_columns where table_name = 'TEST_EMP1_BKP')"
df=spark.read.format('jdbc').option('driver',driver).option('url',url).option('dbtable',query).option('user',user).option('password',password).load()
df.show()



