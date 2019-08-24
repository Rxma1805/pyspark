!pip install pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
conf = SparkConf().setAll([('spark.executor.memory', '1g'),('spark.driver.memory','1g')])
sc =  SparkContext(conf=conf)
# solve the question:AttributeError: 'PipelinedRDD' object has no attribute 'toDF'
sqlContext = SQLContext(sc)
rdd = sc.parallelize([1,2,3,4])
df = rdd.map(lambda l: Row(l)).toDF()
with open ('/bigdata/xiaoma/spark/data/people.csv') as f:
    for l in f:
        print(l)
myDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("file:///bigdata/xiaoma/spark/data/people.csv")
df.registerTempTable("tasks")
results = sqlContext.sql("select * from tasks")
results.show()
lines = sc.textFile('file:///bigdata/xiaoma/spark/data/people.csv')\
.map(lambda x:x.split(','))\
.map(lambda l:Row(ID=l[0],name=l[1],age=l[2],sex=l[3],val=l[4]))
for i in lines.collect(): 
    print(i)
    
myDF = sc.textFile('file:///bigdata/xiaoma/spark/data/people.csv')\
.map(lambda x:x.split(','))\
.map(lambda l:Row(ID=l[0],name=l[1],age=l[2],sex=l[3],val=l[4]))\
.toDF()
myDF.show(20)  
myDF.select('name').show
myDF.registerTempTable("tmp_df")
sqlContext.sql("desc tmp_df")
sqlContext.sql("select * from tmp_df").show()
import pandas as pd
pd.DataFrame(myDF)
