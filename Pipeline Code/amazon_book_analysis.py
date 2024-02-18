
import pyspark as sc
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import translate

#Reading Data using SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
data = spark.read.option("header",True).options(inferSchema='True',delimiter=',').csv("gs://bucket-amazonn/bestsellers with categories.csv" )

data.show(truncate=False)
data.printSchema()

#Replacing ‘,’ with ‘;’ in the name column
data = data.withColumn('Name', translate('Name', ',', ';'))
data.show(truncate=False)

#Find averages of fiction , non- fiction and overall

data.filter(data['Genre']=='Fiction').agg({'User Rating' : 'avg'}).show()
data.filter(data['Genre']=='Non Fiction').agg({'User Rating' : 'avg'}).show()
data.agg({'User Rating': 'avg'}).show()

avg_fict = data.filter(data['Genre']=='Fiction').agg({'User Rating' : 'avg'})
avg_non_fict = data.filter(data['Genre']=='Non Fiction').agg({'User Rating' : 'avg'})
avg_all = data.agg({'User Rating' : 'avg'})

avg_fict.write.csv('gs://bucket-amazonn/Avg_of_Fiction')
avg_non_fict.write.csv('gs://bucket-amazonn/Avg_of_Non_Fiction')
avg_all.write.csv('gs://bucket-amazonn/Avg_of_Allbooks')

data.orderBy(data.Name.desc()).limit(5).show(truncate=False)

data.write.csv('gs://bucket-amazonn/clean_books_amazon.csv')


