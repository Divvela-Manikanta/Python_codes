from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Sparknew').getOrCreate()
print("spark: ", spark)

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

#Create Schema
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])
#Create empty DataFrame from empty RDD
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()


df = spark.read.csv(r"Sample-Spreadsheet-100-rows.csv")
print(df.collect())
print(df.show(truncate= False))
print("pythondf-------->")
new =df.toPandas()
print(new['_c1'].astype("str"))
print(df.select('_c1').show())
new = df.select(df['_c1'],df['_c2'])
print(new.show())
print(new.collect())

