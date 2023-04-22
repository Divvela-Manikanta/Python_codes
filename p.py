from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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
# new =df.toPandas()
# print(new['_c1'].astype("str"))
# print(df.select('_c1').show())
# new = df.select(df['_c1'],df['_c2'])
# print(new.show())
# print(new.collect())
# df=df.withColumnRenamed("_c1","data")
# df = df.withColumn("S.no",col("_c0"))      #creating the column with the same info of "_c0"
df2 = df.withColumn("S.no", 1 * col("_c0"))

df.withColumn("_c0",col("_c0").cast("Integer")).show()
drop_df= df.drop(df._c0>50)
print(df.show())
df.filter(~(df._c0>50)).show(truncate=False)
co   = df.groupBy("_c0").count()
df.distinct()
df.dropDuplicates()
print(df.show())
df.unionByName(df2,allowMissingColumns=True)
print(df.count())