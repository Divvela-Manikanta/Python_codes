from pyspark.sql import SparkSession


print("******************88")
spark = SparkSession.builder \
    .appName('MyApp') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()

print("spark: ", spark)


emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

# from py4j.java_gateway import JavaGateway, GatewayParameters

# gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_field="MY_PORT_FIELD"))


# print(gateway)