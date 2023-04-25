from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("createdf").getOrCreate()


df_email = spark.read.csv(
    r"/home/albanero/Downloads/email-password-recovery-code.csv", header=True)
df_second = spark.read.csv(r"/home/albanero/Downloads/second.csv", header=True)


df_email = df_email.withColumnRenamed("Login email", "Login_email")
df_second = df_second.withColumnRenamed("Login email", "Login_email")

df_email.createOrReplaceTempView("Table1")
df_second.createOrReplaceTempView("Table2")

query = """
                      SELECT a.*, b.Location
                    FROM Table1 a 
                    LEFT JOIN Table2 b 
                    ON a.Login_email = b.Login_email;

                   """

sql_df = spark.sql(query)
sql_df.createOrReplaceTempView("Leftjoin")
left_drop_val = spark.sql("select * from Table2 where Login_email not in(select Login_email from Leftjoin ) ")

df_wise = sql_df.select("Login_email", "Identifier")
df_left = df_email.select("Login_email", "Identifier")
df_right = df_second.select("Login_email", "Identifier")
combined_df = df_wise.unionByName(df_left).unionByName(df_right)
combined_df= combined_df.dropDuplicates(keep=None)
print("done")
1