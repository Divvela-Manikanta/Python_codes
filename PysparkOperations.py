from pyspark.sql import SparkSession 
from pyspark.sql.functions import count,isnan,col,when,countDistinct,length,min,max,regexp_replace


spark = SparkSession.builder.appName("check").getOrCreate()


class PysparkImplementation():
    def __init__(self,path) -> None:
        self.path = path 
    

    def verfication_data(self):
        verfication_result ={}
        df = spark.read.csv(fr"{self.path}",header=True)

        df_counts = df.select([count(when(isnan(c) | col(c).isNull() | col(c).contains("None"), c)).alias(c) for c in df.columns])
        df_pands = df_counts.toPandas()
        mydict = df_pands.to_dict(orient='records')
        verfication_result['Count of Null'] = mydict

        df_distinct_count = df.select([countDistinct(col(col_name)).alias(col_name) for col_name in df.columns])
        df_pands = df_distinct_count.toPandas()
        mydict = df_pands.to_dict(orient='records')
        verfication_result['Count for distinct values'] = mydict
        
        temp = {}
        data = df.dtypes
        print(type(data[0]))
        for value in data:
            temp[value[0]] = value[1]
        verfication_result["Data Types For colums"] = temp 


        # temp ={}
        # df.createOrReplaceTempView("Table")
        # for col_data in df.columns:
        #     df_sql = spark.sql(f"select {col_data} as data ,count({col_data}) as count from Table group by {col_data}")
        #     df_sql = df_sql.dropna()
        #     df_pandas = df_sql.toPandas()
        #     temp[col_data] = df_pandas.iloc[1:]
        # verfication_result["Count of data in each column"] = temp 

        temp = {}   
        for column in df.columns:
            min_length = df.agg(min(length(column))).collect()[0][0]
            max_length = df.agg(max(length(column))).collect()[0][0]
            temp[column] = {"min":min_length,"max":max_length}
        verfication_result["Minimum and Maximum Length column wise"] = temp 
        
        for column2 in df.columns:
            if df.select(column2).dtypes[0][1] in ['int', 'double', 'float']:
                integer_count = df.select(sum(regexp_replace(col(column2), "[^0-9]", "").cast("int"))).collect()[0][0]
                float_count = df.select(sum(regexp_replace(col(column2), "[^0-9\.]", "").cast("float"))).collect()[0][0]
       
        print((verfication_result))
        return (verfication_result)

