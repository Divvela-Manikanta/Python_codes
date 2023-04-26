from pyspark.sql import SparkSession 
from pyspark.sql.functions import count,isnan,col,when,countDistinct,length,min,max,regexp_replace
import pandas as pd

spark = SparkSession.builder.appName("check").getOrCreate()


class PysparkImplementation():
    def __init__(self,path) -> None:
        self.path = path 
    

    def verfication_data(self):
        verfication_result ={}
        verfication_result["path"] = self.path
        df = spark.read.csv(fr"{self.path}",header=True)

        df_counts = df.select([count(when(isnan(c) | col(c).isNull() | col(c).contains("None"), c)).alias(c) for c in df.columns])
        df_pands = df_counts.toPandas()
        mydict = df_pands.to_dict(orient='records')
        verfication_result['Count of Null'] = mydict[0]

        df_distinct_count = df.select([countDistinct(col(col_name)).alias(col_name) for col_name in df.columns])
        df_pands = df_distinct_count.toPandas()
        mydict = df_pands.to_dict(orient='records')
        verfication_result['Count for distinct values'] = mydict[0]
        
        temp = {}
        data = df.dtypes
        for value in data:
            temp[value[0]] = value[1]
        verfication_result["Data Types For colums"] = temp 


        temp_count_data ={}
        temp_min_max = {}
        temp_data = {}
        df.createOrReplaceTempView("Table")
        for col_data in df.columns:
            if(verfication_result["Count for distinct values"][col_data]<=100):
                df_sql = spark.sql(f"select {col_data} as data ,count({col_data}) as count from Table group by {col_data}")
                df_sql = df_sql.dropna()
                df_pandas = df_sql.toPandas()
                temp_count_data[col_data] = df_pandas.to_dict(orient='records')

            min_length = df.agg(min(length(col_data))).collect()[0][0]
            max_length = df.agg(max(length(col_data))).collect()[0][0]
            temp_min_max[col_data] = {"min":min_length,"max":max_length}

            integer_count = df.filter(col(col_data).rlike("^[0-9]+$")).count()
            float_count = df.filter(col(col_data).rlike("^\\d+\\.\\d+$")).count()
            boolean_count = df.filter((df[col_data] == "True") | (df[col_data] == "False")).count()
            temp_data[col_data] = {"Integer Count":integer_count,"Float Count":float_count,"Boolean count":boolean_count}

        verfication_result["Count of data in each column"] = temp_count_data 
        verfication_result["Minimum and Maximum Length column wise"] = temp_min_max 
        verfication_result["Count based on data types"] = temp_data
       

        return (verfication_result)
