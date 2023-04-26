from pyspark.sql import SparkSession 
from pyspark.sql.functions import count,isnan,col,when,countDistinct,length,min,max,regexp_replace
import pandas as pd

spark = SparkSession.builder.appName("check").getOrCreate()


class PysparkImplementation():
    def __init__(self,path) -> None:
        self.path = path 
    

    def verfication_data(self):
        """
         In this we have perform the operation on the file and tarnsfer the dictionary to the app.py 
         In the whole process we are store the data for each validation in a temp dict and finally sending the data to the main dict verfication_result.
        """
        try:
            # Inserting entire data in the verfication_result
            verfication_result ={} 
            verfication_result["path"] = self.path
            df = spark.read.csv(fr"{self.path}",header=True)
            
            # check whether containg null values based columns using for loop 
            df_counts = df.select([count(when(isnan(c) | col(c).isNull() | col(c).contains("None"), c)).alias(c) for c in df.columns])
            df_pands = df_counts.toPandas()
            mydict = df_pands.to_dict(orient='records')
            verfication_result['Count of Null'] = mydict[0]

            # check count for distinct values based columns using for loop 
            df_distinct_count = df.select([countDistinct(col(col_name)).alias(col_name) for col_name in df.columns])
            df_pands = df_distinct_count.toPandas()
            mydict = df_pands.to_dict(orient='records')
            verfication_result['Count for distinct values'] = mydict[0]
            
            # checking the datatypes column and inserting the data to a temp dict and assign the whole data to main dict 
            temp = {}
            data = df.dtypes # list conting tuples
            for value in data:
                temp[value[0]] = value[1]
            verfication_result["Data Types For colums"] = temp 

            #checking the count for distinct values,finding min and max length of column,and finding the datatypes count like integer,float and boolean
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

                # for min length and max length
                min_length = df.agg(min(length(col_data))).collect()[0][0]
                max_length = df.agg(max(length(col_data))).collect()[0][0]
                temp_min_max[col_data] = {"min":min_length,"max":max_length}
                
                # for count of integer,float,boolean 
                integer_count = df.filter(col(col_data).rlike("^[0-9]+$")).count()
                float_count = df.filter(col(col_data).rlike("^\\d+\\.\\d+$")).count()
                boolean_count = df.filter((df[col_data] == "True") | (df[col_data] == "False")).count()
                temp_data[col_data] = {"Integer Count":integer_count,"Float Count":float_count,"Boolean count":boolean_count}

            verfication_result["Count of data in each column"] = temp_count_data 
            verfication_result["Minimum and Maximum Length column wise"] = temp_min_max 
            verfication_result["Count based on data types"] = temp_data
        
            return [verfication_result,1]
        except Exception as ex:
            return [{"message":"unable to process the request try with another path",
                     "status code":500
            },0]
  