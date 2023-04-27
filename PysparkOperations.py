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
            df = spark.read.csv(fr"{self.path}",header=True,inferSchema=True)

            # checking the datatypes column and inserting the data to a temp dict and assign the whole data to main dict
            
            #checking the count for distinct values,finding min and max length of column,and finding the datatypes count like integer,float and boolean
            for col_data in df.columns:
                df_counts = df.select([count(when(isnan(col_data) | col(col_data).isNull() | col(col_data).contains("None"), col_data)).alias(col_data)])
                df_pands = df_counts.toPandas()
                mydict = df_pands.to_dict(orient='records')[0]
                verfication_result[col_data] = {}    # creating the empy dict for each column 
                verfication_result[col_data]["Count_for_null_values"] = mydict[col_data]

                df_distinct_count = df.select([countDistinct(col(col_data)).alias(col_data)])
                df_pands = df_distinct_count.toPandas()
                mydict = df_pands.to_dict(orient='records')[0]
                verfication_result[col_data]["count_of_distinct_values"] = mydict[col_data]

                if verfication_result[col_data]["count_of_distinct_values"] <=100 :
                    df_temp = df.groupby(col_data).agg(count('*').alias('count'))
                    df_temp=df_temp.withColumnRenamed(col_data,"value")
                    df_sql = df_temp.dropna()
                    df_pandas = df_sql.toPandas()
                    verfication_result[col_data]["Count_of_distinct_data"] = df_pandas.to_dict(orient='records')

                # for min length and max length
                min_length = df.agg(min(length(col_data))).collect()[0][0]
                max_length = df.agg(max(length(col_data))).collect()[0][0]
                verfication_result[col_data]["Minimum_and_Maximum_Length"] = {"min":min_length,"max":max_length}
                
                # # # for count of integer,float,boolean 
                integer_count = df.filter(col(col_data).rlike("^[0-9]+$")).count()
                float_count = df.filter(col(col_data).rlike("^\\d+\\.\\d+$")).count()
                boolean_count = df.filter((df[col_data] == "True") | (df[col_data] == "False")).count()
                verfication_result[col_data]["Count_of_inner_data_types"]= {"Integer Count":integer_count,"Float Count":float_count,"Boolean count":boolean_count}

            data = df.dtypes # list conting tuples
            for value in data:
                verfication_result[value[0]]["Data_Types"] =  value[1]

            return [verfication_result,1]
       
        except Exception as ex:
            return [{"message":"unable to process the request try with another path",
                    "status code":500
            },0]

