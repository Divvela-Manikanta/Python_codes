from flask import Flask,request,jsonify
from DataclassScript import Valid
from MarshmallowValidations import valid_method
from pyspark.sql import SparkSession


app = Flask(__name__)
spark = SparkSession.builder.appName('dedups').getOrCreate()


@app.route("/",methods=["POST"])
def deduo_method():
    try:
        response = request.get_json()
        obj_dataclass = Valid.from_dict(response)
        marsh_response = valid_method(obj_dataclass)
        df = spark.read.csv("/home/albanero/Downloads/second.csv",header=True)
        
        if(marsh_response['colum_list']==None):
            df_without_duplicates = df.dropDuplicates()  
            print("---------->data with no duplicates------------------")
            df_without_duplicates.show()
        
        else:
            df_without_duplicates = df.dropDuplicates(subset=marsh_response['colum_list'])
            print("---------->data with no duplicates------------------")
            df_without_duplicates.show()
    
        print("---------->droped duplicates------------------")
        df_droped_duplicates=df.exceptAll(df_without_duplicates)
        (df_droped_duplicates.show())
        
        df_without_duplicates.write.csv("/home/albanero/Downloads/without_duplicted.csv")
        df_droped_duplicates.write.csv("/home/albanero/Downloads/dropped_duplicates.csv")

        return jsonify({"message":"The operation has been performed",
                "status":True})
   
    except Exception as ex:
        return jsonify({"message":"Something went worng",
                "status":False,
                "statuscode":500})


if __name__ == "__main__":
    app.run(debug=True)         




