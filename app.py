from flask import Flask,request,jsonify
from DataclassScript import Valid
from MarshmallowValidations import valid_method
from pyspark.sql import SparkSession

app = Flask(__name__)
spark = SparkSession.builder.appName('newclass').getOrCreate()


@app.route("/",methods=["POST"])
def check():
    response = request.get_json()
    obj_dataclass = Valid.from_dict(response)
    marsh_response = valid_method(obj_dataclass)
    try:
        df1 = spark.read.csv(rf"{marsh_response['table1']}",header=True)
        df2 = spark.read.csv(rf"{marsh_response['table2']}",header=True)
        print("------------------>read the data<------------------")
        df1=df1.drop(*marsh_response['columnstab1'])  
        df2=df2.drop(*marsh_response['columnstab2'])
        print("------------------>deleting the columns<------------------")
        df1.createOrReplaceTempView("tab1")
        df2.createOrReplaceTempView("tab2")
        print("------------------>created temp tables<------------------")
     
        if(marsh_response['wherecon']==None):
            resultent_df = spark.sql(f"select a.*,b.* from tab1 as a {marsh_response['jointype']} tab2 as b on a.{marsh_response['columns'][0]}=b.{marsh_response['columns'][1]}")
            print(resultent_df.show())
            # resultent_df.write.format("csv").save("/home/albanero/Downloads/outputfile.csv")
            print("exported")
        else:
            data =marsh_response["wherecon"]
            resultent_df = spark.sql(f"select table1.*,table2.* from tab1 as table1 {marsh_response['jointype']} tab2 as table2 on table1.{marsh_response['columns'][0]}=table2.{marsh_response['columns'][1]} where {data}")
        
       
        return("done")
    except Exception as ex:
        return jsonify({"Message":"please find the path you have entered is correct",
               "statue":False})
    
    return marsh_response 


if __name__ == "__main__":
    app.run(debug=True)