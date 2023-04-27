from flask import Flask,request,jsonify
from DataclassScript import Valid
from MarshmallowValidations import valid_method
from  PysparkOperations import PysparkImplementation
from DatabaseOperations import find_data,insert_data
from dataclasses import asdict

app = Flask(__name__)


@app.route("/path",methods=["post"])
def  perform_validations():
    response_data = request.get_json()
    dataclass_validations = Valid.from_dict(response_data)
    marsh_validations = valid_method(dataclass_validations)
    marsh_validations = asdict(marsh_validations)

    # calling the pyspark operation and validating the file by passing the path as the parameter
    pyspark_result = PysparkImplementation(marsh_validations["path"])
    data_dict = pyspark_result.verfication_data()
   
    # sending the data to the mongodb to store 
    if(data_dict[1]):  # checkin whether the pyspark operations are done properly 
        store = insert_data(data_dict[0])
        return jsonify(store)
    else:
        return jsonify(data_dict[0])
   
  

@app.route("/getdata",methods=["get"])
def get_data():
    path = request.args.get('path') # getting the path to find in the url 
    return  jsonify(find_data(path))
   


if __name__ == "__main__":
    app.run(debug=True,port = 6000)
