from flask import Flask,request,jsonify
from DataclassScript import Valid
from MarshmallowValidations import valid_method
from  PysparkOperations import PysparkImplementation
from DatabaseOperations import find_data,insert_data

app = Flask(__name__)


@app.route("/path",methods=["post"])
def  perform_validations():
    response_data = request.get_json()
    dataclass_validations = Valid.from_dict(response_data)
    marsh_validations = valid_method(dataclass_validations)

    # calling the pyspark operation and validating the file by passing the path as the parameter
    pyspark_result = PysparkImplementation(marsh_validations["path"])
    data_dict = pyspark_result.verfication_data()

    # sending the data to the mongodb to store 
    store = insert_data(data_dict)
    return jsonify(store)

    

@app.route("/getdata",methods=["get"])
def get_data():
    path = request.args.get('path')
    return  jsonify(find_data(path))
   


if __name__ == "__main__":
    app.run(debug=True,port = 6000)
