from pymongo import MongoClient

conn = MongoClient("localhost",27017)
db = conn.assignment
collection = db.result


def insert_data(data): # Inserting data to the mongo db
    try:
        check = collection.insert_one(data)
        return ({"Meassage":"Data is inserted successfully.",
                        "Success": True,
                        "Status":200})
    except Exception as ex:
        return ({"Meassage":"Unable to insert the data",
                        "Success": False,
                        })
    
def find_data(data): # finding the data in mongo db based on the path 
    try:
        get = collection.find_one({"path":data},{"_id":0,"path":0})
        if get == None:
            return ({"Meassage":"Their is no data with the specified path",
                        "Success": True,
                        "Status":200})
        else:
            return get
    except Exception as ex:
        return {"Meassage":"Unable to show the data",
                        "Success": False,
                        "Status":500
                        }

    
        