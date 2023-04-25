from marshmallow import Schema,validate,fields,ValidationError

class Validations(Schema):
    colum_list = fields.List(fields.Str(),allow_none =True)


def valid_method(response_dc):
    try:
        obj_val = Validations()
        loded_data = obj_val.dump(response_dc)
        valid_data = obj_val.load(loded_data)
        return valid_data
    except ValidationError as ex:
        return({"Message":ex.messages,
                "status":False,
                "Satuscode":500
                })