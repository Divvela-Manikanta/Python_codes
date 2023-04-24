from marshmallow import Schema,validate,fields,ValidationError

class Validations(Schema):
    table1 = fields.Str(required=True)
    table2 = fields.Str(required=True)
    jointype = fields.Str(required=True)
    columns = fields.List(fields.Str())
    columnstab1 = fields.List(fields.Str(),allow_none =True)
    columnstab2 = fields.List(fields.Str(),allow_none = True)
    wherecon = fields.Str(allow_none=True)

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