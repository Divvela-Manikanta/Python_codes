from marshmallow import Schema,validate,fields,ValidationError

class Validations(Schema):
    path = fields.Str(required=True)


def valid_method(response_dc):
    try:
        Validations().validate(response_dc)      
        return response_dc
    
    except ValidationError as ex:
        return({"Message":ex.messages,
                "status":False,
                "Satuscode":500
                })