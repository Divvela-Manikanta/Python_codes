from dataclasses import dataclass
from dataclass_wizard import JSONSerializable


@dataclass
class Valid(JSONSerializable):
    table1:str
    table2:str
    jointype:str
    columns:list
    columnstab1:list =None
    columnstab2:list =None
    wherecon:str = None

