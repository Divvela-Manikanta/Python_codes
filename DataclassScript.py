from dataclasses import dataclass
from dataclass_wizard import JSONSerializable


@dataclass
class Valid(JSONSerializable):
    path:str
   