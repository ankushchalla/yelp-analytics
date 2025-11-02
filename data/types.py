from typing import NamedTuple
from enum import Enum
from dataclasses import dataclass
import shapely.geometry
import shapely.wkt

class YelpJsonFields(Enum):
    BUSINESS_ID = 'business_id'
    NAME = 'name'
    ADDRESS = 'address'
    CITY = 'city'
    STATE = 'state'
    POSTAL_CODE = 'postal_code'
    LATITUDE = 'latitude'
    LONGITUDE = 'longitude'
    ATTRIBUTES = 'attributes'
    CATEGORIES = 'categories'

class Point(NamedTuple):
    latitude: float
    longitude: float

class Address(NamedTuple):
    business_id: str
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    latitude: float
    longitude: float
    # coordinates: str

def to_address(json) -> Address:
    coordinates = shapely.geometry.Point(json[YelpJsonFields.LATITUDE.value], json[YelpJsonFields.LONGITUDE.value])
    return Address(
        business_id=json[YelpJsonFields.BUSINESS_ID.value],
        name=json[YelpJsonFields.NAME.value],
        address=json[YelpJsonFields.ADDRESS.value], 
        city=json[YelpJsonFields.CITY.value], 
        state=json[YelpJsonFields.STATE.value],
        latitude=float(json[YelpJsonFields.LATITUDE.value]),
        longitude=float(json[YelpJsonFields.LONGITUDE.value]),
        # coordinates=shapely.wkt.dumps(coordinates),
        postal_code=json[YelpJsonFields.POSTAL_CODE.value]
    )

class Attribute(NamedTuple):
    business_id: str
    name: str
    value: str

def to_attribute_list(json) -> list[Attribute] | None:
    if json.get(YelpJsonFields.ATTRIBUTES.value) is None:
        return None
    attrs = json.get(YelpJsonFields.ATTRIBUTES.value).items()
    if (len(attrs) == 0):
        return None
    
    return [Attribute(json[YelpJsonFields.BUSINESS_ID.value], attr_name, attr_value) for (attr_name, attr_value) in attrs]

class Category(NamedTuple):
    business_id: str
    name: str

def to_category_list(json) -> list[Category] | None:
    if json.get(YelpJsonFields.CATEGORIES.value) is None:
        return None
    categories = json.get(YelpJsonFields.CATEGORIES.value).split(',')
    if (len(categories) == 0):
        return None
    
    return [Category(json[YelpJsonFields.BUSINESS_ID.value], category.lstrip()) for category in categories]

class Date(NamedTuple):
    year: int
    month: int
    day: int

@dataclass(frozen=True)
class BusinessRecord:
    address: Address
    attributes: list[Attribute] | None
    categories: list[Category] | None

        
