import datetime
from json import JSONEncoder

from bson.objectid import ObjectId


class MyEncoder(JSONEncoder):
    """JSONEncoder for types."""
    def default(self, obj):

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)

        return JSONEncoder.default(self, obj)