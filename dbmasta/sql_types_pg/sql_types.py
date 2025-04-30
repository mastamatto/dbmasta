# -*- coding: utf-8 -*-
import datetime as dt
import json
import uuid
from dbmasta.exceptions import InvalidDate


def unpack(obj, **kwargs):
    for k,v in kwargs.items():
        t = getattr(obj, k, 'NON-EXISTENT-VALUE')
        if t == 'NON-EXISTENT-VALUE':
            setattr(obj, k, v)


ESCAPE_CHARS = []


### TEXT TYPES
class CHAR:
    _type = str
    def __init__(self, value:str, length=1, **kwargs):
        self._value = value
        self._length = kwargs.get('CHARACTER_MAXIMUM_LENGTH', length)
        self.nullable = kwargs.get('IS_NULLABLE', False)
        unpack(self, **kwargs)

    @property
    def value(self):
        if self._value is None:
            return None
        return str(self._value)[:min(len(str(self._value)), self._length)]

    @property
    def SQL(self):
        if self.value is None and self.nullable:
            return 'NULL'
        return f"'{self.value}'"

    def __repr__(self):
        return self.value



class VARCHAR:
    _type = str
    def __init__(self, value, length=255, **kwargs):
        self._value = value
        self._length = kwargs.get('character_maximum_length', length)
        self.nullable = kwargs.get('is_nullable', False)
        unpack(self, **kwargs)

    def clearEscapeChars(self):
        v = str(self._value)
        for x,y in ESCAPE_CHARS:
            v = v.replace(x,y)
        return v

    @property
    def value(self):
        if self._value is None:
            return None
        if isinstance(self._value, (dict, list)):
            v = json.dumps(self._value, default=str)
        else:
            v = self.clearEscapeChars()
        return v[:self._length]

    @property
    def SQL(self):
        if self.value is None and self.nullable:
            return 'NULL'
        return f"'{self.value}'"

    def __repr__(self):
        return f"'{self.value}'"


class TEXT(VARCHAR):
    def __init__(self, value, **kwargs):
        super().__init__(value=value, length=65535, **kwargs)

class STR(VARCHAR):
    pass

class ENUM(VARCHAR):
    def __init__(self, value:str, allowed:list[str]=None, **kwargs):
        super().__init__(value=value, length=65535, **kwargs)
        if allowed and value not in allowed:
            raise ValueError(f"Value '{value}' not in allowed ENUM: {allowed}")


### INTEGER TYPES
class INT:
    _type = int
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        try:
            return int(float(self._value))
        except:
            return 0

    @property
    def SQL(self):
        return f"{self.value}"

    def __repr__(self):
        return f"{self.value}"

class TINYINT(INT): pass
class SMALLINT(INT): pass
class BIGINT(INT): pass

class BOOL:
    def __init__(self, value, **kwargs):
        self._value = bool(value)

    @property
    def value(self):
        return self._value

    @property
    def SQL(self):
        return 'TRUE' if self.value else 'FALSE'

    def __repr__(self):
        return str(self.value)


### FLOAT TYPES
class FLOAT:
    _type = float
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        try:
            return float(str(self._value).replace('(','-').replace(')','').replace(',','').replace(' ',''))
        except:
            return 0.0

    @property
    def SQL(self):
        return f"{self.value}"

    def __repr__(self):
        return f"{self.value}"

class DECIMAL(FLOAT): pass
class DOUBLE(FLOAT): pass


### DATE & TIME TYPES
class DATE:
    _type = dt.date
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        if isinstance(self._value, dt.datetime):
            return self._value.date()
        if isinstance(self._value, self._type):
            return self._value
        if not self._value:
            return None
        raise InvalidDate(f"'{self._value}' must be date")

    @property
    def SQL(self):
        return 'NULL' if self.value is None else f"'{self.value}'"

    def __repr__(self):
        return str(self.value)


class TIMESTAMP:
    _type = dt.datetime
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        if isinstance(self._value, self._type):
            return self._value.replace(microsecond=0)
        if not self._value:
            return None
        raise InvalidDate(f"'{self._value}' must be datetime")

    @property
    def SQL(self):
        return None if self.value is None else f"'{self.value}'"

    def __repr__(self):
        return str(self.value)


class TIMESTAMPTZ(TIMESTAMP):  # Timestamp with time zone
    def __init__(self, value, **kwargs):
        super().__init__(value, **kwargs)


class DATETIME(TIMESTAMP):
    pass

class TIME:
    _type = dt.time
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        if isinstance(self._value, self._type):
            return self._value
        if isinstance(self._value, dt.datetime):
            return self._value.time()
        if not self._value:
            return None
        raise InvalidDate(f"'{self._value}' must be time")

    @property
    def SQL(self):
        return 'NULL' if self.value is None else f"'{self.value}'"

    def __repr__(self):
        return str(self.value)


class YEAR:
    _type = int
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        if isinstance(self._value, dt.datetime):
            return self._value.year
        if isinstance(self._value, int):
            return self._value
        if not self._value:
            return None
        raise InvalidDate(f"'{self._value}' must be datetime or int")

    @property
    def SQL(self):
        return 'NULL' if self.value is None else f"'{self.value}'"

    def __repr__(self):
        return str(self.value)



class JSONTYPE:
    _type = dict
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        if isinstance(self._value, str):
            try:
                return json.loads(self._value)
            except json.JSONDecodeError:
                return {}
        elif isinstance(self._value, dict) or isinstance(self._value, list):
            return self._value
        return {}

    @property
    def SQL(self):
        return f"'{json.dumps(self.value)}'"

    def __repr__(self):
        return str(self.value)
    
    
    
class JSONB:
    _type = dict

    def __init__(self, value, **kwargs):
        self._value = value
        self.nullable = kwargs.get('is_nullable', True)
        unpack(self, **kwargs)

    @property
    def value(self):
        if isinstance(self._value, str):
            try:
                return json.loads(self._value)
            except json.JSONDecodeError:
                return {}
        elif isinstance(self._value, (dict, list)):
            return self._value
        return {}

    @property
    def SQL(self):
        if self.value is None and self.nullable:
            return 'NULL'
        return f"'{json.dumps(self.value)}'"

    def __repr__(self):
        return str(self.value)
    
    
class POINT:
    _type = tuple
    def __init__(self, value, **kwargs):
        self._value = value  # Should be tuple (x, y)
        unpack(self, **kwargs)

    @property
    def value(self):
        if isinstance(self._value, (tuple, list)) and len(self._value) == 2:
            return tuple(map(float, self._value))
        return None

    @property
    def SQL(self):
        if self.value:
            return f"'({self.value[0]}, {self.value[1]})'"
        return 'NULL'

    def __repr__(self):
        return str(self.value)


class NUMERIC(DECIMAL): pass


class MONEY(FLOAT):
    pass


class BYTEA:
    _type = bytes
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        return self._value

    @property
    def SQL(self):
        return f"E'\\x{self._value.hex()}'" if self._value else 'NULL'

    def __repr__(self):
        return str(self._value)




class UUID:
    _type = str
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)

    @property
    def value(self):
        try:
            return str(uuid.UUID(str(self._value)))
        except Exception:
            return None

    @property
    def SQL(self):
        return f"'{self.value}'" if self.value else 'NULL'

    def __repr__(self):
        return str(self.value)
