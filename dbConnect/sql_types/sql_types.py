# -*- coding: utf-8 -*-
import datetime as dt
import json
from dbConnect.exceptions import InvalidDate

def unpack(obj, **kwargs):
    for k,v in kwargs.items():
        t = getattr(obj, k, 'NON-EXISTENT-VALUE')
        if t == 'NON-EXISTENT-VALUE':
            setattr(obj, k, v)

ESCAPE_CHARS = [
    # ('\\','\\\\'), ('%','%%'), ("'","''")
    ]

### TEXT
class VARCHAR:
    _type = str
    def __init__(self, 
                 value,
                 length = 255,
                 **kwargs):
        self._value = value
        self._length = kwargs.get('CHARACTER_MAXIMUM_LENGTH',length)
        self.nullable = kwargs.get('IS_NULLABLE', False)
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
        else:
            if isinstance(self._value,dict) or isinstance(self._value, list):
                v = json.dumps(self._value, default=str)
            v = self.clearEscapeChars()
            v = v[:min(len(v),self._length)]
            return v
    
    @property
    def SQL(self):
        if self.value is None and self.nullable:
            return 'NULL'
        return f"'{self.value}'"
    
    def __repr__(self):
        return f"'{self.value}'"


class CHAR:
    _type = str
    def __init__(self, 
                 value:str,
                 length = 255,
                 **kwargs
                 ):
        self._value = value
        self._length = kwargs.get('CHARACTER_MAXIMUM_LENGTH',length)
        self.nullable = kwargs.get('IS_NULLABLE', False)
        unpack(self, **kwargs)
        
    @property
    def value(self):
        if self._value is None:
            return None
        else:
            return str(self._value)[:min(len(str(self._value)),self._length)]
    
    @property
    def SQL(self):
        if self.value is None and self.nullable:
            return 'NULL'
        return f"'{self.value}'"
    
    def __repr__(self):
        return self.value


class STR(VARCHAR):
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)


class BLOB(VARCHAR): # CASE SENSITIVE
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)
        
    @property
    def value(self):
        if isinstance(self._value, dict):
            return json.dumps(self._value, default=str)
        else:
            return self._value

class TEXT(VARCHAR):
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)


class TINYBLOB(VARCHAR): # CASE SENSITIVE
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)
        
    @property
    def value(self):
        if isinstance(self._value, dict):
            return json.dumps(self._value, default=str)
        else:
            return self._value

class TINYTEXT(VARCHAR):
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)


class MEDIUMBLOB(BLOB): # CASE SENSITIVE
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)
        
    @property
    def value(self):
        if isinstance(self._value, dict):
            return json.dumps(self._value, default=str)
        else:
            return self._value

class MEDIUMTEXT(VARCHAR):
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)


class LONGBLOB(BLOB): # CASE SENSITIVE
    def __init__(self, value, **kwargs):
        super().__init__(value=value, **kwargs)
        
    @property
    def value(self):
        if isinstance(self._value, dict):
            return json.dumps(self._value, default=str)
        else:
            return self._value

class LONGTEXT(VARCHAR):
    def __init__(self, value, **kwargs):
        super().__init__(value=value, length=4_294_967_295, **kwargs)

### 
class ENUM(VARCHAR):
    def __init__(self, value:str, **kwargs):
        super().__init__(value=value,length=65535, **kwargs)
        # TODO include validator to make sure value is in the enum list





### INTEGERS
class INT:
    _type = int
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)
        
    @property
    def value(self):
        if not isinstance(self._value, self._type):
            if str(self._value).isnumeric():
                return int(round(float(self._value),0))
            else:
                return 0
        else:
            return self._value
    
    @property
    def SQL(self):
        return f"{self.value}"
    
    def __repr__(self):
        return f"{self.value}"

class TINYINT(INT):
    pass

class SMALLINT(INT):
    pass

class MEDIUMINT(INT):
    pass

class BIGINT(INT):
    pass

class BOOL(TINYINT):
    def __init__(self, value, **kwargs):
        if value:
            value = 1
        else:
            value = 0
        super().__init__(value=value)


### FLOATS
class FLOAT:
    _type = float
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)
        
    @property
    def value(self):
        test = str(self._value).replace('(','-').replace(')','').replace(',','').replace(' ','')
        accept = ['0','1','2','3','4','5','6','7','8','9','-','.']
        if not isinstance(self._value, self._type):
            if all([str(x) in accept for x in list(test)]):
                return float(test or 0)
            else:
                return 0
        else:
            return self._value
    
    @property
    def SQL(self):
        return f"{self.value}"
    
    def __repr__(self):
        return f"{self.value}"

class DECIMAL(FLOAT):
    pass

class DOUBLE(FLOAT):
    pass


### DATES & TIMES
class DATE:
    _type = dt.date
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)
        _=self.value # force check type
    
    @property
    def value(self):
        if isinstance(self._value, dt.datetime):
            return self._value.date()
        elif isinstance(self._value, self._type):
            return self._value
        elif ((self._value == '' or self._value is None or self._value == 'NULL') and 
              getattr(self, 'IS_NULLABLE', True)):
            return None # 'NULL'
        else:
            raise InvalidDate(f'"{self._value}" must be of type datetime.date')
    
    @property
    def SQL(self):
        if self.value == 'NULL':
            return self.value
        else:
            return f"'{self.value}'"
    
    def __repr__(self):
        return str(self.value)

class DATETIME:
    _type = dt.datetime
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)
        _=self.value # force check type
    
    @property
    def value(self):
        if isinstance(self._value, self._type):
            v = self._value
            v = v.replace(microsecond=0)
            return v
        elif ((self._value == '' or self._value == None or self._value == 'NULL') and 
              getattr(self, 'IS_NULLABLE', True)):
            return None
        else:
            raise InvalidDate(f'"{self._value}" must be of type datetime.datetime')
    
    @property
    def SQL(self):
        if self.value == 'NULL':
            return self.value
        else:
            return f"'{self.value}'"
    
    def __repr__(self):
        return str(self.value)

class TIMESTAMP:
    _type = dt.datetime
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)
        _=self.value # force check type
    
    @property
    def value(self):
        if isinstance(self._value, self._type):
            v = self._value
            v = v.replace(microsecond=0)
            v = str(v)
            # v = v.replace('-','').replace(' ','').replace(':','')
            return v
        elif ((self._value == '' or self._value == None or self._value == 'NULL') and 
              getattr(self, 'IS_NULLABLE', True)):
            return None
        else:
            raise InvalidDate(f'"{self._value}" must be of type datetime.datetime')
    
    @property
    def SQL(self):
        if self.value == 'NULL':
            return self.value
        else:
            return f"'{self.value}'"
    
    def __repr__(self):
        return str(self.value)

class TIME:
    _type = dt.datetime
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)
        _=self.value # force check type
    
    @property
    def value(self):
        if isinstance(self._value, self._type):
            v = self._value
            v = v.replace(microsecond=0)
            v = str(v).split(' ')[1]
            return v
        elif ((self._value == '' or self._value == None or self._value == 'NULL') and 
              getattr(self, 'IS_NULLABLE', True)):
            return None
        else:
            raise InvalidDate(f'"{self._value}" must be of type datetime.datetime')
    
    @property
    def SQL(self):
        if self.value == 'NULL':
            return self.value
        else:
            return f"'{self.value}'"
    
    def __repr__(self):
        return str(self.value)

class YEAR:
    _type = dt.datetime
    def __init__(self, value, **kwargs):
        self._value = value
        unpack(self, **kwargs)
        _=self.value # force check type
    
    @property
    def value(self):
        if isinstance(self._value, self._type):
            v = self._value
            return v.year
        elif ((self._value == '' or self._value == None or self._value == 'NULL') and 
              getattr(self, 'IS_NULLABLE', True)):
            return None
        else:
            raise InvalidDate(f'"{self._value}" must be of type datetime.datetime')
    
    @property
    def SQL(self):
        if self.value == 'NULL':
            return self.value
        else:
            return f"'{self.value}'"
    
    def __repr__(self):
        return str(self.value)
