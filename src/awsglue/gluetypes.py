"""``awsglue.gluetypes`` (spec §5): minimal Glue type names so imports and
simple schema construction work. Not used by prytt for any real typing;
``DynamicFrame.schema()`` returns the Spark ``StructType``.
"""

__all__ = [
    "DataType",
    "AtomicType",
    "BinaryType",
    "BooleanType",
    "ByteType",
    "DateType",
    "DecimalType",
    "DoubleType",
    "FloatType",
    "IntegerType",
    "LongType",
    "NullType",
    "ShortType",
    "StringType",
    "TimestampType",
    "ArrayType",
    "ChoiceType",
    "MapType",
    "StructType",
    "EnumType",
    "UnknownType",
    "Field",
]


class DataType:
    def __init__(self, properties=None):
        self.properties = properties or {}

    @classmethod
    def typeName(cls):
        return cls.__name__[: -len("Type")].lower()

    def __repr__(self):
        return f"{type(self).__name__}({self.properties or ''})"

    def __eq__(self, other):
        return type(self) is type(other) and vars(self) == vars(other)

    def __hash__(self):
        return hash(type(self).__name__)


class AtomicType(DataType):
    pass


def _atomic(name):
    return type(name, (AtomicType,), {"__module__": __name__})


BinaryType = _atomic("BinaryType")
BooleanType = _atomic("BooleanType")
ByteType = _atomic("ByteType")
DateType = _atomic("DateType")
DoubleType = _atomic("DoubleType")
FloatType = _atomic("FloatType")
IntegerType = _atomic("IntegerType")
LongType = _atomic("LongType")
NullType = _atomic("NullType")
ShortType = _atomic("ShortType")
StringType = _atomic("StringType")
TimestampType = _atomic("TimestampType")
UnknownType = _atomic("UnknownType")


class DecimalType(AtomicType):
    def __init__(self, precision=10, scale=2, properties=None):
        super().__init__(properties)
        self.precision = precision
        self.scale = scale


class EnumType(DataType):
    def __init__(self, options=(), properties=None):
        super().__init__(properties)
        self.options = list(options)


class ArrayType(DataType):
    def __init__(self, elementType=None, properties=None):
        super().__init__(properties)
        self.elementType = elementType


class MapType(DataType):
    def __init__(self, valueType=None, properties=None):
        super().__init__(properties)
        self.valueType = valueType


class Field:
    def __init__(self, name, dataType, properties=None):
        self.name = name
        self.dataType = dataType
        self.properties = properties or {}

    def __repr__(self):
        return f"Field({self.name}, {self.dataType!r})"

    def __eq__(self, other):
        return isinstance(other, Field) and (self.name, self.dataType) == (other.name, other.dataType)

    def __hash__(self):
        return hash(self.name)


class StructType(DataType):
    def __init__(self, fields=None, properties=None):
        super().__init__(properties)
        self.fields = list(fields or [])

    def add(self, field):
        self.fields.append(field)
        return self

    def getField(self, name):
        return next((f for f in self.fields if f.name == name), None)

    def hasField(self, field):
        name = field.name if isinstance(field, Field) else field
        return self.getField(name) is not None


class ChoiceType(DataType):
    def __init__(self, choices=None, json_name=None, properties=None):
        super().__init__(properties)
        self.choices = {c.typeName(): c for c in (choices or [])}
        self.json_name = json_name
