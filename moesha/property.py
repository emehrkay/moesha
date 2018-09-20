import copy
import json

from collections import OrderedDict
from datetime import datetime


class PropertyManager(object):

    def __init__(self, properties=None, data_type='python',
                 allow_undefined=False):
        self._properties = {}
        self.properties = properties or {}
        self._data_type = data_type
        self.data_type = data_type
        self.allow_undefined = allow_undefined

    def reset(self):
        for f, p in self.properties.items():
            p.reset()

        return self

    def safely_stringify_for_pudb(self):
        return None

    def _set_data_type(self, data_type):
        self._data_type = data_type

        for name, field in self.properties.items():
            field.data_type = data_type

    def _get_data_type(self):
        return self._data_type

    data_type = property(_get_data_type, _set_data_type)

    def _get_properties(self):
        return self._properties

    def _set_properties(self, properties):
        for name, field in properties.items():
            self._properties[name] = field

    properties = property(_get_properties, _set_properties)

    @property
    def default_data(self):
        data = {n: f.value for n, f in self.properties.items()}

        return OrderedDict(sorted(data.items()))

    def data(self, properties=None):
        default = copy.copy(self.default_data)
        default.update(properties or {})
        data = {}

        for name, value in default.items():
            prop = self.get_property(name, value)

            if prop:
                prop.value = value
                data[name] = prop.value

        return data

    def __getitem__(self, field):
        if not field in self.properties:
            return None

        return self.properties[field].value

    def __setitem__(self, field, value):
        prop = self.get_property(field, value)

        if prop:
            prop.value = value

    def __delitem__(self, field):
        if field in self.properties:
            del(self.properties[field])

    def get_property(self, field, value=None):
        if field in self.properties:
            return self.properties[field]

        if self.allow_undefined:
            if isinstance(value, bool):
                obj = Boolean
            elif isinstance(value, str):
                obj = String
            elif isinstance(value, int):
                obj = Integer
            elif isinstance(value, float):
                obj = Float
            elif isinstance(value, datetime):
                obj = DateTime

            prop = obj(value=value, data_type=self.data_type, name=field)

            if prop not in self.properties:
                self.properties[field] = prop

        return prop


class Property(object):
    default = None

    def __init__(self, value=None, data_type='python', default=None,
                 immutable=False, name=None):
        self.immutable = False
        self.name = name
        self._value = value
        self._original_value = value
        self._data_type = data_type
        self.data_type = data_type
        self.default = default or self.default
        self.immutable = immutable

    def reset(self):
        self._value = self._original_value or self.default

        return self

    def _set_data_type(self, data_type):
        self._data_type = data_type

    def _get_data_type(self):
        return self._data_type

    data_type = property(_get_data_type, _set_data_type)

    def _get_value(self):
        if callable(self._value):
            value = self._value()
        else:
            value = self._value

        if not value and self.default:
            if callable(self.default):
                value = self.default()
            else:
                value = self.default

        if self.data_type == 'python':
            return self.to_python(value)

        return self.to_graph(value)

    def _set_value(self, value):
        if self.immutable:
            return

        self._value = value

    value = property(_get_value, _set_value)

    def to_python(self, value):
        return value

    def to_graph(self, value):
        return self.to_python(value)


class String(Property):

    def to_python(self, value):
        if not value:
            return ''

        return str(value)


class Integer(Property):
    default = 0

    def to_python(self, value):
        try:
            return int(float(value))
        except:
            return self.default


class Increment(Integer):

    def to_graph(self, value):
        self._value = self.to_python(value)
        self._value += 1

        return self._value


class Float(Property):
    default = 0.0

    def to_python(self, value):
        try:
            return float(value)
        except:
            return self.default


class Boolean(Property):
    default = False

    def _convert(self, value):
        if str(value).lower().strip() not in ['true', 'false']:
            value = bool(value)

        value = str(value).lower().strip()

        return bool(json.loads(value))

    def to_python(self, value):
        try:
            return self._convert(value)
        except:
            return False


class DateTime(Float):

    def to_python(self, value):
        if isinstance(value, datetime):
            value = value.timestamp()

        return super().to_python(value=value)


class TimeStamp(DateTime):

    def __init__(self, value=None, **kwargs):

        def default():
            return datetime.now()

        super().__init__(value=value, default=default, immutable=True)
