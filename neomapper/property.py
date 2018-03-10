import copy
import json

from collections import OrderedDict
from datetime import datetime


class PropertyManager:

    def __init__(self, properties=None, data_type='python',
                 allow_undefined=False):
        self._properties = {}
        self.properties = properties or {}
        self._data_type = data_type
        self.data_type = data_type
        self.allow_undefined = allow_undefined

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
    def data(self):
        data = {n: f.value for n, f in self.properties.items()}

        return OrderedDict(sorted(data.items()))

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

            prop = obj(value=value, data_type=self.data_type)

            if prop not in self.properties:
                self.properties[field] = prop

        return None

    def hydrate(self, **properties):
        for field, value in properties.items():
            self.__setitem__(field, value)

        return self

    def force_hydrate(self, **properties):
        for field, value in properties.items():
            prop = self.get_property(field, value)

            if not prop:
                continue

            reimmute = prop.immutable
            prop.immutable = False
            prop.value = value

            if reimmute:
                prop.immutable = True

        return self


class Property:
    default = None

    def __init__(self, value=None, data_type='python', default=None,
                 immutable=False):
        self.immutable = False
        self._value = value
        self._data_type = data_type
        self.data_type = data_type
        self.default = default or self.default
        self.immutable = immutable

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
