# -*- coding: utf8 -*-
from __future__ import unicode_literals

from abc import ABCMeta

from six import iteritems

from nagini.errors import NaginiError
from nagini.fields import BaseField


class DuplicateFieldName(NaginiError):
    pass


class AttributeDict(dict):
    def __getattr__(self, item):
        return self[item]

    def __setattr__(self, key, value):
        self[key] = value


class MetaClassWithFields(ABCMeta):
    def __new__(mcs, name, bases, namespace):
        fields = set()
        for base in bases:
            fields.update(base.__dict__.get('base_fields', []))

        for field_name, field in iteritems(namespace):
            if isinstance(field, BaseField):
                if field.name is None:
                    field.name = field_name
                fields.add(field)
        namespace['base_fields'] = fields
        return super(MetaClassWithFields, mcs).__new__(mcs, name, bases, namespace)


class ClassWithFields(object):
    __metaclass__ = MetaClassWithFields
    base_fields = None

    def __init__(self, params=None):
        self.params = AttributeDict(self.clean_params(params or {}))
        self.global_params = params

    @classmethod
    def clean_params(cls, params):
        cleaned_data = {}
        for field in cls.base_fields:
            value = field.value_from_dict(params)

            if field.name in cleaned_data:
                raise DuplicateFieldName(field.name)

            cleaned_data[field.name] = field.to_python(value)
        return cleaned_data
