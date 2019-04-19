# -*- coding: utf8 -*-
from __future__ import unicode_literals

import pytest
from six.moves import range

from nagini.fields import IntField
from nagini.v2.job import Job
from nagini.v2.manager import JobManager


class A(Job):
    value = IntField()

    def run(self):
        return self.params.value


class B(Job):
    requires = A
    value = IntField()

    def configure(self):
        self.params.value += 3

    def run(self):
        return self.input * 2 + self.params.value


class C(Job):
    requires = {'A': A, 'B': B}

    def run(self):
        return self.input['B'] + 2 - self.input['A']


@pytest.mark.parametrize('value', list(range(10)))
def test_manager(value):
    params = {'value': value}

    manager = JobManager(C)
    manager.run(params)

    assert manager.jobs[C].result == (value * 2 + value + 3) + 2 - value
