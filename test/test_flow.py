# -*- coding: utf8 -*-
from __future__ import unicode_literals

import pytest
from six.moves import range

from nagini.fields import IntField
from nagini.v2.flow import Flow
from nagini.v2.job import Job


class A(Job):
    value = IntField()

    def run(self):
        return self.params.value


class B(Job):
    requires = A
    value = IntField()

    def __init__(self, *args, **kwargs):
        super(B, self).__init__(*args, **kwargs)
        self.counter = 0

    def configure(self):
        self.params.value += 3

    def run(self):
        self.counter += 1
        return self.input * 2 + self.params.value


class C(Job):
    requires = {'A': A, 'B': B}

    def run(self):
        return self.input['B'] + 2 - self.input['A']


class D(Job):
    requires = [B]

    def run(self):
        return self.input[0] * 3


class TestFlow(Flow):
    value = IntField()
    heads = (D, C)

    def configure(self):
        params = super(TestFlow, self).configure()
        params['value'] += 1
        return params


@pytest.mark.parametrize('value', list(range(10)))
def test_flow(value):
    params = {'value': value}

    flow = TestFlow(params=params)
    flow.run()

    changed_value = value + 1
    assert flow.job_manager.jobs[C].result == (changed_value * 2 + changed_value + 3) + 2 - changed_value
    assert flow.job_manager.jobs[D].result == (changed_value * 2 + changed_value + 3) * 3

    # Checking the count of executions B. It should be execute only 1 time
    assert flow.job_manager.jobs[B].counter == 1
