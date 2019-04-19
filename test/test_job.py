# -*- coding: utf8 -*-
from __future__ import unicode_literals

import pytest

from nagini.v2.job import Job
from nagini.fields import BooleanField


class A(Job):
    def run(self):
        return 'Hello!'


class B(Job):
    def run(self):
        return 'Bye!'


class C(Job):
    requires = A


class D(Job):
    requires = [A, B]


class E(Job):
    requires = {'A': A, 'B': B}


@pytest.fixture(scope='module')
def jobs():
    jobs = {}
    a = jobs[A] = A()
    b = jobs[B] = B()
    jobs[C] = C()
    jobs[D] = D()
    jobs[E] = E()

    a.execute()
    b.execute()

    return jobs


def test_result(jobs):
    a = jobs[A]
    b = jobs[B]

    assert a.result == 'Hello!'
    assert b.result == 'Bye!'


def test_input(jobs):
    c = jobs[C]
    c.update_input(jobs)
    assert c.input == 'Hello!'

    d = jobs[D]
    d.update_input(jobs)
    assert d.input[0] == 'Hello!'
    assert d.input[1] == 'Bye!'

    e = jobs[E]
    e.update_input(jobs)
    assert e.input['A'] == 'Hello!'
    assert e.input['B'] == 'Bye!'


class TestException(Exception):
    pass


class OnFailureJob(Job):
    def on_failure(self):
        self._result = 'Sadness'

    def run(self):
        raise TestException


def test_on_failure():
    job = OnFailureJob()

    with pytest.raises(TestException):
        job.execute()

    assert job.result == 'Sadness'


class OnSuccessJob(Job):
    def on_success(self):
        self._result = 'Happiness'

    def run(self):
        return 'Override result!'


def test_on_success():
    job = OnSuccessJob()

    assert job.run() == 'Override result!'
    assert job.result is None

    job.execute()
    assert job.result == 'Happiness'


class ConfigureJob(Job):
    dance = BooleanField()

    def configure(self):
        self.params['dance'] = False

    def run(self):
        pass


def test_configure():
    params = {'dance': '1'}
    job = ConfigureJob(params=params)

    assert job.params.dance is True

    job.configure()
    assert job.params.dance is False

    job.params.dance = True
    job.execute()
    assert job.params.dance is False
