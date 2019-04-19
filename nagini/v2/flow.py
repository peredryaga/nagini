# -*- coding: utf8 -*-
from __future__ import unicode_literals

from inspect import isclass

from nagini.v2.base import ClassWithFields
from nagini.v2.manager import JobManager


class Flow(ClassWithFields):
    heads = None

    def __init__(self, params, job_manager=JobManager):
        super(Flow, self).__init__(params=params)
        self.job_manager = job_manager(self.heads) if isclass(job_manager) else job_manager

    def configure(self):
        return self.params

    def run(self):
        params = self.configure()
        self.job_manager.run(params)
