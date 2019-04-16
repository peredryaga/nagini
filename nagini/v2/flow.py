# -*- coding: utf8 -*-
from __future__ import unicode_literals

from inspect import isclass

from nagini.v2.base import ClassWithFields
from nagini.v2.manager import JobManager


class Flow(ClassWithFields):
    requires = None
    head_job = None

    def __init__(self, params, job_manager=JobManager):
        super(Flow, self).__init__(params=params)
        self.job_manager = job_manager

    def configure(self):
        if isclass(self.job_manager):
            self.job_manager = self.job_manager(self.head_job)

        return self.params

    def run(self):
        params = self.configure()
        self.job_manager.run(params)
