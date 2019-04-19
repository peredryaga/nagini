# -*- coding: utf8 -*-
from __future__ import unicode_literals

import logging

from six import iteritems

from nagini.v2.base import ClassWithFields, JobMetaClass


class Job(ClassWithFields):
    __metaclass__ = JobMetaClass
    requires = None

    retries = 0
    max_retry_delay = 0

    def __init__(self, logger=None, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        self.logger = logger or logging.getLogger('nagini.job.%s' % self.__class__.__name__)
        self._result = None
        self._input = None

    def can_skip(self):
        return False

    @property
    def input(self):
        return self._input

    def update_input(self, all_jobs):
        requires = self.requires
        if isinstance(requires, (tuple, list, set)):
            self._input = [all_jobs[r].result for r in requires]
        elif isinstance(requires, JobMetaClass):
            self._input = all_jobs[requires].result
        elif isinstance(requires, dict):
            self._input = {k: all_jobs[v].result for k, v in iteritems(requires)}
        elif requires is not None:
            raise ValueError('requires() must return BaseJob, list[BaseJob] '
                             'or dict[str, BaseJob]')

    def configure(self):
        pass

    def run(self):
        raise NotImplementedError

    def on_failure(self):
        pass

    def on_success(self):
        pass

    def execute(self):
        self.configure()

        try:
            self._result = self.run()
        except Exception as e:
            self.on_failure()
            raise
        else:
            self.on_success()

    @property
    def result(self):
        # TODO return copy of result
        return self._result
