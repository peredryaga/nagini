# -*- coding: utf8 -*-
from __future__ import unicode_literals

import logging

from six import iteritems

from nagini.v2.base import ClassWithFields


class Job(ClassWithFields):
    new_requires = None
    retries = 0
    max_retry_delay = 0

    def __init__(self, logger=None, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        self.logger = logger or logging.getLogger('nagini.job.%s' % self.__class__.__name__)
        self.result = None
        self._input = {}

    def can_skip(self):
        return False

    @classmethod
    def get_requires(cls, global_params=None):
        return cls.new_requires

    @property
    def input(self):
        return self._input

    @input.setter
    def input(self, jobs):
        requires = self.get_requires(self.global_params)
        if isinstance(requires, (tuple, list, set)):
            self._input = [jobs[r].result for r in requires]
        elif isinstance(requires, Job):
            self._input = jobs[requires].result
        elif isinstance(requires, dict):
            self._input = {k: jobs[v].result for k, v in iteritems(requires)}
        else:
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
            self.result = self.run()
        except BaseException as e:
            self.on_failure()
            raise
        else:
            self.on_success()
