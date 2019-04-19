# -*- coding: utf8 -*-
from __future__ import unicode_literals

import logging
import random
import time

from nagini.utility import flatten
from nagini.v2.graph import ExecutionGraph


class JobManager(object):
    def __init__(self, heads, logger=None):
        """
        :type heads: nagini.v2.base.JobMetaClass | list | tuple | set| dict
        :type logger: logging.Logger
        """
        self.heads = flatten(heads)
        self.jobs = {}
        self.logger = logger or logging.getLogger(__name__)

    def build_graph(self, params):
        graph = ExecutionGraph(self.heads)
        graph.build(params)
        graph.validate()
        return graph

    def execute_job(self, job):
        job.execute()

    def prepare_job_input(self, node, params):
        for require_job in (r for r in node.children if r.job_class not in self.jobs):
            self.run_node(require_job, params)

        node.job.update_input(self.jobs)

    def run_node(self, node, params):
        if node.job_class not in self.jobs:
            self.jobs[node.job_class] = node.job

            if not node.job.can_skip():
                self.prepare_job_input(node, params)
                self.execute_job(node.job)

    def run(self, params):
        graph = self.build_graph(params)

        for head in graph.heads:
            self.run_node(head, params)


class JobManagerWithRetries(JobManager):
    def execute_job(self, job):
        i = 0
        while True:
            next_sleep = min(random.random() * (2 ** i), job.max_retry_delay)

            try:
                super(JobManagerWithRetries, self).execute_job(job)
            except Exception as e:
                i += 1
                if i <= job.retries:
                    time.sleep(next_sleep)
                else:
                    raise
