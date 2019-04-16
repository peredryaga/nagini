# -*- coding: utf8 -*-
from __future__ import unicode_literals

import logging
import random
import time

from nagini.v2.graph import ExecutionGraph


class JobManager(object):
    def __init__(self, head_job, logger=None):
        self.head_job = head_job
        self.jobs = {}
        self.logger = logger or logging.getLogger(__name__)

    def build_graph(self, params):
        graph = ExecutionGraph(self.head_job)
        graph.build(params)
        graph.validate()
        return graph

    def init_job(self, job, params):
        job = job(params=params)
        return job

    def execute_job(self, job):
        job.execute_new()

    def prepare_job_input(self, node, job, params):
        for require in (r for r in node.children if r.job_class not in self.jobs):
            self.run_node(require, params)

        job.update_input(self.jobs, params=params)

    def run_node(self, node, params):
        if node.job_class not in self.jobs:
            job = self.jobs[node.job_class] = self.init_job(node.job_class, params)

            if not job.can_skip():
                self.prepare_job_input(node, job, params)
                self.execute_job(job)

    def run(self, params):
        graph = self.build_graph(params)
        self.run_node(graph.head, params)


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
