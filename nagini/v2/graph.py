# -*- coding: utf8 -*-
from __future__ import unicode_literals

from nagini.errors import NaginiError
from nagini.utility import flatten


class CircularDependency(NaginiError):
    pass


class NodeAlreadyExists(NaginiError):
    pass


class Node(object):
    def __init__(self, job_class):
        """
        :type job_class: nagini.v2.job.BaseJob
        """
        self.job_class = job_class
        self._job = None
        self.children = []
        self.parents = []

    def add(self, node):
        self.children.append(node)
        node.parents.append(self)

    def init_job(self, params):
        self._job = self.job_class(params=params)

    @property
    def job(self):
        """
        :rtype: nagini.v2.job.Job
        """
        return self._job

    def __str__(self):
        return self.job_class.__name__


class ExecutionGraph(object):
    def __init__(self, heads):
        self.jobs_dict = {}
        self.heads = [self.create_node(head) for head in flatten(heads)]

    def create_node(self, job):
        if job in self.jobs_dict:
            raise NodeAlreadyExists('Node for job %s already exists' % job)

        job_node = self.jobs_dict[job] = Node(job)
        return job_node

    def build_branches(self, node, branch=None):
        """Try to create branches of the tree"""
        if branch is None:
            branch = [node]
        elif node not in branch:
            branch = branch + [node]
        else:
            raise CircularDependency(branch)

        for child in node.children:
            self.build_branches(child, branch)

    def validate(self):
        """
        Check there is no circular dependency
        """
        for head in self.heads:
            self.build_branches(head)

    def build(self, params):
        """Building tree from required jobs of the head job"""
        check = self.heads[:]
        while check:
            node = check.pop(0)
            node.init_job(params)

            for required_job_class in flatten(node.job.requires):
                if required_job_class not in self.jobs_dict:
                    required_node = self.create_node(required_job_class)
                    check.append(required_node)
                else:
                    required_node = self.jobs_dict[required_job_class]
                node.add(required_node)

    def iter_nodes(self, parent, reverse=False):
        """
        Iterate over the children of the parent
        :type parent: Node
        :param bool reverse: If False - the first node is the head, else - one of the last child
        """
        if reverse:
            for node in parent.children:
                for child_node in self.iter_nodes(node):
                    yield child_node
                yield node
        else:
            for node in parent.children:
                yield node
                for child_node in self.iter_nodes(node):
                    yield child_node
