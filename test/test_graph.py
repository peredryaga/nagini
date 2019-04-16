# -*- coding: utf8 -*-
from __future__ import unicode_literals

import pytest

from nagini.graph import CircularDependency, ExecutionGraph
from nagini.job import BaseJob


def test_graph():
    class A(BaseJob):
        pass

    class B(BaseJob):
        new_requires = A

    class C(BaseJob):
        new_requires = B

    class D(BaseJob):
        new_requires = (C, B)

    graph = ExecutionGraph(D)
    graph.build({})
    graph.validate()

    assert graph.head.job_class == D
    assert map(lambda n: n.job_class, graph.iter_nodes(graph.head)) == [C, B, A, B, A]


def test_circular_dependency():
    class A(BaseJob):
        pass

    class B(BaseJob):
        @classmethod
        def get_requires(cls, params=None):
            return [A, D]

    class D(BaseJob):
        new_requires = (A, B)

    graph = ExecutionGraph(D)
    graph.build({})

    with pytest.raises(CircularDependency):
        graph.validate()


def test_skip_node():
    class A(BaseJob):
        pass

    class B(BaseJob):
        new_requires = A

    class C(BaseJob):
        new_requires = B

        @classmethod
        def get_requires(cls, params=None):
            if params.get('skip C req'):
                return []
            return super(C, cls).get_requires(params)

    class D(BaseJob):
        new_requires = (C, B)

    graph = ExecutionGraph(D)
    graph.build({'skip C req': True})
    graph.validate()

    assert map(lambda n: n.job_class, graph.iter_nodes(graph.head)) == [C, B, A]

    graph = ExecutionGraph(D)
    graph.build({})
    graph.validate()

    assert map(lambda n: n.job_class, graph.iter_nodes(graph.head)) == [C, B, A, B, A]
