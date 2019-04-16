# -*- coding: utf8 -*-
from __future__ import unicode_literals

import pytest

from nagini.v2.graph import CircularDependency, ExecutionGraph, Node, NodeAlreadyExists
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
        def get_requires(cls, global_params=None):
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
        def get_requires(cls, global_params=None):
            if global_params.get('skip C req'):
                return []
            return super(C, cls).get_requires(global_params)

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


def test_create_node():
    class A(BaseJob):
        pass

    class B(BaseJob):
        new_requires = A

    graph = ExecutionGraph(B)

    with pytest.raises(NodeAlreadyExists):
        graph.create_node(B)
    assert graph.jobs_dict[B].job_class == B

    assert A not in graph.jobs_dict
    assert graph.create_node(A) == graph.jobs_dict[A]
    assert graph.jobs_dict[A].job_class == A


def test_node():
    class A(BaseJob):
        pass

    class B(BaseJob):
        pass

    class C(BaseJob):
        pass

    node_a = Node(A)
    node_b = Node(B)

    node_b.add(node_a)
    assert node_b.children[0] == node_a
    assert node_a.parents[0] == node_b

    node_c = Node(C)
    node_a.add(node_c)

    assert node_b.children[0] == node_a
    assert node_a.parents[0] == node_b
    assert node_a.children[0] == node_c
    assert node_c.parents[0] == node_a
