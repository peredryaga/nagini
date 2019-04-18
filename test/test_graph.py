# -*- coding: utf8 -*-
from __future__ import unicode_literals

import pytest

from nagini.v2.graph import CircularDependency, ExecutionGraph, Node, NodeAlreadyExists
from nagini.v2.job import Job


def test_graph():
    class A(Job):
        pass

    class B(Job):
        requires = A

    class C(Job):
        requires = B

    class D(Job):
        requires = (C, B)

    graph = ExecutionGraph(D)
    graph.build({})
    graph.validate()

    assert graph.heads[0].job_class == D
    assert map(lambda n: n.job_class, graph.iter_nodes(graph.heads[0])) == [C, B, A, B, A]


def test_circular_dependency():
    class A(Job):
        pass

    class B(Job):
        def requires(self):
            return (A, D)

    class D(Job):
        requires = (A, B)

    graph = ExecutionGraph(D)
    graph.build({})

    with pytest.raises(CircularDependency):
        graph.validate()


def test_skip_node():
    class A(Job):
        pass

    class B(Job):
        requires = A

    class C(Job):
        def requires(self):
            if self.global_params.get('skip C req'):
                return []
            return B

    class D(Job):
        requires = (C, B)

    graph = ExecutionGraph(D)
    graph.build({'skip C req': True})
    graph.validate()

    assert map(lambda n: n.job_class, graph.iter_nodes(graph.heads[0])) == [C, B, A]

    graph = ExecutionGraph(D)
    graph.build({})
    graph.validate()

    assert map(lambda n: n.job_class, graph.iter_nodes(graph.heads[0])) == [C, B, A, B, A]


def test_create_node():
    class A(Job):
        pass

    class B(Job):
        requires = A

    graph = ExecutionGraph(B)

    with pytest.raises(NodeAlreadyExists):
        graph.create_node(B)
    assert graph.jobs_dict[B].job_class == B

    assert A not in graph.jobs_dict
    assert graph.create_node(A) == graph.jobs_dict[A]
    assert graph.jobs_dict[A].job_class == A


def test_node():
    class A(Job):
        pass

    class B(Job):
        pass

    class C(Job):
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
