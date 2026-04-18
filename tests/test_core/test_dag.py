from __future__ import annotations

from yaruk.core.graph import DAG, Node


def _noop(*a: object) -> None:
    pass


def test_topological_order() -> None:
    dag = DAG()
    dag.add(Node(name="ocr", fn=_noop))
    dag.add(Node(name="layout", fn=_noop, depends_on=["ocr"]))
    dag.add(Node(name="table", fn=_noop, depends_on=["layout"]))
    dag.add(Node(name="text", fn=_noop, depends_on=["layout"]))
    dag.add(Node(name="merge", fn=_noop, depends_on=["table", "text"]))
    order = dag.topological_order()
    assert order.index("ocr") < order.index("layout")
    assert order.index("layout") < order.index("table")
    assert order.index("merge") == len(order) - 1


def test_parallel_groups() -> None:
    dag = DAG()
    dag.add(Node(name="ocr", fn=_noop))
    dag.add(Node(name="layout", fn=_noop, depends_on=["ocr"]))
    dag.add(Node(name="table", fn=_noop, depends_on=["layout"]))
    dag.add(Node(name="text", fn=_noop, depends_on=["layout"]))
    dag.add(Node(name="merge", fn=_noop, depends_on=["table", "text"]))
    groups = dag.parallel_groups()
    assert groups[0] == ["ocr"]
    assert groups[1] == ["layout"]
    assert set(groups[2]) == {"table", "text"}
    assert groups[3] == ["merge"]
