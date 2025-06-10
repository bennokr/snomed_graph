import os
import sys

# add project root to sys.path so tests can import the package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import networkx as nx
import pytest

from snomed_graph.snomed_graph import (
    SnomedConceptDetails,
    SnomedRelationship,
    SnomedRelationshipGroup,
    SnomedGraph,
)


def make_graph():
    G = nx.DiGraph()
    # add nodes with fsn and synonyms
    G.add_node(1, fsn="Root (body structure)", synonyms=["RootSyn"])
    G.add_node(2, fsn="Child (body structure)", synonyms=["ChildSyn"])
    G.add_node(3, fsn="Grandchild (body structure)", synonyms=["GrandChildSyn"])
    # add is-a relationships (group 0)
    is_a = SnomedGraph.is_a_relationship_typeId
    rel_type = "is a"
    G.add_edge(2, 1, group=0, type=rel_type, type_id=is_a)
    G.add_edge(3, 2, group=0, type=rel_type, type_id=is_a)
    # add a non-is-a relationship for testing inferred relationships
    G.add_edge(2, 3, group=1, type="foo", type_id=999)
    return G


@pytest.fixture
def small_sg():
    return SnomedGraph(make_graph())


def test_concept_details_hierarchy_and_repr_eq_hash():
    cd = SnomedConceptDetails(sctid=42, fsn="Heart (organ)", synonyms=["Cardiac"])
    assert cd.hierarchy == "organ"
    assert repr(cd) == "42 | Heart (organ)"
    # equality and hash based on sctid
    cd2 = SnomedConceptDetails(sctid=42, fsn="HeartDifferent (organ)", synonyms=[])
    assert cd == cd2
    assert hash(cd) == 42


def test_relationship_and_group_repr():
    src = SnomedConceptDetails(1, "A (a)", [])
    tgt = SnomedConceptDetails(2, "B (b)", [])
    rel = SnomedRelationship(src, tgt, group=0, type="foo", type_id=123)
    assert repr(rel) == "[1 | A (a)] ---[foo]---> [2 | B (b)]"
    group = SnomedRelationshipGroup(0, [rel])
    text = repr(group)
    assert "Group 0" in text
    assert repr(rel) in text


def test_snomed_graph_basic_operations(small_sg):
    sg = small_sg
    # repr, length, containment
    assert repr(sg) == "SNOMED graph has 3 vertices and 3 edges"
    assert len(sg) == 3
    assert 1 in sg
    assert 99 not in sg

    # get children and parents for is-a hierarchy
    children = sg.get_children(1)
    assert children == [SnomedConceptDetails(2, "Child (body structure)", ["ChildSyn"])]
    parents = sg.get_parents(2)
    assert parents == [SnomedConceptDetails(1, "Root (body structure)", ["RootSyn"])]

    # inferred relationships (non-is-a)
    inf = sg.get_inferred_relationships(2)
    assert len(inf) == 1
    grp = inf[0]
    assert grp.group == 1
    assert isinstance(grp.relationships[0], SnomedRelationship)

    # concept details and full concept
    cd = sg.get_concept_details(1)
    assert cd == SnomedConceptDetails(1, "Root (body structure)", ["RootSyn"])
    full = sg.get_concept_details(2)
    # synonyms and fsn
    assert full.fsn == "Child (body structure)"
    assert full.synonyms == ["ChildSyn"]

    # descendants and ancestors
    desc = sg.get_descendants(1)
    assert {c.sctid for c in desc} == {2, 3}
    anc = sg.get_ancestors(3)
    assert {c.sctid for c in anc} == {1, 2}

    # neighbourhood
    neigh = sg.get_neighbourhood(2)
    assert {c.sctid for c in neigh} == {1, 3}

    # find path (both directions)
    p = sg.find_path(2, 1)
    assert len(p) == 1 and p[0].src.sctid == 2 and p[0].tgt.sctid == 1
    p_rev = sg.find_path(1, 2)
    assert len(p_rev) == 1 and p_rev[0].src.sctid == 2 and p_rev[0].tgt.sctid == 1

    # relationship types
    assert sg.relationship_types == {"is a", "foo"}

    # pandas export
    nodes_df, edges_df = sg.to_pandas()
    assert set(nodes_df.index) == {1, 2, 3}
    for col in ["source", "target", "group", "type", "type_id"]:
        assert col in edges_df.columns


def test_save_and_from_serialized(tmp_path):
    sg = SnomedGraph(make_graph())
    path = tmp_path / "graph.gml"
    sg.save(str(path))
    loaded = SnomedGraph.from_serialized(str(path))
    assert len(loaded) == len(sg)


def test_iter_and_len(small_sg):
    sg = small_sg
    ids = {cd.sctid for cd in sg}
    assert ids == {1, 2, 3}