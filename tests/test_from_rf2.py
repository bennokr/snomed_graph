import os
import sys

# add project root to sys.path so tests can import the package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from snomed_graph.snomed_graph import SnomedGraph, SnomedConceptDetails


def test_from_rf2_release():
    # Path to the dummy RF2 release data included in tests
    data_dir = os.path.join(os.path.dirname(__file__), "data", "rf2",
                             "SnomedCT_DummyRF2_PRODUCTION_20230101T000000Z")
    assert os.path.isdir(data_dir), f"Test data directory {data_dir} does not exist"

    sg = SnomedGraph.from_rf2(data_dir)
    # The graph should have 2 concepts (nodes 1 and 2) and 1 relationship
    assert repr(sg) == "SNOMED graph has 2 vertices and 1 edges"

    # Check concept details FSNs
    cd1 = sg.get_concept_details(1)
    cd2 = sg.get_concept_details(2)
    assert isinstance(cd1, SnomedConceptDetails)
    assert cd1.fsn == "Foo (example)"
    assert cd2.fsn == "Bar (example)"

    # Check relationship type mapping
    assert sg.relationship_types == {"reltype example"}

    # Check the edge data directly
    edges = list(sg.G.edges(data=True))
    assert len(edges) == 1
    src, tgt, data = edges[0]
    assert (src, tgt) == (1, 2)
    assert data["type"] == "reltype example"
    assert data["type_id"] == 999