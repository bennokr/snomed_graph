import os
import sys

# add project root to sys.path so tests can import the package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import duckdb

from snomed_graph.import_snomedct import import_rf2_to_duckdb


def test_import_rf2_to_duckdb(tmp_path):
    data_dir = os.path.join(
        os.path.dirname(__file__), 'data', 'rf2',
        'SnomedCT_DummyRF2_PRODUCTION_20230101T000000Z'
    )
    db_file = tmp_path / 'test.duckdb'
    conn = import_rf2_to_duckdb(data_dir, core_file=None, db_path=str(db_file))
    # Concept table should have two entries (IDs 1 and 2)
    count = conn.execute('SELECT COUNT(*) FROM Concept').fetchone()[0]
    assert count == 2
    # Description table should have three entries (two concepts and one relationship type)
    count = conn.execute('SELECT COUNT(*) FROM Description').fetchone()[0]
    assert count == 3
    # TextDefinition table should have one entry
    count = conn.execute('SELECT COUNT(*) FROM TextDefinition').fetchone()[0]
    assert count == 1
    # Relationship table should have one entry
    count = conn.execute('SELECT COUNT(*) FROM Relationship').fetchone()[0]
    assert count == 1

    # Full-text search index on Description.term should find expected entries
    # Search for 'Foo' returns only the description with id 1
    res = conn.execute(
        "SELECT id FROM Description WHERE fts_main_Description.match_bm25(id, 'Foo') IS NOT NULL"
    ).fetchall()
    assert res == [(1,)]

    # Full-text search index on TextDefinition.term should find expected entry
    res = conn.execute(
        "SELECT id FROM TextDefinition WHERE fts_main_TextDefinition.match_bm25(id, 'Baz') IS NOT NULL"
    ).fetchall()
    assert res == [(4,)]
    conn.close()