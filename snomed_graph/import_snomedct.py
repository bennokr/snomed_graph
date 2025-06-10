"""Module for importing SNOMED CT RF2 release into a DuckDB database."""

import os
import csv
import duckdb


def import_rf2_to_duckdb(rf2_dir: str, core_file: str = None, db_path: str = None,
                         only_active_concept: bool = False, language: str = 'en') -> duckdb.DuckDBPyConnection:
    """Import a SNOMED CT RF2 release into a DuckDB database.

    Args:
        rf2_dir: Path to the RF2 release directory (must contain Snapshot/Terminology).
        core_file: Optional path to CORE problem list file to flag is_in_core concepts.
        db_path: Optional path to output DuckDB database file. Defaults to 'snomedct.duckdb' in cwd.
        only_active_concept: If True, only import active concepts.
        language: Language code for language-dependent tables (default 'en').

    Returns:
        A DuckDBPyConnection to the created database.
    """
    if db_path is None:
        db_path = os.path.join(os.getcwd(), 'snomedct.duckdb')
    conn = duckdb.connect(database=db_path, read_only=False)
    cur = conn.cursor()

    core_ids = set()
    if core_file:
        with open(core_file, encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='|')
            next(reader, None)
            for row in reader:
                if len(row) > 7 and row[7] == 'False':
                    core_ids.add(int(row[0]))

    base = os.path.basename(rf2_dir.rstrip(os.sep))
    version = base.split('_')[-1]
    if version.endswith('RF2Release'):
        version = version.replace('RF2Release', '')
    version = version[:8]

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Concept (
            id BIGINT PRIMARY KEY,
            effectiveTime VARCHAR,
            active INTEGER,
            moduleId BIGINT,
            definitionStatusId BIGINT,
            is_in_core INTEGER
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Description (
            id BIGINT PRIMARY KEY,
            effectiveTime VARCHAR,
            active INTEGER,
            moduleId BIGINT,
            conceptId BIGINT,
            languageCode VARCHAR,
            typeId BIGINT,
            term VARCHAR,
            caseSignificanceId BIGINT
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS TextDefinition (
            id BIGINT PRIMARY KEY,
            effectiveTime VARCHAR,
            active INTEGER,
            moduleId BIGINT,
            conceptId BIGINT,
            languageCode VARCHAR,
            typeId BIGINT,
            term VARCHAR,
            caseSignificanceId BIGINT
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Relationship (
            id BIGINT PRIMARY KEY,
            effectiveTime VARCHAR,
            active INTEGER,
            moduleId BIGINT,
            sourceId BIGINT,
            destinationId BIGINT,
            relationshipGroup INTEGER,
            typeId BIGINT,
            characteristicTypeId BIGINT
        );
    ''')

    terminology_dir = os.path.join(rf2_dir, 'Snapshot', 'Terminology')
    tables = [
        ('Concept', False),
        ('TextDefinition', True),
        ('Description', True),
        ('Relationship', False),
    ]
    for table, lang_dep in tables:
        fname = (f'sct2_{table}_Snapshot-{language}_INT_{version}.txt'
                 if lang_dep else f'sct2_{table}_Snapshot_INT_{version}.txt')
        path = os.path.join(terminology_dir, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")

        # load table using DuckDB's CSV reader
        rel = conn.from_csv_auto(path)
        if table == 'Concept':
            if only_active_concept:
                rel = rel.filter("active = 1")
            if core_ids:
                ids = ",".join(str(cid) for cid in core_ids)
                rel = rel.project(
                    "id, effectiveTime, active, moduleId, definitionStatusId, "
                    f"CASE WHEN id IN ({ids}) THEN 1 ELSE 0 END AS is_in_core"
                )
            else:
                rel = rel.project(
                    "id, effectiveTime, active, moduleId, definitionStatusId, 0 AS is_in_core"
                )
        elif table == 'Relationship':
            # drop modifierId column
            rel = rel.project(
                "id, effectiveTime, active, moduleId, sourceId, destinationId, "
                "relationshipGroup, typeId, characteristicTypeId"
            )
        # for Description and TextDefinition the columns match table schema
        rel.insert_into(table)
        conn.commit()

    # create full-text search indexes on description and definition terms
    cur.execute("PRAGMA create_fts_index('Description', 'id', 'term')")
    cur.execute("PRAGMA create_fts_index('TextDefinition', 'id', 'term')")
    conn.commit()

    return conn
