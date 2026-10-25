"""Fast loader for the NBLAST / NeuronBridge similarity edges.

Replaces the row-by-row LOAD CSV blocks in finalStep.py. Those looked up every pair in the
graph (an undirected OPTIONAL MATCH over a node's similarity edges, ~400 on average and up to
~15,000), then MERGEd or SET, then set the node label again, one row at a time on one core.

This module gets the same end state in three phases:

1. Read: the Individual short_forms and the similarity edges already in the graph (the ones
   the OWL dumps brought in) are exported over HTTP.
2. Resolve: DuckDB reads every input file in the order finalStep.py loaded them, keeps only
   rows whose two neurons exist, and works out per unordered pair what the old sequence of
   statements would have left behind:
     * legacy files (query/target columns): the last row loaded sets the score;
     * new-format files (neuron_1/neuron_2): only rows >= NBLAST_N2N_MIN_SCORE, and the
       score only ever goes up (ties keep the earlier value);
     * NeuronBridge (top20_scores_agg_short_forms.tsv): the last row sets neuronbridge_score;
     * a new edge points the way the first row that created it did.
   The result is two lists: edges to CREATE and existing edges (by id) whose properties change.
3. Write: CREATE / SET in large UNWIND batches over HTTP, optionally from several workers, then
   one label pass from the set of neurons the rows touched (the labels the old per-row SETs
   gave).

Nothing is written to the graph until phase 2 has finished, so if anything before that fails
the caller can fall back to the old statements (PreWriteError).

Env:
  NBLAST_N2N_MIN_SCORE     new-format score threshold (default 0.25, as before)
  NBLAST_LOAD_BATCH        rows per write transaction (default 20000)
  NBLAST_LOAD_WORKERS      parallel write workers (default 1)
  NBLAST_DUCKDB_MEMORY     DuckDB memory limit (default: 50% of the container limit)
  NBLAST_DUCKDB_DIR        working directory for the DuckDB file + spill (default ./.nblast_duckdb)
  NBLAST_CHECKPOINT_IOPS   dbms.checkpoint.iops.limit while loading (default -1 = unlimited;
                           empty = leave it alone). Restored afterwards.
"""

import glob
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

# NBLAST_TEST_SUFFIX renames the relationship types and labels, so the loader can be timed and
# compared against the old statements on a copy of a released graph without touching its real
# similarity edges. Leave it unset in the pipeline.
_SFX = os.environ.get('NBLAST_TEST_SUFFIX', '')
SIM = 'has_similar_morphology_to' + _SFX
PART = 'has_similar_morphology_to_part_of' + _SFX
LABELS = {'sim': 'NBLAST' + _SFX, 'part': 'NBLASTexp' + _SFX, 'nb': 'neuronbridge' + _SFX}
BATCH_CACHE = re.compile(r'_q\d+_t\d+\.tsv$')
LEGACY, MAXK, NB = 0, 1, 2


class PreWriteError(Exception):
    """Failure before anything was written: safe to fall back to the old loader."""


def log(*a):
    print(*a, flush=True)


def input_plan(min_score):
    """(file, relationship, kind, (col_a, col_b)) in the order finalStep.py used to load them."""
    qt, n12, nb = ('query', 'target'), ('neuron_1', 'neuron_2'), ('n.short_form_x', 'n.short_form_y')
    plan = [(f, SIM, LEGACY, qt) for f in ['OL_FW_FC_ALL_ALL_SWC.tsv', 'HB_to_HB_OL_FW_FC_SWC.tsv']]
    plan += [(f, SIM, LEGACY, qt) for f in glob.glob('swc_swc_*.tsv')]          # same (unsorted) order as before
    plan += [('splits_swc.tsv', PART, LEGACY, qt)]
    plan += [(f, SIM, MAXK, n12) for f in sorted(glob.glob('nblast_n_2_n_*.tsv')) if not BATCH_CACHE.search(f)]
    plan += [(f, PART, MAXK, n12) for f in sorted(glob.glob('nblast_n_2_split_*.tsv')) if not BATCH_CACHE.search(f)]
    plan += [('top20_scores_agg_short_forms.tsv', PART, NB, nb)]
    return plan


class Neo:
    def __init__(self, endpoint, password):
        self.url = endpoint.rstrip('/') + '/db/data/transaction/commit'
        self.auth = ('neo4j', password)
        self.local = threading.local()

    def _session(self):
        if not hasattr(self.local, 's'):
            self.local.s = requests.Session()
            self.local.s.auth = self.auth
        return self.local.s

    def run(self, statement, params=None, retries=6, timeout=3600):
        body = {'statements': [{'statement': statement, 'parameters': params or {}}]}
        for attempt in range(retries):
            r = self._session().post(self.url, json=body, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            if not j['errors']:
                res = j['results'][0]
                return [dict(zip(res['columns'], d['row'])) for d in res['data']]
            code = j['errors'][0].get('code', '')
            if 'TransientError' in code and attempt < retries - 1:      # deadlock / lock timeout: retry
                time.sleep(1 + 2 * attempt)
                continue
            raise RuntimeError(f"{code}: {j['errors'][0].get('message', '')[:500]}")


def ensure_duckdb():
    try:
        import duckdb  # noqa: F401
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', 'duckdb==1.1.3'])
    import duckdb
    return duckdb


def container_memory_bytes():
    for p in ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        try:
            v = open(p).read().strip()
            if v.isdigit() and int(v) < 1 << 50:
                return int(v)
        except OSError:
            pass
    return 8 << 30


# ---------------------------------------------------------------- phase 1: read the graph

def export_graph(neo, workdir):
    """Individual short_forms and the existing similarity edges, written to TSVs for DuckDB."""
    t = time.time()
    rng = neo.run("MATCH (n:Individual) RETURN min(id(n)) AS lo, max(id(n)) AS hi, count(n) AS n")[0]
    step = max(1, (rng['hi'] - rng['lo']) // 40 + 1)
    nodes_tsv, edges_tsv = f'{workdir}/nodes.tsv', f'{workdir}/edges.tsv'
    n_nodes = n_edges = 0
    with open(nodes_tsv, 'w') as fn, open(edges_tsv, 'w') as fe:
        fn.write('sf\n')
        fe.write('rel\trid\ta\tb\tn_s\tn_f\tnb_s\n')
        for lo in range(rng['lo'], rng['hi'] + 1, step):
            p = {'lo': lo, 'hi': lo + step}
            for row in neo.run("MATCH (n:Individual) WHERE id(n) >= $lo AND id(n) < $hi "
                               "AND n.short_form IS NOT NULL RETURN n.short_form AS sf", p):
                fn.write(row['sf'] + '\n')
                n_nodes += 1
            for rel in (SIM, PART):
                for row in neo.run(
                        f"MATCH (a:Individual)-[r:{rel}]->(b:Individual) WHERE id(a) >= $lo AND id(a) < $hi "
                        "RETURN id(r) AS rid, a.short_form AS a, b.short_form AS b, "
                        "r.NBLAST_score[0] AS n, r.neuronbridge_score[0] AS nb", p):
                    n = row['n']
                    try:
                        nf = float(n) if n is not None else None
                    except (TypeError, ValueError):
                        nf = None                                   # Cypher toFloat() -> null
                    fe.write('\t'.join([rel, str(row['rid']), row['a'], row['b'],
                                        '' if n is None else str(n), '' if nf is None else repr(nf),
                                        '' if row['nb'] is None else str(row['nb'])]) + '\n')
                    n_edges += 1
    log(f"  exported {n_nodes} Individuals and {n_edges} existing similarity edges in {time.time() - t:.0f}s")
    if n_nodes != rng['n']:
        log(f"  note: {rng['n'] - n_nodes} Individuals have no short_form")
    return nodes_tsv, edges_tsv


# ---------------------------------------------------------------- phase 2: resolve in DuckDB

def resolve(duckdb, plan, nodes_tsv, edges_tsv, workdir, min_score):
    t = time.time()
    mem = os.environ.get('NBLAST_DUCKDB_MEMORY') or f'{container_memory_bytes() // 2 >> 20}MB'
    db = duckdb.connect(f'{workdir}/nblast.duckdb')
    db.execute(f"SET memory_limit='{mem}'")
    db.execute(f"SET temp_directory='{workdir}/spill'")
    db.execute("SET preserve_insertion_order=true")
    log(f"  DuckDB {duckdb.__version__}, memory limit {mem}")

    db.execute(f"CREATE TABLE nodes AS SELECT row_number() OVER ()::INTEGER AS nid, sf FROM "
               f"read_csv('{nodes_tsv}', delim='\t', header=true, quote='', columns={{'sf': 'VARCHAR'}})")
    db.execute(f"""CREATE TABLE ex AS SELECT e.rel, e.rid::BIGINT AS rid, na.nid AS a, nb.nid AS b,
                       nullif(e.n_s, '') AS n_s, TRY_CAST(nullif(e.n_f, '') AS DOUBLE) AS n_f,
                       nullif(e.nb_s, '') AS nb_s
                   FROM read_csv('{edges_tsv}', delim='\t', header=true, quote='', all_varchar=true) e
                   JOIN nodes na ON na.sf = e.a JOIN nodes nb ON nb.sf = e.b""")
    db.execute("CREATE TABLE rows (rel VARCHAR, kind TINYINT, seq BIGINT, a INTEGER, b INTEGER, "
               "s VARCHAR, f DOUBLE)")
    present = []
    for fo, (path, rel, kind, (ca, cb)) in enumerate(plan):
        if not os.path.exists(path):
            log(f"  WARNING: NBLAST input {path} is missing, not loaded")
            continue
        present.append(path)
        ft = time.time()
        db.execute("DROP TABLE IF EXISTS raw")
        db.execute(f"CREATE TABLE raw AS SELECT * FROM read_csv('{path}', delim='\t', header=true, "
                   f"quote='', all_varchar=true, null_padding=true)")
        cols = [c[0] for c in db.execute("DESCRIBE raw").fetchall()]
        if ca not in cols or cb not in cols or 'score' not in cols:
            raise PreWriteError(f"{path}: expected columns {ca}, {cb}, score; found {cols}")
        thr = f'AND TRY_CAST(r.score AS DOUBLE) >= {min_score}' if kind == MAXK else ''
        n_raw = db.execute("SELECT count(*) FROM raw").fetchone()[0]
        db.execute(f"""INSERT INTO rows SELECT '{rel}', {kind}, {fo}::BIGINT * 10000000000 + r.rowid,
                           na.nid, nb.nid, r.score, TRY_CAST(r.score AS DOUBLE)
                       FROM raw r JOIN nodes na ON na.sf = r."{ca}" JOIN nodes nb ON nb.sf = r."{cb}"
                       WHERE r."{ca}" IS NOT NULL AND r."{cb}" IS NOT NULL {thr}""")
        n_kept = db.execute(f"SELECT count(*) FROM rows WHERE seq >= {fo}::BIGINT * 10000000000 "
                            f"AND seq < {fo + 1}::BIGINT * 10000000000").fetchone()[0]
        log(f"  read {path}: {n_raw} rows, {n_kept} kept ({time.time() - ft:.0f}s)")
    db.execute("DROP TABLE IF EXISTS raw")

    # per unordered pair: what the old sequence of statements ended with
    db.execute(f"""CREATE TABLE pairs AS SELECT rel, least(a, b) AS lo, greatest(a, b) AS hi,
            arg_max(s, seq) FILTER (WHERE kind = {LEGACY}) AS l_s,
            arg_max(s, [f, -seq]) FILTER (WHERE kind = {MAXK}) AS m_s,      -- ties: earliest row
            max(f)          FILTER (WHERE kind = {MAXK})   AS m_f,
            arg_max(s, seq) FILTER (WHERE kind = {NB})     AS nb_new,
            arg_min(a, seq) AS first_a, arg_min(b, seq) AS first_b
        FROM rows GROUP BY ALL""")
    # existing edges: per edge id, apply legacy-overwrite, then max, then NeuronBridge
    db.execute("""CREATE TABLE updates AS
        WITH j AS (
            SELECT e.rel, e.rid, e.n_s, e.n_f, e.nb_s, p.l_s, p.m_s, p.m_f, p.nb_new,
                   CASE WHEN p.l_s IS NOT NULL THEN TRY_CAST(p.l_s AS DOUBLE) ELSE e.n_f END AS base_f,
                   (p.l_s IS NULL AND e.n_s IS NULL) AS base_null
            FROM ex e JOIN pairs p ON p.rel = e.rel AND p.lo = least(e.a, e.b) AND p.hi = greatest(e.a, e.b))
        SELECT rel, rid,
            CASE WHEN m_f IS NOT NULL AND (base_null OR base_f < m_f) THEN m_s ELSE l_s END AS n_new,
            nb_new
        FROM j""")
    db.execute("DELETE FROM updates WHERE n_new IS NULL AND nb_new IS NULL")
    db.execute("""CREATE TABLE creates AS
        SELECT p.rel, na.sf AS a, nb.sf AS b,
            CASE WHEN p.m_f IS NOT NULL AND (p.l_s IS NULL OR TRY_CAST(p.l_s AS DOUBLE) < p.m_f)
                 THEN p.m_s ELSE p.l_s END AS n_new,
            p.nb_new
        FROM pairs p
        JOIN nodes na ON na.nid = p.first_a JOIN nodes nb ON nb.nid = p.first_b
        WHERE NOT EXISTS (SELECT 1 FROM ex e WHERE e.rel = p.rel AND least(e.a, e.b) = p.lo
                          AND greatest(e.a, e.b) = p.hi)
        ORDER BY p.rel, na.sf""")
    db.execute(f"""CREATE TABLE labelled AS
        SELECT DISTINCT CASE WHEN rel = '{SIM}' THEN '{LABELS['sim']}' WHEN kind = {NB} THEN '{LABELS['nb']}'
                             ELSE '{LABELS['part']}' END AS label, nid
        FROM (SELECT rel, kind, a AS nid FROM rows UNION ALL SELECT rel, kind, b FROM rows)""")
    summary = db.execute("""SELECT (SELECT count(*) FROM rows), (SELECT count(*) FROM pairs),
                                   (SELECT count(*) FROM creates), (SELECT count(*) FROM updates),
                                   (SELECT count(*) FROM labelled), (SELECT count(*) FROM ex)""").fetchone()
    log(f"  resolved {summary[0]} rows into {summary[1]} pairs: {summary[2]} new edges, "
        f"{summary[3]} existing edges to update, {summary[4]} node labels; {time.time() - t:.0f}s")
    return db, present


# ---------------------------------------------------------------- phase 3: write

def write(neo, db, batch, workers):
    t = time.time()

    def props(rel, n_new, nb_new):
        p = {'iri': f'http://n2o.neo/custom/{rel}', 'short_form': rel, 'type': 'Annotation'}
        if n_new is not None:
            p['NBLAST_score'] = [n_new]
        if nb_new is not None:
            p['neuronbridge_score'] = [nb_new]
        return p

    def chunks(sql):
        cur = db.execute(sql)
        while True:
            rows = cur.fetchmany(batch)
            if not rows:
                return
            yield rows

    def run_parallel(statement, batches):
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for b in batches:
                futures.append(pool.submit(neo.run, statement, {'rows': b}))
                if len(futures) >= workers * 4:
                    done += sum(f.result()[0]['n'] for f in futures)
                    futures = []
            done += sum(f.result()[0]['n'] for f in futures)
        return done

    totals = {}
    for rel in (SIM, PART):
        st = (f"UNWIND $rows AS row MATCH (s:Individual {{short_form: row.a}}) "
              f"MATCH (b:Individual {{short_form: row.b}}) CREATE (s)-[r:{rel}]->(b) SET r = row.p "
              f"RETURN count(r) AS n")
        made = run_parallel(st, ([{'a': a, 'b': b, 'p': props(rel, n, nb)} for a, b, n, nb in rows]
                                 for rows in chunks(f"SELECT a, b, n_new, nb_new FROM creates WHERE rel = '{rel}'")))
        st = ("UNWIND $rows AS row MATCH ()-[r]->() WHERE id(r) = row.id "
              "FOREACH (x IN CASE WHEN row.n IS NULL THEN [] ELSE [1] END | SET r.NBLAST_score = [row.n]) "
              "FOREACH (x IN CASE WHEN row.nb IS NULL THEN [] ELSE [1] END | SET r.neuronbridge_score = [row.nb]) "
              "RETURN count(r) AS n")
        upd = run_parallel(st, ([{'id': rid, 'n': n, 'nb': nb} for rid, n, nb in rows]
                                for rows in chunks(f"SELECT rid, n_new, nb_new FROM updates WHERE rel = '{rel}'")))
        totals[rel] = (made, upd)
        log(f"  {rel}: created {made}, updated {upd} ({time.time() - t:.0f}s)")
    for label in LABELS.values():
        st = f"UNWIND $rows AS sf MATCH (n:Individual {{short_form: sf}}) SET n:{label} RETURN count(n) AS n"
        n = run_parallel(st, ([r[0] for r in rows] for rows in chunks(
            f"SELECT n.sf FROM labelled l JOIN nodes n USING (nid) WHERE l.label = '{label}' ORDER BY n.sf")))
        log(f"  :{label} set on {n} neurons")
    return totals


def verify(neo, db, before, totals):
    """Edge counts must equal what was there plus what was created."""
    ok = True
    for rel in (SIM, PART):
        now = neo.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS n")[0]['n']
        want = before[rel] + totals[rel][0]
        exp = db.execute(f"SELECT count(*) FROM creates WHERE rel = '{rel}'").fetchone()[0]
        log(f"  {rel}: {now} edges (was {before[rel]}, +{totals[rel][0]} created, {exp} planned)")
        ok &= (now == want and totals[rel][0] == exp)
    if not ok:
        raise RuntimeError("NBLAST load verification mismatch: edge counts do not match the plan")


def load_all(endpoint, password):
    min_score = float(os.environ.get('NBLAST_N2N_MIN_SCORE', '0.25'))
    batch = int(os.environ.get('NBLAST_LOAD_BATCH', '20000'))
    workers = int(os.environ.get('NBLAST_LOAD_WORKERS', '1'))
    workdir = os.path.abspath(os.environ.get('NBLAST_DUCKDB_DIR', '.nblast_duckdb'))
    iops = os.environ.get('NBLAST_CHECKPOINT_IOPS', '-1')
    neo = Neo(endpoint, password)
    t0 = time.time()
    log(f"Fast NBLAST load (min score {min_score}, batch {batch}, workers {workers})")

    # phases 1 and 2: nothing written to the graph yet
    try:
        duckdb = ensure_duckdb()
        plan = input_plan(min_score)
        need = sum(os.path.getsize(p) for p, *_ in plan if os.path.exists(p))
        shutil.rmtree(workdir, ignore_errors=True)
        os.makedirs(f'{workdir}/spill')
        free = shutil.disk_usage(workdir).free
        if free < 1.5 * need:
            raise PreWriteError(f"only {free >> 30} GiB free in {workdir}, need ~{int(1.5 * need) >> 30} GiB")
        before = {rel: neo.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS n")[0]['n']
                  for rel in (SIM, PART)}
        nodes_tsv, edges_tsv = export_graph(neo, workdir)
        db, present = resolve(duckdb, plan, nodes_tsv, edges_tsv, workdir, min_score)
    except PreWriteError:
        raise
    except Exception as e:
        raise PreWriteError(f"{type(e).__name__}: {e}") from e

    # phase 3: writes
    old_iops = None
    if iops != '':
        try:
            old_iops = neo.run("CALL dbms.listConfig('dbms.checkpoint.iops.limit') YIELD value RETURN value")[0]['value']
            neo.run("CALL dbms.setConfigValue('dbms.checkpoint.iops.limit', $v)", {'v': iops})
            log(f"  dbms.checkpoint.iops.limit {old_iops} -> {iops} for the load")
        except Exception as e:
            old_iops = None
            log(f"  WARNING: could not change dbms.checkpoint.iops.limit ({str(e)[:200]})")
    try:
        totals = write(neo, db, batch, workers)
        verify(neo, db, before, totals)
    finally:
        if old_iops is not None:
            neo.run("CALL dbms.setConfigValue('dbms.checkpoint.iops.limit', $v)", {'v': old_iops})
            log(f"  dbms.checkpoint.iops.limit restored to {old_iops}")
        db.close()
        shutil.rmtree(workdir, ignore_errors=True)
    log(f"Fast NBLAST load done in {time.time() - t0:.0f}s from {len(present)} files")
