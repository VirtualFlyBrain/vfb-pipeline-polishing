"""Checks nblast_load.resolve() against a row-by-row simulation of the old finalStep.py statements.

python3 test_nblast_load.py   (needs duckdb; no Neo4j)
"""
import os, random, shutil, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckdb
import nblast_load as nl


def old_semantics(plan_rows, existing, nodes, min_score):
    """Sequential model of the old statements (each row sees all earlier rows' writes)."""
    edges = {k: dict(v) for k, v in existing.items()}             # rid -> {rel,a,b,n,nb}
    nxt = max(edges, default=0) + 1
    labels = set()
    tofloat = lambda x: (float(x) if x is not None and _num(x) else None)
    for rel, kind, a, b, s in plan_rows:
        if a not in nodes or b not in nodes:
            continue
        if kind == nl.MAXK and not (_num(s) and float(s) >= min_score):
            continue
        rs = [r for r, e in edges.items() if e['rel'] == rel and {e['a'], e['b']} == {a, b}]
        if not rs:
            e = {'rel': rel, 'a': a, 'b': b, 'n': None, 'nb': None}
            e['nb' if kind == nl.NB else 'n'] = s
            edges[nxt] = e
            nxt += 1
        else:
            for r in rs:
                e = edges[r]
                if kind == nl.LEGACY:
                    e['n'] = s
                elif kind == nl.MAXK:
                    if e['n'] is None or (tofloat(e['n']) is not None and tofloat(e['n']) < float(s)):
                        e['n'] = s
                else:
                    e['nb'] = s
        lab = nl.LABELS['sim'] if rel == nl.SIM else (nl.LABELS['nb'] if kind == nl.NB else nl.LABELS['part'])
        labels |= {(lab, a), (lab, b)}
    return edges, labels


def _num(x):
    try:
        float(x)
        return True
    except (TypeError, ValueError):
        return False


def run_case(seed):
    rnd = random.Random(seed)
    work = tempfile.mkdtemp()
    cwd = os.getcwd()
    os.chdir(work)
    try:
        nodes = [f'VFB_{i:08d}' for i in range(40)]
        known = set(nodes[:36])                                     # 4 ids in files are not in the graph
        score = lambda: rnd.choice([f'{rnd.random():.2f}', f'{rnd.random():.3f}', '0.25', '0.5', 'x'])
        pick = lambda: rnd.choice(nodes)
        files = {
            'OL_FW_FC_ALL_ALL_SWC.tsv': ('query', 'target'), 'swc_swc_A.tsv': ('query', 'target'),
            'swc_swc_B.tsv': ('query', 'target'), 'splits_swc.tsv': ('query', 'target'),
            'nblast_n_2_n_X_c.tsv': ('neuron_1', 'neuron_2'), 'nblast_n_2_n_Y_c.tsv': ('neuron_1', 'neuron_2'),
            'nblast_n_2_n_Y_c_q1_t2.tsv': ('neuron_1', 'neuron_2'),         # batch cache: ignored
            'nblast_n_2_split_Z.tsv': ('neuron_1', 'neuron_2'),
            'top20_scores_agg_short_forms.tsv': ('n.short_form_x', 'n.short_form_y'),
        }
        content = {}
        for f, (ca, cb) in files.items():
            rows = [(pick(), pick(), score()) for _ in range(rnd.randint(0, 120))]
            content[f] = rows
            with open(f, 'w') as h:
                h.write(f'{ca}\t{cb}\tscore\n' + ''.join(f'{a}\t{b}\t{s}\n' for a, b, s in rows))
        existing, rid = {}, 100
        for _ in range(30):
            a, b = pick(), pick()
            if a in known and b in known:
                rel = rnd.choice([nl.SIM, nl.PART])
                n = rnd.choice([None, str(round(rnd.random(), 2)), repr(rnd.random())])
                existing[rid] = {'rel': rel, 'a': a, 'b': b, 'n': n,
                                 'nb': rnd.choice([None, '0.7']) if rel == nl.PART else None}
                rid += 1
        os.makedirs('w')
        with open('w/nodes.tsv', 'w') as h:
            h.write('sf\n' + ''.join(n + '\n' for n in sorted(known)))
        with open('w/edges.tsv', 'w') as h:
            h.write('rel\trid\ta\tb\tn_s\tn_f\tnb_s\n')
            for r, e in existing.items():
                nf = repr(float(e['n'])) if e['n'] is not None and _num(e['n']) else ''
                h.write('\t'.join([e['rel'], str(r), e['a'], e['b'], e['n'] or '', nf, e['nb'] or '']) + '\n')
        plan = nl.input_plan(0.25)
        db, _ = nl.resolve(duckdb, plan, 'w/nodes.tsv', 'w/edges.tsv', os.path.abspath('w'), 0.25)

        # apply the plan to the existing edges
        got = {k: dict(v) for k, v in existing.items()}
        for rid_, n, nb in db.execute("SELECT rid, n_new, nb_new FROM updates").fetchall():
            if n is not None: got[rid_]['n'] = n
            if nb is not None: got[rid_]['nb'] = nb
        for rel, a, b, n, nb in db.execute("SELECT rel, a, b, n_new, nb_new FROM creates").fetchall():
            got[len(got) + 10000] = {'rel': rel, 'a': a, 'b': b, 'n': n, 'nb': nb}
        got_labels = set(db.execute("SELECT l.label, n.sf FROM labelled l JOIN nodes n USING (nid)").fetchall())

        seq = []
        for path, rel, kind, _ in plan:
            seq += [(rel, kind, a, b, s) for a, b, s in content.get(path, [])]
        want, want_labels = old_semantics(seq, existing, known, 0.25)
        canon = lambda es: sorted((e['rel'], e['a'], e['b'], str(e['n']), str(e['nb'])) for e in es.values())
        assert canon(got) == canon(want), (seed, set(canon(got)) ^ set(canon(want)))
        assert got_labels == want_labels, (seed, got_labels ^ want_labels)
        db.close()
        return len(want) - len(existing)
    finally:
        os.chdir(cwd)
        shutil.rmtree(work)


if __name__ == '__main__':
    made = [run_case(s) for s in range(int(sys.argv[1]) if len(sys.argv) > 1 else 200)]
    print(f'OK: {len(made)} random cases, {sum(made)} edges created in total')
