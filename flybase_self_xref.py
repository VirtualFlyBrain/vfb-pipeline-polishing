"""FlyBase self_xref marker: point it at the FlyBase Site and add it to FlyBase-report classes.

A node's self_xref names the Site whose link_base, plus the node's own short_form, is the node's own report
page (VFB_json_schema query_roller.xrefs: OPTIONAL MATCH (s:Site { short_form: self_xref[0] })). The KB loaders
write 'FlyBase', but the FlyBase Site's short_form is 'FlyBaseSite': 'FlyBase' is already the short_form of a pub
and of an annotation property, and pdb has unique_Individual_short_form. So the lookup found no Site and term
info lost its FlyBase link. Some FlyBase-report classes (FBco, FBtp, FBal) never got a marker at all.

The Site is looked up by its label and link_base, so this keeps working if its short_form changes again.
Plain Cypher, no APOC; safe to repeat.
"""
import re

FLYBASE_REPORTS = 'http://flybase.org/reports/'
LEGACY_MARKER = 'FlyBase'


def _rows(result):
    if result and isinstance(result, list) and 'data' in result[0]:
        return [d['row'] for d in result[0]['data']]
    return []


def find_flybase_sites(nc):
    """short_forms of every Site labelled FlyBase whose link_base is the FlyBase report URL."""
    rows = _rows(nc.commit_list(statements=[
        "MATCH (s:Site) WHERE ([] + s.label)[0] = 'FlyBase' AND '%s' IN ([] + s.link_base) "
        "RETURN s.short_form" % FLYBASE_REPORTS]))
    return sorted({r[0] for r in rows if r and r[0]})


def marker_statements(site):
    """Cypher that leaves every FlyBase self_xref pointing at `site` and marks FlyBase-report classes."""
    if not re.fullmatch(r'[A-Za-z0-9_]+', site):
        raise ValueError('unexpected Site short_form: %r' % site)
    stmts = []
    if site != LEGACY_MARKER:
        # [] + x makes a list of a string or a list; null is excluded first
        stmts.append("MATCH (n:Entity) WHERE n.self_xref IS NOT NULL AND '%s' IN ([] + n.self_xref) "
                     "AND NOT '%s' IN ([] + n.self_xref) SET n.self_xref = ['%s']" % (LEGACY_MARKER, site, site))
    # NOT n.self_xref = 'x' is null, not true, when the property is missing, so test IS NULL explicitly
    stmts.append("MATCH (c:Class) WHERE c.iri STARTS WITH '%sFB' AND c.self_xref IS NULL "
                 "SET c.self_xref = ['%s']" % (FLYBASE_REPORTS, site))
    return stmts


def count_marked(nc, site):
    rows = _rows(nc.commit_list(statements=[
        "MATCH (n:Entity) WHERE n.self_xref IS NOT NULL AND '%s' IN ([] + n.self_xref) RETURN count(n)" % site]))
    return rows[0][0] if rows else None


def normalise_flybase_self_xref(nc, log=print):
    """Returns True if the marker was applied. A missing or ambiguous Site is reported and skipped: a wrong
    marker only hides a link, so it must not fail the release."""
    sites = find_flybase_sites(nc)
    if len(sites) != 1:
        log('FlyBase self_xref: expected one FlyBase Site, found %s; markers left as they are' % sites)
        return False
    site = sites[0]
    log('FlyBase self_xref: marking with Site %s (%d nodes already do)' % (site, count_marked(nc, site) or 0))
    nc.commit_list(statements=marker_statements(site))
    log('FlyBase self_xref: now %s nodes' % count_marked(nc, site))
    return True
