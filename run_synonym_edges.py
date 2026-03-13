from vfb_connect.cross_server_tools import VfbConnect

vc = VfbConnect()

synonym_queries = [
    {"synonym_type": "has_exact_synonym", "scope": "has_exact_synonym"},
    {"synonym_type": "has_broad_synonym", "scope": "has_broad_synonym"},
    {"synonym_type": "has_narrow_synonym", "scope": "has_narrow_synonym"},
    {"synonym_type": "has_related_synonym", "scope": "has_related_synonym"}
]

statements = []
for query in synonym_queries:
    statements.append(
        "CALL apoc.periodic.iterate("
        "\"MATCH (primary) WHERE EXISTS(primary." + query['synonym_type'] + ") RETURN primary\", "
        "\"WITH primary, "
        "REDUCE(syns = [], syn IN primary." + query['synonym_type'] + " | syns + [apoc.convert.fromJsonMap(syn)]) AS syns "
        "UNWIND syns AS syn "
        "WITH primary, syn, TRIM(COALESCE(syn.annotations.database_cross_reference[0], '')) AS ref "
        "WITH primary, syn, ref, "
        "CASE WHEN ref <> '' AND ref CONTAINS ':' THEN SPLIT(ref, ':')[0] ELSE NULL END AS prefix, "
        "CASE WHEN ref <> '' AND ref CONTAINS ':' THEN TRIM(SPLIT(ref, ':')[1]) ELSE NULL END AS raw_id "
        "WITH primary, syn, ref, prefix, raw_id, "
        "CASE WHEN prefix = 'doi' AND raw_id IS NOT NULL "
        "THEN 'doi_' + REPLACE(REPLACE(raw_id, '.', '_'), '/', '_') "
        "WHEN raw_id IS NOT NULL THEN raw_id "
        "ELSE 'Unattributed' END AS pub_short_form, "
        "CASE WHEN ref = '' OR NOT ref CONTAINS ':' THEN [syn.value] ELSE [ref] END AS unresolved_ref, "
        "CASE WHEN ref = '' OR NOT ref CONTAINS ':' THEN true ELSE false END AS missing_ref "
        "OPTIONAL MATCH (p:pub {short_form: pub_short_form}) "
        "WITH primary, syn, ref, p, p IS NULL AS unresolved, unresolved_ref, missing_ref "
        "MATCH (fallback:pub {short_form: 'Unattributed'}) "
        "WITH primary, syn, ref, COALESCE(p, fallback) AS resolved_pub, (unresolved OR missing_ref) AS unresolved, unresolved_ref "
        "MERGE (primary)-[r:has_reference {typ: 'syn', value: [syn.value]}]->(resolved_pub) "
        "ON CREATE SET r += { "
        "iri: 'http://purl.org/dc/terms/references', "
        "scope: '" + query['scope'] + "', "
        "short_form: 'references', "
        "typ: 'syn', "
        "label: 'has_reference', "
        "type: 'Annotation'} "
        "SET r.has_synonym_type = syn.annotations.has_synonym_type "
        "WITH r, unresolved, unresolved_ref "
        "FOREACH (x IN CASE WHEN unresolved THEN [1] ELSE [] END | SET r.unresolved_ref = unresolved_ref)\", "
        "{batchSize: 500, iterateList: true})"
    )

vc.nc.commit_list(statements)
print('Done creating synonym edges')
