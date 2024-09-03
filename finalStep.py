import time
import timeit
import os
from vfb_connect.cross_server_tools import VfbConnect

# Set up the VfbConnect instance
vc = VfbConnect(neo_endpoint=str(os.environ.get('PDBserver')), neo_credentials=('neo4j', str(os.environ.get('PDBpass'))))

# Fix RO id edge types
start = timeit.default_timer()
print("Fix RO id edge types...")
vc.nc.commit_list(statements=[
    "MATCH (n:ObjectProperty) WHERE n.label STARTS WITH 'RO_' SET n.label = n.label_rdfs[0]",
    "CALL apoc.periodic.iterate('MATCH (a)<-[r1:RO_0002292]-(b) RETURN a, b, r1', 'MERGE (a)<-[r2:expresses]-(b) SET r2 += r1 SET r2.label=\"expresses\" SET r2.type=\"Related\" DELETE r1', {batchSize: 1000, parallel: true})",
    "CALL apoc.periodic.iterate('MATCH (a)<-[r1:RO_0002120]-(b) RETURN a, b, r1', 'MERGE (a)<-[r2:synapsed_to]-(b) SET r2 += r1 SET r2.label=\"synapsed to\" SET r2.type=\"Related\" DELETE r1', {batchSize: 1000, parallel: true})",
    "CALL apoc.periodic.iterate('MATCH (a)<-[r1:RO_0002175]-(b) RETURN a, b, r1', 'MERGE (a)<-[r2:present_in_taxon]-(b) SET r2 += r1 SET r2.label=\"present in taxon\" SET r2.type=\"Related\" DELETE r1', {batchSize: 1000, parallel: true})",
    "CALL apoc.periodic.iterate('MATCH (a)<-[r1:RO_0002579]-(b) RETURN a, b, r1', 'MERGE (a)<-[r2:is_indirect_form_of]-(b) SET r2 += r1 SET r2.label=\"is indirect form of\" SET r2.type=\"Related\" DELETE r1', {batchSize: 1000, parallel: true})",
    "MATCH (n) WHERE exists(n.nodeLabel) and n.nodeLabel = ['pub'] SET n:pub"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Clean BLOCKED images removing anatomical ind and channel
start = timeit.default_timer()
print("Clean BLOCKED images removing anatomical ind and channel...")
vc.nc.commit_list(statements=[
    "MATCH (i:Individual)<-[:depicts]-(c:Individual)-[:INSTANCEOF]->(cc:Class {short_form:'VFBext_0000014'}) WHERE NOT (c)-[:in_register_with]->(:Template) DETACH DELETE c DETACH DELETE i"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Add has_neuron/region_connectivity labels
start = timeit.default_timer()
print("Add has_neuron/region_connectivity labels...")
vc.nc.commit_list(statements=[
    "CALL apoc.periodic.iterate('MATCH (a:Neuron)-[r:synapsed_to]->(b:Neuron) WHERE EXISTS(r.weight) RETURN a, b', 'SET a:has_neuron_connectivity SET b:has_neuron_connectivity', {batchSize: 1000, parallel: true})",
    "CALL apoc.periodic.iterate('MATCH (n:Neuron)-[r:has_presynaptic_terminals_in]->(c:Synaptic_neuropil) RETURN n', 'SET n:has_region_connectivity', {batchSize: 1000, parallel: true})",
    "CALL apoc.periodic.iterate('MATCH (n:Neuron)-[r:has_postsynaptic_terminal_in]->(c:Synaptic_neuropil) RETURN n', 'SET n:has_region_connectivity', {batchSize: 1000, parallel: true})"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Clean NBLAST
start = timeit.default_timer()
print("Clean NBLAST...")
vc.nc.commit_list(statements=[
    "MATCH (a:NBLAST) REMOVE a:NBLAST",
    "MATCH (a:Individual)-[r1:has_similar_morphology_to]->(a) WHERE r1.NBLAST_score[0] = 1 DELETE r1",
    "MATCH (a:Individual)-[r1:has_similar_morphology_to]->(b:Individual) MATCH (b)-[r2:has_similar_morphology_to]->(a) WHERE NOT r1.NBLAST_score[0] = r2.NBLAST_score[0] MATCH (b)-[r:has_similar_morphology_to]-(a) WITH r1, r2, AVG(r.NBLAST_score[0]) AS mean SET r1.NBLAST_score=[mean] DELETE r2",
    "MATCH (a:Individual)-[r1:has_similar_morphology_to]->(b:Individual) MATCH (b)-[r2:has_similar_morphology_to]->(a) WHERE r1.NBLAST_score[0] = r2.NBLAST_score[0] DELETE r2",
    "MATCH (a:Individual)-[nblast:has_similar_morphology_to]-(b:Individual) SET a:NBLAST SET b:NBLAST"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Add any missing Project Labels
start = timeit.default_timer()
print("Add any missing Project Labels...")
vc.nc.commit_list(statements=[
    "MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:'catmaid_fafb'}) SET i:FAFB",
    "MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:'catmaid_l1em'}) SET i:L1EM",
    "MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:'catmaid_fanc'}) SET i:FANC",
    "MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:'neuprint_JRC_Hemibrain_1point1'}) SET i:FlyEM_HB",
    "MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:'FlyCircuit'}) SET i:FlyCircuit"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Expand any missing synonyms
start = timeit.default_timer()
print("Expand any missing synonyms...")
synonym_queries = [
    {"synonym_type": "has_exact_synonym", "scope": "has_exact_synonym"},
    {"synonym_type": "has_broad_synonym", "scope": "has_broad_synonym"},
    {"synonym_type": "has_narrow_synonym", "scope": "has_narrow_synonym"},
    {"synonym_type": "has_related_synonym", "scope": "has_related_synonym"}
]
statements = []
for query in synonym_queries:
    statements.append(
        f"CALL apoc.periodic.iterate('MATCH (primary) WHERE EXISTS(primary.{query['synonym_type']}) RETURN primary', 'WITH primary, REDUCE(syns = [], syn IN primary.{query['synonym_type']} | syns + [apoc.convert.fromJsonMap(syn)]) AS syns UNWIND syns AS syn MATCH (p:pub {{short_form: COALESCE(SPLIT(syn.annotations.database_cross_reference[0], ':')[1], 'Unattributed')}}) MERGE (primary)-[r:has_reference {{typ:\"syn\", value:[syn.value]}}]->(p) ON CREATE SET r += {{ iri: \"http://purl.org/dc/terms/references\", scope: \"{query['scope']}\", short_form: \"references\", typ: \"syn\", label: \"has_reference\", type: \"Annotation\" }}', {{batchSize: 1000, iterateList: true}})"
    )
vc.nc.commit_list(statements)
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Ensure all deprecated are labelled as such
start = timeit.default_timer()
print("Ensure all deprecated are labelled as such...")
vc.nc.commit_list(statements=[
    "MATCH (n:Individual) WHERE EXISTS(n.deprecated) AND n.deprecated = [true] AND NOT n:Deprecated SET n:Deprecated"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Ensure all xrefs are on separate edges
start = timeit.default_timer()
print("Ensure all xrefs are on separate edges...")
vc.nc.commit_list(statements=[
    "CALL apoc.periodic.iterate('MATCH (n)-[r:database_cross_reference]->(s:Site) WHERE SIZE(r.accession) > 1 RETURN n, s, r', 'SET r.accession = [r.accession[0]] CREATE (n)-[r1:database_cross_reference]->(s) SET r1 = r SET r1.accession = TAIL(r.accession)', {batchSize: 1000, parallel: true})"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Adding ALL SWC <-> SWC NBLAST scores
start = timeit.default_timer()
print("Adding ALL SWC <-> SWC NBLAST scores...")
vc.nc.commit_list(statements=[
    "USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///swc_swc.tsv' AS row FIELDTERMINATOR '\\t' MATCH (s:Individual {short_form: row.query}), (b:Individual {short_form: row.target}) MERGE (s)-[r:has_similar_morphology_to { iri: 'http://n2o.neo/custom/has_similar_morphology_to', short_form: 'has_similar_morphology_to', type: 'Annotation' }]->(b) SET r.NBLAST_score = [toFloat(row.score)], r.mirrored = CASE WHEN row.mirrored = 'y' THEN true ELSE false END SET s:NBLAST, b:NBLAST"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Loading SPLITS <-> SWC NBLAST scores from CSV
start = timeit.default_timer()
print("Loading SPLITS <-> SWC NBLAST scores from CSV...")
vc.nc.commit_list(statements=[
    "USING PERIODIC COMMIT 1000 LOAD CSV WITH HEADERS FROM 'file:///splits_swc.tsv' AS row FIELDTERMINATOR '\\t' MATCH (s:Individual {short_form: row.query}), (b:Individual {short_form: row.target}) MERGE (s)-[r:has_similar_morphology_to_part_of { iri: 'http://n2o.neo/custom/has_similar_morphology_to_part_of', short_form: 'has_similar_morphology_to_part_of', type: 'Annotation' }]->(b) SET r.NBLAST_score = [toFloat(row.score)] SET s:NBLASTexp, b:NBLASTexp"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Add Neuronbridge Hemibrain <-> slide code top 20 scores
start = timeit.default_timer()
print("Add Neuronbridge Hemibrain <-> slide code top 20 scores...")
vc.nc.commit_list(statements=[
    "CALL apoc.periodic.iterate('LOAD CSV WITH HEADERS FROM \"file:///top20_scores_agg.tsv\" AS row FIELDTERMINATOR \"\\t\" RETURN row', 'MATCH (body:Site {short_form: \"neuronbridge\"})<-[r1:database_cross_reference {accession: row.neuprint_xref}]-(b:Individual:Adult)-[:has_source]->(:DataSet {short_form: \"Xu2020NeuronsV1point1\"}) WHERE (b)<-[:depicts]-(:Individual)-[:in_register_with]->(:Template {short_form: \"VFBc_00101567\"}) MATCH (api:API {short_form: \"jrc_slide_code_api\"})<-[r2:database_cross_reference {accession: row.slidecode_API}]-(s:Individual:Adult) WHERE (s)<-[:depicts]-(:Individual)-[:in_register_with]->(:Template {short_form: \"VFBc_00101567\"}) MERGE (s)-[r:has_similar_morphology_to_part_of]->(b) ON CREATE SET r.iri = \"http://n2o.neo/custom/has_similar_morphology_to_part_of\", r.short_form = \"has_similar_morphology_to_part_of\", r.type = \"Annotation\", r.neuronbridge_score = [row.score] SET s:neuronbridge, b:neuronbridge', {batchSize: 1000, parallel: true})"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Add any missing unique facets
start = timeit.default_timer()
print("Add any missing unique facets...")
vc.nc.commit_list(statements=[
    "MATCH (n:Deprecated) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['Deprecated']",
    "MATCH (n:Deprecated) WHERE EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' AND NOT 'Deprecated' IN n.uniqueFacets SET n.uniqueFacets=n.uniqueFacets + ['Deprecated']",
    "MATCH (n:DataSet) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['DataSet']",
    "MATCH (n:pub) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['pub']",
    "MATCH (n:Person) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['Person']",
    "MATCH (n:Site) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['Site']",
    "MATCH (n:API) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['API']",
    "MATCH (n:License) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['License']",
    "MATCH (n:Expression_pattern:Split) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['Expression_pattern','Split']",
    "MATCH (n:Expression_pattern) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' SET n.uniqueFacets=['Expression_pattern']",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form STARTS WITH 'FBtp' SET n.uniqueFacets=['Transgenic_Construct']",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form STARTS WITH 'FBti' SET n.uniqueFacets=['Insertion']",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form STARTS WITH 'FBal' SET n.uniqueFacets=['Allele']",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form STARTS WITH 'FBgn' SET n.uniqueFacets=['Gene']",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form STARTS WITH 'FBrf' SET n.uniqueFacets=['FB_Reference']",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form STARTS WITH 'FBim' SET n.uniqueFacets=['FB_Image']",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form STARTS WITH 'FBdv' SET n.uniqueFacets=['Stage']",
    "MATCH (n:Class) WHERE n.short_form STARTS WITH 'FBdv' SET n:Stage",
    "MATCH (n:Class) WHERE n.short_form STARTS WITH 'FBgn' SET n:Gene",
    "MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) SET n.uniqueFacets=['Class']",
    "MATCH (n:Split) WHERE EXISTS(n.uniqueFacets) AND NOT n.short_form STARTS WITH 'VFBc_' AND NOT 'Split' IN n.uniqueFacets SET n.uniqueFacets=n.uniqueFacets + ['Split']"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Fixes for scRNAseq DataSets
start = timeit.default_timer()
print("Fixes for scRNAseq DataSets...")
vc.nc.commit_list(statements=[
    "MATCH (n:DataSet) WHERE n.short_form STARTS WITH 'FBlc' SET n:hasScRNAseq SET n:scRNAseq_DataSet",
    "MATCH (n:DataSet)<-[:has_source]-(:Individual)<-[:depicts]-(:Individual)-[:in_register_with]->(:Template) SET n:has_image",
    "MATCH (a)-[r1:licence]->(l:License) MERGE (a)-[r2:has_license]->(l) ON CREATE SET r2=r1 SET r2.label='has_license' DELETE r1",
    "MATCH (n:Cluster) WHERE EXISTS(n.uniqueFacets) AND NOT 'Cluster' IN n.uniqueFacets SET n.uniqueFacets= n.uniqueFacets + 'Cluster'",
    "MATCH (primary:Individual:Cluster)-[e:expresses]->(g:Gene:Class) SET g:hasScRNAseq",
    "MATCH (parent:Cell)<-[:SUBCLASSOF*]-(primary:Class)<-[:composed_primarily_of]-(c:Cluster)-[:has_source]->(ds:scRNAseq_DataSet) SET primary:hasScRNAseq SET parent:hasScRNAseq",
    "MATCH ()-[r:expresses]->() WHERE EXISTS(r.expression_level) WITH r, SPLIT(TOSTRING(r.expression_level[0]), '.') AS parts WITH SIZE(parts[0]) AS beforeDecimalLength, SIZE(parts[1]) AS afterDecimalLength, r WITH MAX(beforeDecimalLength) AS maxBeforeDecimal, MAX(afterDecimalLength) AS maxAfterDecimal, COLLECT(r) AS relationships UNWIND relationships AS r WITH maxBeforeDecimal, maxAfterDecimal, r, SPLIT(TOSTRING(r.expression_level[0]), '.') AS parts WITH maxBeforeDecimal, maxAfterDecimal, r, parts, APOC.TEXT.LPAD(parts[0], maxBeforeDecimal, '0') AS beforeDecimalPadded, APOC.TEXT.RPAD(parts[1], maxAfterDecimal, '0') AS afterDecimalPadded SET r.expression_level_padded = [beforeDecimalPadded + '.' + afterDecimalPadded]"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Remove any unique facet duplicates
start = timeit.default_timer()
print("Remove any unique facet duplicates...")
vc.nc.commit_list(statements=[
    "MATCH (n) WHERE EXISTS(n.uniqueFacets) SET n.uniqueFacets = apoc.coll.toSet(n.uniqueFacets)"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Expand term_replace_by parameter into edge links
start = timeit.default_timer()
print("Expand term_replace_by parameter into edge links...")
vc.nc.commit_list(statements=[
    "MATCH (n:Deprecated) WHERE EXISTS(n.term_replaced_by) AND NOT (n)-[:term_replaced_by]->() WITH n, REPLACE(n.term_replaced_by[0], ':', '_') AS id MATCH (r {short_form: id}) MERGE (n)-[t:term_replaced_by]->(r) ON CREATE SET t.iri = 'http://purl.obolibrary.org/obo/IAO_0100001', t.short_form = 'IAO_0100001', t.type = 'Annotation', t.label = 'term replaced by'"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Fix targeted schema issues
start = timeit.default_timer()
print("Fix targeted schema issues...")
vc.nc.commit_list(statements=[
    "MATCH ()-[r]->() WHERE EXISTS(r.pub) SET r.pub = r.pub + []",
    "MATCH ()-[r]->() WHERE EXISTS(r.typ) SET r.typ = (r.typ + [])[0]",
    "MATCH (n:pub) WHERE n.short_form STARTS WITH 'FBrf' AND NOT EXISTS(n.FlyBase) SET n.FlyBase = [n.short_form]",
    "MATCH (n:pub) WHERE EXISTS(n.FlyBase) SET n.FlyBase = [] + n.FlyBase"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Fix xref labels being used instead of label_rdfs
start = timeit.default_timer()
print("Fix xref labels being used instead of label_rdfs...")
vc.nc.commit_list(statements=[
    "MATCH (n) WHERE n.short_form = n.label AND EXISTS(n.label_rdfs) AND NOT n.label_rdfs[0] = n.label SET n.label=n.label_rdfs[0]",
    "MATCH (c:Class) WHERE n.label STARTS WITH 'wiki' AND EXISTS(c.label_rdfs) SET c.label = c.label_rdfs[0]",
    "MATCH (c:Class) WHERE c.short_form STARTS WITH 'GO_' AND NOT c.label = c.label_rdfs[0] SET c.label = c.label_rdfs[0]"
])
stop = timeit.default_timer()
print('Run time: ', stop - start)

# Monitoring function for running 'USING PERIODIC COMMIT' queries
def is_periodic_commit_running():
    query = (
        "CALL dbms.listQueries() "
        "YIELD query, status "
        "WHERE query CONTAINS 'USING PERIODIC COMMIT' AND status = 'running' AND NOT query CONTAINS 'dbms.listQueries' "
        "RETURN COUNT(*) AS running"
    )
    vc = VfbConnect(neo_endpoint=str(os.environ.get('PDBserver')), neo_credentials=('neo4j', str(os.environ.get('PDBpass'))))
    result = vc.nc.commit_list(statements=[query])
    return result[0]['data'][0]['row'][0] > 0

def monitor_queries(check_interval=1800):
    print("Monitoring for running 'USING PERIODIC COMMIT' queries...")
    while True:
        if is_periodic_commit_running():
            print(f"A 'USING PERIODIC COMMIT' query is still running. Checking again in {check_interval // 60} minutes...")
            time.sleep(check_interval)
        else:
            print("No 'USING PERIODIC COMMIT' queries are running. Exiting monitoring.")
            break

start = timeit.default_timer()
monitor_queries()
stop = timeit.default_timer()
print('Run time: ', stop - start)
