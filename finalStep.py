import timeit
import os
from vfb_connect.cross_server_tools import VfbConnect
vc = VfbConnect(neo_endpoint="http://pdb.ug.virtualflybrain.org", neo_credentials=('neo4j',str(os.environ.get('PDBpass'))))

start = timeit.default_timer()
print("Add has_neuron/region_connectivity labels...")
vc.nc.commit_list(statements=[
    'MATCH p=(a:Neuron)-[r:synapsed_to]->(b:Neuron) WHERE exists(r.weight) SET a:has_neuron_connectivity SET b:has_neuron_connectivity',
    'MATCH (n:Neuron)-[r:has_presynaptic_terminals_in]->(c:Synaptic_neuropil) SET n:has_region_connectivity',
    'MATCH (n:Neuron)-[r:has_postsynaptic_terminal_in]->(c:Synaptic_neuropil) SET n:has_region_connectivity'
])
stop = timeit.default_timer()
print('Run time: ', stop - start) 

start = timeit.default_timer()
print("Clean NBLAST...")
vc.nc.commit_list(statements=[
    'MATCH (a:NBLAST) REMOVE a:NBLAST',
    'MATCH (a:Individual)-[r1:has_similar_morphology_to]->(a) WHERE r1.NBLAST_score[0] = 1 DELETE r1',
    'MATCH (a:Individual)-[r1:has_similar_morphology_to]->(b:Individual) MATCH (b)-[r2:has_similar_morphology_to]->(a) WHERE not r1.NBLAST_score[0] = r2.NBLAST_score[0] MATCH (b)-[r:has_similar_morphology_to]-(a) WITH r1, r2, avg(r.NBLAST_score[0]) as mean SET r1.NBLAST_score=[mean] DELETE r2',
    'MATCH (a:Individual)-[r1:has_similar_morphology_to]->(b:Individual) MATCH (b)-[r2:has_similar_morphology_to]->(a) WHERE r1.NBLAST_score[0] = r2.NBLAST_score[0] DELETE r2',
    'MATCH (a:Individual)-[nblast:has_similar_morphology_to]-(b:Individual) SET a:NBLAST SET b:NBLAST'
])
stop = timeit.default_timer()
print('Run time: ', stop - start) 

start = timeit.default_timer()
print("Add any missing Project Labels...")
vc.nc.commit_list(statements=[
    'MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:"catmaid_fafb"}) SET i:FAFB',
    'MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:"catmaid_l1em"}) SET i:L1EM',
    'MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:"catmaid_fanc"}) SET i:FANC',
    'MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:"neuprint_JRC_Hemibrain_1point1"}) SET i:FlyEM_HB',
    'MATCH (i:Individual)-[:database_cross_reference]->(s:Site {short_form:"FlyCircuit"}) SET i:FlyCircuit'
])
stop = timeit.default_timer()
print('Run time: ', stop - start) 

start = timeit.default_timer()
print("expand any missing synonyms...")
vc.nc.commit_list(statements=['MATCH (primary) WHERE exists(primary.has_exact_synonym) WITH primary, reduce(syns = [], syn IN primary.has_exact_synonym | syns + [apoc.convert.fromJsonMap(syn)]) as syns UNWIND syns as syn MATCH (p:pub {short_form:coalesce(split(syn.annotations.database_cross_reference[0],":")[1],"Unattributed")}) MERGE (primary)-[r:has_reference {typ:"syn",value:[syn.value]}]->(p) ON CREATE SET r += {iri: "http://purl.org/dc/terms/references", scope: "has_exact_synonym", short_form: "references", typ: "syn", label: "has_reference", type: "Annotation"}','MATCH (primary) WHERE exists(primary.has_broad_synonym) WITH primary, reduce(syns = [], syn IN primary.has_broad_synonym | syns + [apoc.convert.fromJsonMap(syn)]) as syns UNWIND syns as syn MATCH (p:pub {short_form:coalesce(split(syn.annotations.database_cross_reference[0],":")[1],"Unattributed")}) MERGE (primary)-[r:has_reference {typ:"syn",value:[syn.value]}]->(p) ON CREATE SET r += {iri: "http://purl.org/dc/terms/references", scope: "has_broad_synonym", short_form: "references", typ: "syn", label: "has_reference", type: "Annotation"}','MATCH (primary) WHERE exists(primary.has_narrow_synonym) WITH primary, reduce(syns = [], syn IN primary.has_narrow_synonym | syns + [apoc.convert.fromJsonMap(syn)]) as syns UNWIND syns as syn MATCH (p:pub {short_form:coalesce(split(syn.annotations.database_cross_reference[0],":")[1],"Unattributed")}) MERGE (primary)-[r:has_reference {typ:"syn",value:[syn.value]}]->(p) ON CREATE SET r += {iri: "http://purl.org/dc/terms/references", scope: "has_narrow_synonym", short_form: "references", typ: "syn", label: "has_reference", type: "Annotation"}','MATCH (primary) WHERE exists(primary.has_related_synonym) WITH primary, reduce(syns = [], syn IN primary.has_related_synonym | syns + [apoc.convert.fromJsonMap(syn)]) as syns UNWIND syns as syn MATCH (p:pub {short_form:coalesce(split(syn.annotations.database_cross_reference[0],":")[1],"Unattributed")}) MERGE (primary)-[r:has_reference {typ:"syn",value:[syn.value]}]->(p) ON CREATE SET r += {iri: "http://purl.org/dc/terms/references", scope: "has_related_synonym", short_form: "references", typ: "syn", label: "has_reference", type: "Annotation"}'])
stop = timeit.default_timer()
print('Run time: ', stop - start) 

start = timeit.default_timer()
print("Ensure all depreciated are labelled as such...")
vc.nc.commit_list(statements=["MATCH (n:Individual) WHERE exists(n.deprecated) AND n.deprecated = [true] AND not n:Deprecated SET n:Deprecated"])
stop = timeit.default_timer()
print('Run time: ', stop - start) 

start = timeit.default_timer()


print("Adding ALL SWC <-> SWC NBLAST scores...")

import csv
vc = None

start = timeit.default_timer()

tsv_file = open("swc_swc.tsv")
read_tsv = csv.reader(tsv_file, delimiter="\t")
statements=[]
for row in read_tsv:
	if row[0].startswith('VFB_'):
		try:  
			m = " SET r.mirrored=false"
			if 'y' in row[3]:
				m = " SET r.mirrored=true"
			statements.append("MATCH (b:Individual),(s:Individual) WHERE b.short_form IN ['" + str(row[0]) + "'] AND s.short_form IN ['" + str(row[1]) + "'] MERGE (s)-[r:has_similar_morphology_to {iri:'http://n2o.neo/custom/has_similar_morphology_to',short_form:'has_similar_morphology_to',type: 'Annotation'}]->(b) SET r.NBLAST_score=[" + row[2] + "]" + m)
		except:
			print(row)

statements.append("MATCH (a:Individual)-[r:has_similar_morphology_to]->(b:Individual) WHERE exists(r.NBLAST_score) SET a:NBLAST SET b:NBLAST")

vc = VfbConnect(neo_endpoint="http://pdb.ug.virtualflybrain.org", neo_credentials=('neo4j','vfb'))
try:
	vc.nc.commit_list_in_chunks(statements=statements, chunk_length=2000)
except:
	print(statements)

stop = timeit.default_timer()
print('Run time: ', stop - start) 

print("Adding SPLITS <-> SWC NBLAST scores...")

vc = None

start = timeit.default_timer()

tsv_file = open("splits_swc.tsv")
read_tsv = csv.reader(tsv_file, delimiter="\t")
statements=[]
for row in read_tsv:
	if row[0].startswith('VFB_'):
		try:  
			statements.append("MATCH (b:Individual),(s:Individual) WHERE b.short_form IN ['" + str(row[1]) + "'] AND s.short_form IN ['" + str(row[0]) + "'] MERGE (s)-[r:has_similar_morphology_to_part_of {iri:'http://n2o.neo/custom/has_similar_morphology_to_part_of',short_form:'has_similar_morphology_to_part_of',type: 'Annotation'}]->(b) SET r.NBLAST_score=[" + row[2] + "]")
		except:
			print(row)

statements.append("MATCH (a:Individual)-[r:has_similar_morphology_to_part_of]->(b:Individual) WHERE exists(r.NBLAST_score) SET a:NBLASTexp SET b:NBLASTexp")

vc = VfbConnect(neo_endpoint="http://pdb.ug.virtualflybrain.org", neo_credentials=('neo4j','vfb'))
try:
	vc.nc.commit_list_in_chunks(statements=statements, chunk_length=2000)
except:
	print(statements)

stop = timeit.default_timer()
print('Run time: ', stop - start) 


start = timeit.default_timer()
vc = None
print("Add Neuronbridge Hemibrain <-> slide code top 20 scores...")
print("loading lookups...")
vc = VfbConnect(neo_endpoint="http://pdb.ug.virtualflybrain.org", neo_credentials=('neo4j','vfb'))

result=vc.nc.commit_list(statements=["MATCH (n:API {short_form:'jrc_slide_code_api'})<-[r:database_cross_reference]-(i:Individual:Adult) WHERE (i)<-[:depicts]-(:Individual)-[:in_register_with]->(:Template {short_form:'VFBc_00101567'}) WITH r.accession as id, i.short_form as vfbid ORDER BY id ASC WITH distinct {dbid:id,vfbid:collect(vfbid)} as cross RETURN collect(cross) as lookup"])
lookup=result[0]['data'][0]['row'][0]
slidecode={}
for row in lookup:
    slidecode[row['dbid'][0]]=row['vfbid']

result=vc.nc.commit_list(statements=["MATCH (n:Site {short_form:'neuronbridge'})<-[r:database_cross_reference]-(i:Individual:Adult)-[:has_source]->(:DataSet {short_form:'Xu2020NeuronsV1point1'}) WHERE (i)<-[:depicts]-(:Individual)-[:in_register_with]->(:Template {short_form:'VFBc_00101567'}) WITH r.accession as id, i.short_form as vfbid ORDER BY id ASC WITH distinct {dbid:id,vfbid:collect(vfbid)} as cross RETURN collect(cross) as lookup"])
lookup=result[0]['data'][0]['row'][0]
bodyid={}
for row in lookup:
    bodyid[row['dbid'][0]]=row['vfbid']    

import csv

vc = None
print("reading results...")

tsv_file = open("top20_scores_agg.tsv")
read_tsv = csv.reader(tsv_file, delimiter="\t")
statements=[]
for row in read_tsv:
    if row[0] in bodyid.keys():
        if row[1] in slidecode.keys():
            try:  
                statements.append("MATCH (b:Individual),(s:Individual) WHERE b.short_form IN " + str(bodyid[row[0]]) + " AND s.short_form IN " + str(slidecode[row[1]]) + " MERGE (s)-[r:has_similar_morphology_to_part_of {iri:'http://n2o.neo/custom/has_similar_morphology_to_part_of',short_form:'has_similar_morphology_to_part_of',type: 'Annotation'}]->(b) SET r.neuronbridge_score=['" + row[2] + "'] ")
            except:
                print(row)

statements.append("MATCH (a:Individual)-[r:has_similar_morphology_to_part_of]->(b:Individual) WHERE exists(r.neuronbridge_score) SET a:neuronbridge SET b:neuronbridge")

print("loading into PDB...")

vc = VfbConnect(neo_endpoint="http://pdb.ug.virtualflybrain.org", neo_credentials=('neo4j','vfb'))

vc.nc.commit_list_in_chunks(statements=statements, chunk_length=2000)

stop = timeit.default_timer()
print('Run time: ', stop - start) 

start = timeit.default_timer()
print("Add any missing unique facets...")
vc.nc.commit_list(statements=[
    'MATCH (n:Deprecated) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["Deprecated"];',
    'MATCH (n:Deprecated) WHERE EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" AND NOT "Deprecated" IN n.uniqueFacets SET n.uniqueFacets=n.uniqueFacets + ["Deprecated"];',
    'MATCH (n:DataSet) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["DataSet"];',
    'MATCH (n:pub) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["pub"];',
    'MATCH (n:Person) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["Person"];',
    'MATCH (n:Site) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["Site"];',
    'MATCH (n:API) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["API"];',
    'MATCH (n:License) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["License"];',
    'MATCH (n:Expression_pattern:Split) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["Expression_pattern","Split"];',
    'MATCH (n:Expression_pattern) WHERE NOT EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" SET n.uniqueFacets=["Expression_pattern"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form starts with "FBtp" SET n.uniqueFacets=["Transgenic_Construct"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form starts with "FBti" SET n.uniqueFacets=["Insertion"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form starts with "FBal" SET n.uniqueFacets=["Allele"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form starts with "FBgn" SET n.uniqueFacets=["Gene"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form starts with "FBrf" SET n.uniqueFacets=["FB_Reference"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form starts with "FBim" SET n.uniqueFacets=["FB_Image"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) AND n.short_form starts with "FBdv" SET n.uniqueFacets=["Stage"];',
    'MATCH (n:Class) WHERE NOT EXISTS(n.uniqueFacets) SET n.uniqueFacets=["Class"];',
    'MATCH (n:Split) WHERE EXISTS(n.uniqueFacets) AND NOT n.short_form starts with "VFBc_" AND NOT "Split" IN n.uniqueFacets SET n.uniqueFacets=n.uniqueFacets + ["Split"];'
])
stop = timeit.default_timer()
print('Run time: ', stop - start) 

