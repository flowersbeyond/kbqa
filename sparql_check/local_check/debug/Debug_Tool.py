from sparql_check.local_check.DBPedia_Utils import parse_dbpedia_line

'''
extend_cover_triples_file = ''

all_triples_file = './data/QALD/all_triples.txt'
all_triples = set()
with open(all_triples_file, encoding='utf-8') as fin:
    for l in fin:
        items = l.split('\t')
        triple = (items[0], items[1], items[2])
        all_triples.add(triple)

full_triple_file_map = {}

hit_triple_core_dbfilemap = './data/QALD/hit_triple_core_dbfilemap.txt'
with open(hit_triple_core_dbfilemap, encoding='utf-8') as fin:
    for l in fin:
        items = l.strip().split('\t')
        triple = (items[0], items[1], items[2])
        if triple not in full_triple_file_map:
            full_triple_file_map[triple] = [items[3]]
        else:
            full_triple_file_map[triple].append(items[3])

hit_triple_extend_core_dbfilemap = './data/QALD/hit_triple_extend_core_dbfilemap.txt'
with open(hit_triple_extend_core_dbfilemap, encoding='utf-8') as fin:
    for l in fin:
        items = l.strip().split('\t')
        triple = (items[0], items[1], items[2])
        if triple not in full_triple_file_map:
            full_triple_file_map[triple] = [items[3]]
        else:
            full_triple_file_map[triple].append(items[3])

conflict_groups = set()
for triple in full_triple_file_map:
    if len(full_triple_file_map[triple]) >=2:
        conflict_groups.add((full_triple_file_map[triple][0], full_triple_file_map[triple][1]))

for item in conflict_groups:
    print(str(item))
'''

merged_extend_core_file = './data/DBPedia/slices/merge/merge_core_extend.ttl'
triple_line_map = {}
with open(merged_extend_core_file, encoding='utf-8') as fin:
    for l in fin:
        subj, pred, obj = parse_dbpedia_line(l)
        triple_line_map[(subj, pred, obj)] = l.strip()

question_triple1 = ['<http://dbpedia.org/resource/A_Hit_Is_a_Hit>', '<http://dbpedia.org/ontology/series>', '<http://dbpedia.org/resource/The_Sopranos>']
question_triple1 = ['<http://dbpedia.org/resource/Alice_Comedies>', '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://dbpedia.org/ontology/TelevisionShow>']
question_triple2 = ['<http://dbpedia.org/resource/A_Hit_Is_a_Hit>', '<http://dbpedia.org/ontology/seasonNumber>', '1']
question_triple2 = ['<http://dbpedia.org/resource/Alice_Comedies>', '<http://dbpedia.org/ontology/creator>', '<http://dbpedia.org/resource/Walt_Disney>']

question_triple1 = ['<http://dbpedia.org/resource/Buchanan_Field_Airport>',	'<http://dbpedia.org/ontology/operator>',	'<http://dbpedia.org/resource/California>']

triple_tuple1 = (question_triple1[0], question_triple1[1], question_triple1[2])
triple_tuple2 = (question_triple2[0], question_triple2[1], question_triple2[2])
if triple_tuple1 in triple_line_map:
    print(triple_line_map[triple_tuple1])
if triple_tuple2 in triple_line_map:
    print(triple_line_map[triple_tuple2])