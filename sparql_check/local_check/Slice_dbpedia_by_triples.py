import json
from multiprocessing import Process
from os import listdir
from os.path import isfile, join
from tqdm import tqdm

from utils.Sparql_Utils import parse_triple_from_query_answer
from utils.DBPedia_Utils import parse_dbpedia_line
def extract_triple_id_map(file, idflag):

    triple_id_map = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)

            id = question['id']
            query = question['query']['sparql']
            triples = parse_triple_from_query_answer(query, question['answers'])

            for triple in triples:
                triple_tuple = (triple[0], triple[1], triple[2])
                if triple_tuple not in triple_id_map:
                    triple_id_map[triple_tuple] = set()
                triple_id_map[triple_tuple].add(idflag + '_' + str(id))

    return triple_id_map



def slice_dbpedia(dbpedia_file, filter_result_file, query_triples):

    hit_triple_set = set()
    with open(dbpedia_file, encoding='utf-8') as fin, open(filter_result_file, encoding='utf-8', mode='w') as fout:
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue

            subj, pred, obj = parse_dbpedia_line(l)
            possible_hits = [(subj, pred, obj), ('VAR', pred, obj), (subj, pred, 'VAR'), (subj, 'VAR', pred),
                             ('VAR', 'VAR', obj), (subj, 'VAR', 'VAR'), ('VAR', pred, 'VAR')]

            for hit in possible_hits:
                if hit in query_triples:
                    fout.write(l)
                    hit_triple_set.add(hit)
    return hit_triple_set



if __name__ == '__main__':

    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    train_triple_ids = extract_triple_id_map(qald_multilingual_train, 'train')

    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'
    test_triple_ids = extract_triple_id_map(qald_multilingual_test, 'test')

    all_triples = train_triple_ids

    for triple in test_triple_ids:
        if not triple in all_triples:
            all_triples[triple] = test_triple_ids[triple]
        else:
            all_triples[triple].update(test_triple_ids[triple])


    all_triples[('VAR','VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>')] = set('exception')
    all_triples[('VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>', 'VAR')] = set('exception')
    all_triples[('<http://dbpedia.org/resource/Daniel_Jurafsky>', 'VAR', 'VAR')] = set('exception')

    all_triples_file = './data/QALD/all_triples.txt'
    with open(all_triples_file, encoding='utf-8', mode='w') as fout:
        for triple in all_triples:
            unknown_count = 0
            for i in range(0, 3):
                if triple[i] == 'VAR':
                    unknown_count += 1
            if True:#unknown_count >= 2:
                fout.write('%s\t%s\t%s\t{%s}\n' % (triple[0], triple[1], triple[2], str(all_triples[triple])))


    do_slice = True
    if do_slice:

        dbpedia_data_dir = 'D:/Research/kbqa/data/DBPedia/EN1610/'
        all_file_names = [f[0:-4] for f in listdir(dbpedia_data_dir) if
                         isfile(join(dbpedia_data_dir, f)) and f.endswith('.ttl')]

        total_hit_dict = {}
        for name in all_file_names:
            dbpedia_file = '%s/%s.ttl' % (dbpedia_data_dir, name)
            filter_file = '%s/filtered/%s.ttl' % (dbpedia_data_dir, name + '_filter')
            p = Process(target=slice_dbpedia, args=(dbpedia_file, filter_file, set(all_triples.keys())))
            p.start()

