from tqdm import tqdm
from os import listdir
from os.path import isfile, join
from multiprocessing import Process
import bz2
import json
from sparql_check.local_check.DBPedia_Utils import parse_dbpedia_line
from sparql_check.local_check.Sparql_Utils import parse_triple_from_query_answer

def extract_still_not_covered_query_triples(
        data_file,
        triple_covered_failed_file,
        local_exe_result_detail_file
    ):


    not_covered_qids = []

    with open(triple_covered_failed_file, encoding='utf-8') as fin:
        for l in fin:
            id = int(l.split('\t')[0].strip(':'))
            not_covered_qids.append(id)
    with open(local_exe_result_detail_file, encoding='utf-8') as fin:
        for l in fin:
            id = int(json.loads(l)['id'])
            not_covered_qids.append(id)


    target_triples = set()
    with open(data_file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']

            if id in not_covered_qids:

                query = question['query']['sparql']
                triples = parse_triple_from_query_answer(query, question['answers'])

                for triple in triples:
                    triple_tuple = (triple[0], triple[1], triple[2])
                    target_triples.add(triple_tuple)

    return target_triples



def unify_triple_item_format(item):
    item = item.replace('\'', '\"')
    if item.startswith('<'):
        item = item[0: item.rfind('>') + 1]
    elif item.startswith('\"'):
        pos = 1
        token = ''
        while item[pos] != '\"':
            token += item[pos]
            pos += 1
        item = token
    return item


def slice_single_bz2_file(dbpedia_file, slice_result_file_name, target_triples):

    with bz2.open(dbpedia_file, mode='rt', encoding='utf-8') as fin, open(slice_result_file_name, encoding='utf-8', mode='w') as fout:

        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue

            subj, pred, obj = parse_dbpedia_line(l)

            possible_hits = [(subj, pred, obj), ('VAR', pred, obj), (subj, pred, 'VAR'), (subj, 'VAR', pred),
                             ('VAR', 'VAR', obj), (subj, 'VAR', 'VAR'), ('VAR', pred, 'VAR')]

            for hit in possible_hits:
                if hit in target_triples:
                    fout.write(l)




def slice_bz2_files(data_dir, sec, target_triples):
    for name in sec:
        dbpedia_file = '%s/%s.ttl.bz2'%(data_dir, name)
        slice_result_file_name = '%s/%s.ttl'%(data_dir, name + '_filter')
        slice_single_bz2_file(dbpedia_file, slice_result_file_name, target_triples)



if __name__ == '__main__':


    train_data_file = './data/QALD/train-multilingual-4-9.jsonl'
    train_triple_covered_failed_file = './data/QALD/train_failed.txt'
    train_local_result_compare_detail = './data/QALD/train-multilingual-4-9_local_result_compare_detail.jsonl'
    train_target_triples = extract_still_not_covered_query_triples(
        train_data_file,
        train_triple_covered_failed_file,
        train_local_result_compare_detail
    )

    test_data_file = './data/QALD/test-multilingual-4-9.jsonl'
    test_triple_covered_failed_file = './data/QALD/test_failed.txt'
    test_local_result_compare_detail = './data/QALD/test-multilingual-4-9_local_result_compare_detail.jsonl'
    test_target_triples = extract_still_not_covered_query_triples(
        test_data_file,
        test_triple_covered_failed_file,
        test_local_result_compare_detail
    )

    all_target_triples = train_target_triples.union(test_target_triples)


    full_dbpedia_data_dir = './data/DBPedia/EN1610/'

    all_bz2_files = [f for f in listdir(full_dbpedia_data_dir) if isfile(join(full_dbpedia_data_dir, f)) and f.endswith('.bz2')]
    core_names = [
        'labels_en',
        'category_labels_en',

        'article_categories_en',
        'instance_types_en',
        'infobox_properties_en',
        'mappingbased_literals_en',
        'mappingbased_objects_en',
        'persondata_en'

    ]

    file_sections = []
    for i in range( 0, 6):
        file_sections.append([])

    for i in range(0, len(all_bz2_files)):
        bz2_file_name = all_bz2_files[i]
        bz2_file_name = bz2_file_name[0:bz2_file_name.find('.ttl.bz2')]
        if bz2_file_name not in core_names:
            file_sections[i % 6].append(bz2_file_name)

    for sec in file_sections:
        p = Process(target=slice_bz2_files, args=(full_dbpedia_data_dir, sec, all_target_triples))
        p.start()
