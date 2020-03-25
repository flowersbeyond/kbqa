import json
from tqdm import tqdm

from utils.Sparql_Execute_Utils import query_dbpedia
from utils.Rdf_Utils import public_prefix
from utils.Sparql_Utils import extract_prefixes
from rdflib.graph import Graph

def extract_id_query(data_file):

    questions = {}
    with open(data_file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']

            query = question['query']['sparql']
            questions[id] = query

    return questions

def append_missing_prefix(query):
    existing_prefixs = extract_prefixes(query)
    for pref in public_prefix:
        if pref not in existing_prefixs:
            query = ('PREFIX %s: %s ' % (pref, public_prefix[pref])) + query

    return query

def interpret_rdflib_result(qres):
    answers = []

    if qres.bindings != None:

        for binding in qres.bindings:
            for var in qres.vars:
                answer = {str(var): str(binding[var])}
                answers.append(answer)

    elif qres.askAnswer != None:
        answers = [{'bool': str(qres.askAnswer)}]

    else:
        for row in qres:
            answers.append((str(row)))

    return answers

def run_gold_query(data_file_name, ans_output_file, unknown_list, type, rdfgraph=None):
    print("Processing %s" % data_file_name)
    questions = extract_id_query(data_file_name)

    with open(ans_output_file, mode='w', encoding='utf-8') as fout:
        pbar = tqdm(questions)
        for id in pbar:
            if id in unknown_list:
                query = questions[id]
                query = append_missing_prefix(query)
                if type == 'jena':
                    result = query_dbpedia(query, "http://localhost:3030/ds/query")
                else:
                    try:
                        qres = rdfgraph.query(query)
                        result = interpret_rdflib_result(qres)

                    except Exception as e:
                        result = {'error': str(e)}

                result_dict = {}
                result_dict['id'] = id
                result_dict['result'] = result

                fout.write(json.dumps(result_dict).strip() + '\n')

def get_unknown_list(unknown_file):
    with open(unknown_file) as fin:
        l = fin.readline()
        unknown_list = json.loads(l)['unknown']
        return unknown_list

if __name__ == '__main__':

    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'


    dataset_mode = 'core'
    run_jena = False
    if run_jena:
        train_unknown_file = './data/QALD/%s_question_coverage_%s.txt' % ('train', dataset_mode)
        train_unknown_list = get_unknown_list(train_unknown_file)

        test_unknown_file = './data/QALD/%s_question_coverage_%s.txt' % ('train', dataset_mode)
        test_unknown_list = get_unknown_list(test_unknown_file)

        train_result = './data/QALD/train-multilingual-4-9_jena_%s_result.jsonl' % dataset_mode
        test_result = './data/QALD/test-multilingual-4-9_jena_%s_result.jsonl' % dataset_mode

        run_gold_query(qald_multilingual_train, train_result, train_unknown_list, type='jena')
        run_gold_query(qald_multilingual_test, test_result, test_unknown_list, type='jena')

    run_rdflib = True
    if run_rdflib:
        for dataset_mode in ['minicore', 'core', 'extend']:
            g = Graph()
            dbpedia_root_dir = 'D:/Research/kbqa/data/DBPedia/'
            ttl_file = dbpedia_root_dir + 'slices/merge/' + dataset_mode + '.ttl'
            print('begin loading.')
            g.parse(ttl_file, format='turtle')
            print('finished loading.')

            train_unknown_file = './data/QALD/%s_question_coverage_%s.txt' % ('train', dataset_mode)
            train_unknown_list = get_unknown_list(train_unknown_file)

            test_unknown_file = './data/QALD/%s_question_coverage_%s.txt' % ('train', dataset_mode)
            test_unknown_list = get_unknown_list(test_unknown_file)

            train_result = './data/QALD/train-multilingual-4-9_rdflib_%s_result.jsonl' % dataset_mode
            test_result = './data/QALD/test-multilingual-4-9_rdflib_%s_result.jsonl' % dataset_mode

            run_gold_query(qald_multilingual_train, train_result, train_unknown_list, type='rdflib', rdfgraph=g)
            run_gold_query(qald_multilingual_test, test_result, test_unknown_list, type='rdflib', rdfgraph=g)
