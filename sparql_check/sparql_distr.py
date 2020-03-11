import json
import re

sparql_keywords = ['select', 'construct', 'ask', 'describe',
                   'distinct','named', 'where', 'limit',
                   'group', 'order', 'count', 'max', 'min', 'avg', 'sum',
                   'having', 'offset', 'optional', 'union', 'minus', 'filter', 'exists',
                   'bindings', 'isuri', 'isblank', 'isliteral', 'bound', 'regex', 'graph']
def isValidToken(token):

    if token.lower() in sparql_keywords:
        return True

    return False


def gen_feature(query):
    tokens = re.split('[<>\s\.,:;\{\}\(\)!]',query)
    feature = {}
    for t in tokens:
        if isValidToken(t):
            if t not in feature:
                feature[t] = 1
            else:
                feature[t] += 1


    return feature

def get_stat(queries):
    features = {}
    for q in queries:
        f = gen_feature(q)
        for i in f:
            if i not in features:
                features[i] = f[i]
            else:
                features[i] += f[i]

    unified_features = {}
    for key in sparql_keywords:
        unified_features[key] = 0

    for f in features:
        unified_features[f.lower()] += features[f]
    for key in sparql_keywords:
        unified_features[key] = unified_features[key] / len(queries)

    #sorted_features = sorted(features.items(), key=lambda x: x[1], reverse=True)

    return unified_features

def write_to_file(features, filename):
    with open(filename, encoding='utf-8', mode='w') as fout:
        for key in features:
            fout.write('%s\t%f\n' % (key, features[key]))


def extract_qald_multilingual_queries(file):
    queries = []
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            query = question['query']['sparql']
            queries.append(query)

    return queries


def extract_qald_7_largescale_queries(data_file, ans_detail):
    queries = []
    bad_ids = []
    with open(ans_detail, encoding='utf-8') as fin:
        for l in fin:
            id = json.loads(l)['id']
            bad_ids.append(id)

    with open(data_file, encoding='utf-8') as fd:
        data = json.load(fd)
        for q in data:
            id = int(q['id'])
            if id not in bad_ids:
                queries.append(q['sparql'])
    return queries


def extract_lcquad_1_queries(train, train_ans_detail,test,test_ans_detail):
    queries = []
    bad_ids = []
    with open(train_ans_detail, encoding='utf-8') as fin:
        for l in fin:
            id = json.loads(l)['id']
            bad_ids.append(id)

    with open(train, encoding='utf-8') as fd:
        data = json.load(fd)

        for q in data:
            id = int(q['_id'])
            if id not in bad_ids:
                queries.append(q['sparql_query'])

    bad_ids = []
    with open(test_ans_detail, encoding='utf-8') as fin:
        for l in fin:
            id = json.loads(l)['id']
            bad_ids.append(id)

    with open(test, encoding='utf-8') as fd:
        data = json.load(fd)

        for q in data:
            id = int(q['_id'])
            if id not in bad_ids:
                queries.append(q['sparql_query'])

    return queries


def main():
    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    queries = extract_qald_multilingual_queries(qald_multilingual_train)
    stat = get_stat(queries)
    qald_multilingual_train_stat = './data/QALD/train-multilingual-4-9-sparql_stat.txt'
    write_to_file(stat, qald_multilingual_train_stat)

    stat_in_one_place = './data/sparqlstat/qald-merge-train-sparql_stat.txt'
    write_to_file(stat, stat_in_one_place)

    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'
    queries = extract_qald_multilingual_queries(qald_multilingual_test)
    stat = get_stat(queries)
    qald_multilingual_test_stat = './data/QALD/train-multilingual-4-9-sparql_stat.jsonl'
    write_to_file(stat, qald_multilingual_test_stat)
    stat_in_one_place = './data/sparqlstat/qald-merge-test-sparql_stat.txt'
    write_to_file(stat, stat_in_one_place)

    qald_7_largescale = './data/QALD/7/data/large-scale/qald-7-test-largescale-sample.json'
    qald_7_largescale_ans_detail = './data/QALD/7/data/large-scale/qald-7-test-largescale-sample_detail.txt'
    queries = extract_qald_7_largescale_queries(qald_7_largescale, qald_7_largescale_ans_detail)
    stat = get_stat(queries)
    qald_7_largescale_stat = './data/QALD/7/data/large-scale/qald-7-test-largescale-sample-sparql_stat.txt'
    write_to_file(stat, qald_7_largescale_stat)

    stat_in_one_place = './data/sparqlstat/qald-7-largescale-sparql_stat.txt'
    write_to_file(stat, stat_in_one_place)

    lcquad_1_train = './data/LC-QuAD-1.0/train-data.json'
    lcquad_1_test = './data/LC-QuAD-1.0/test-data.json'
    lcquad_1_train_ans_detail = './data/LC-QuAD-1.0/train-data_detail.txt'
    lcquad_1_test_ans_detail = './data/LC-QuAD-1.0/test-data_detail.txt'
    queries = extract_lcquad_1_queries(lcquad_1_train, lcquad_1_train_ans_detail,lcquad_1_test,lcquad_1_test_ans_detail)
    stat = get_stat(queries)
    lcquad_1_stat = './data/LC-QuAD-1.0/lcquad-1_sparql_stat.txt'
    write_to_file(stat, lcquad_1_stat)

    stat_in_one_place = './data/sparqlstat/lcquad-1-sparql_stat.txt'
    write_to_file(stat, stat_in_one_place)

if __name__ == '__main__':
    main()