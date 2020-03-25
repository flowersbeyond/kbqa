from utils.Sparql_Utils import tokenize,extract_triples, extract_prefixes, unify_triple_item_format
from utils.Rdf_Utils import public_prefix
import json

sparql_keywords = ['select', 'construct', 'ask', 'describe',
                   'distinct','named', 'where', 'limit',
                   'group', 'order', 'count', 'by','max', 'min', 'avg', 'sum',
                   'having', 'offset', 'optional', 'union', 'minus', 'filter', 'exists',
                   'bindings', 'isuri', 'isblank', 'isliteral', 'bound', 'regex', 'graph']
def get_sketch(query):
    tokens = tokenize(query)
    prefixes = extract_prefixes(query)
    triples = extract_triples(query)

    vars = {}
    entities = {}
    preds = {}
    for triple in triples:
        for ele in triple:
            if ele.startswith('?'):
                if ele not in vars:
                    vars[ele] = 'var'+ str(len(vars))

        subj_obj = [triple[0], triple[2]]
        for item in subj_obj:
            if not item.startswith('?') and item not in entities:
                entities[item] = 'ent' + str(len(entities))

        pred = triple[1]
        if not pred.startswith('?'):
            if pred not in preds:
                preds[pred] = 'pred' + str(len(preds))

    i = 0
    while i < len(tokens):
        if tokens[i].lower() in ['select', 'ask']:
            break
        i += 1

    sketch_tokens = []
    while i < len(tokens):

        token = tokens[i]
        if token.lower() in sparql_keywords:
            sketch_tokens.append(token.upper())

        elif token.startswith('(') and token.endswith(')'):
            sketch_tokens.append('CONDITION')

        elif token.startswith('?'):
            token = token.strip('.')
            if token not in vars:
                vars[token] = 'var'+ str(len(vars))
            sketch_tokens.append(vars[token])
        elif token == '.':
            i += 1
            continue
        elif token in ['{', '}', ';']:
            sketch_tokens.append(token)

        else:
            complete_token = inflate_prefix(token, prefixes)
            if complete_token in entities:
                token = entities[complete_token]
            elif complete_token in preds:
                token = preds[complete_token]
            else:
                token = 'UNK'
            sketch_tokens.append(token)

        i += 1

    sketch = ' '.join(sketch_tokens)
    return sketch

def inflate_prefix(token, prefixes):
    if token == 'a':
        token = 'rdf:type'
    if token.endswith('.'):
        if token.find(':') != -1 or token.startswith('?'):
            token = token.strip('.')
        # else:
        #    print(query + ':\t' + item)

    inflated_token = token
    if token.find(':') != -1 and not token.startswith('<') and not token.startswith('\"') and not token.startswith('\''):
        pair = token.split(':')
        if pair[0] not in prefixes:
            inflated_token = public_prefix[pair[0]][0:-1] + pair[1] + '>'
            # if item != 'rdf:type':
            #    print(query)
        else:
            inflated_token = prefixes[pair[0]][0:-1] + pair[1] + '>'

    inflated_token = unify_triple_item_format(inflated_token)

    return inflated_token


def extract_qald_multilingual_queries(file):
    queries = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']
            query = question['query']['sparql']
            queries[id]=query

    return queries


def extract_qald_7_largescale_queries(data_file, ans_detail):
    queries = {}
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
                queries[id]=q['sparql']
    return queries


if __name__ == '__main__':

    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    queries = extract_qald_multilingual_queries(qald_multilingual_train)
    train_sketch_map = {}
    for id in queries:
        sketch = get_sketch(queries[id])
        if sketch not in train_sketch_map:
            train_sketch_map[sketch] = []
        train_sketch_map[sketch].append(id)

    qald_multilingual_train_stat = './data/QALD/train-multilingual-4-9-sketch_stat.json'
    with open(qald_multilingual_train_stat, encoding='utf-8', mode='w') as fout:
        for sketch in train_sketch_map:
            fout.write(json.dumps({'sketch': sketch, 'ids': train_sketch_map[sketch]}) + '\n')

    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'
    queries = extract_qald_multilingual_queries(qald_multilingual_test)
    test_sketch_map = {}
    for id in queries:
        sketch = get_sketch(queries[id])
        if sketch not in test_sketch_map:
            test_sketch_map[sketch] = []
        test_sketch_map[sketch].append(id)

    qald_multilingual_test_stat = './data/QALD/test-multilingual-4-9-sketch_stat.json'
    with open(qald_multilingual_test_stat, encoding='utf-8', mode='w') as fout:
        for sketch in test_sketch_map:
            fout.write(json.dumps({'sketch': sketch, 'ids': test_sketch_map[sketch]}) + '\n')


    qald_7_largescale = './data/QALD/7/data/large-scale/qald-7-test-largescale-sample.json'
    qald_7_largescale_ans_detail = './data/QALD/7/data/large-scale/qald-7-test-largescale-sample_detail.txt'
    queries = extract_qald_7_largescale_queries(qald_7_largescale, qald_7_largescale_ans_detail)
    qald7_sketch_map = {}
    for id in queries:
        sketch = get_sketch(queries[id])
        if sketch not in qald7_sketch_map:
            qald7_sketch_map[sketch] = []
        qald7_sketch_map[sketch].append(id)

    qald7_sketch_stat = './data/QALD/qald7_sketch_stat.json'
    with open(qald7_sketch_stat, encoding='utf-8', mode='w') as fout:
        for sketch in qald7_sketch_map:
            fout.write(json.dumps({'sketch':sketch, 'ids': qald7_sketch_map[sketch]}) + '\n')



    sketch_summary_file = './data/QALD/sketch_stat.txt'
    with open(sketch_summary_file, encoding='utf-8', mode='w') as fout:
        fout.write('train sketch num:\t%d\n'% len(train_sketch_map))
        fout.write('test sketch num:\t%d\n'% len(test_sketch_map))
        fout.write('qald7 sketch num:\t%d\n'% len(qald7_sketch_map))

        train_sketch_set = set(train_sketch_map.keys())
        test_sketch_set = set(test_sketch_map.keys())
        qald7_sketch_set = set(qald7_sketch_map.keys())

        fout.write('train compare with test:\n')
        fout.write('train distinct:\t%d\n' % len(train_sketch_set.difference(test_sketch_set)))
        fout.write('test distinct:\t%d\n' % len(test_sketch_set.difference(train_sketch_set)))

        fout.write('multilingual compare with qald7:\n')
        fout.write('multilingual train distinct:\t%d\n' % len(train_sketch_set.difference(qald7_sketch_set)))
        fout.write('multilingual test distinct:\t%d\n' % len(test_sketch_set.difference(qald7_sketch_set)))
        fout.write('qald7 distinct:\t%d\n' % len(qald7_sketch_set.difference(train_sketch_set.union(test_sketch_set))))

