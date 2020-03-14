import json
import re
from tqdm import tqdm
from multiprocessing import Process

public_prefix = {
                    'owl': '<http://www.w3.org/2002/07/owl#>',
                    'xsd': '<http://www.w3.org/2001/XMLSchema#>',
                    'rdfs': '<http://www.w3.org/2000/01/rdf-schema#>',
                    'rdf':'<http://www.w3.org/1999/02/22-rdf-syntax-ns#>',
                    'foaf': '<http://xmlns.com/foaf/0.1/>',
                    'yago': '<http://dbpedia.org/class/yago/>',
                    'dbo': '<http://dbpedia.org/ontology/>',
                    'dbp': '<http://dbpedia.org/property/>',
                    'dbr': '<http://dbpedia.org/resource/>',
                    'dct': '<http://purl.org/dc/terms/>',
                    'dbc': '<http://dbpedia.org/resource/Category:>'
                 }
def extract_id_query(file, idflag):

    queries = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']
            query = question['query']['sparql']

            query = unify_query(query)

            if query not in queries:
                queries[query] = []
            queries[query].append(idflag + '_' + str(id))
    return queries


def unify_query(query):
    query = query.replace('\n', ' ')
    query = re.sub(r"\s+", " ", query)
    query = query.strip()

    # sort prefix:
    prefix = ''
    body = ''
    if query.find("ASK") != -1:
        prefix = query[0: query.find('ASK')].strip()
        body = query[query.find('ASK'):].strip()

    elif query.find("SELECT") != -1:
        prefix = query[0:query.find('SELECT')].strip()
        body = query[query.find('SELECT'):].strip()
    else:
        print("ill format query: %s" % query)

    if prefix != '' or body != '':
        prefixes = prefix.split('PREFIX')

        pre_list = []
        for pre in prefixes:
            if pre.strip() != '':
                pre_list.append(pre.strip())

        sorted_pre_list = sorted(pre_list)

        final_prefix_str = ''
        for pre in sorted_pre_list:
            final_prefix_str = final_prefix_str + 'PREFIX ' + pre.strip() + ' '
        final_prefix_str = final_prefix_str.strip()

        final_query = final_prefix_str + ' ' + body

        return final_query

    return None

def tokenize(query):
    query = query.replace('\'', '\"')
    tokens = []

    pos = 0

    parenthesis_layer = 0
    while pos < len(query):
        token = ''
        if query[pos] == ' ':
            pos += 1
            continue
        elif query[pos] in ['{', '}', '.', ';']:
            token = query[pos]
            pos += 1
        elif query[pos] == '(':
            parenthesis_layer = 1
            token = query[pos]

            while parenthesis_layer != 0:
                pos += 1
                if query[pos] == '(':
                    parenthesis_layer += 1
                if query[pos] == ')':
                    parenthesis_layer -= 1
                token += query[pos]
            pos += 1
        elif query[pos] == '\"':
            token = query[pos]
            pos += 1
            while query[pos] != '\"':
                token += query[pos]
                pos += 1
            while query[pos] != ' ':
                token += query[pos]
                pos += 1

        elif query[pos] == '<' and parenthesis_layer == 0:
            while query[pos] != '>':
                token += query[pos]
                pos += 1
            token += '>'
            pos += 1
        else:
            while pos < len(query) and query[pos] not in ['{', '}', '<', '>', '(', ')', '.', ';', ' ']:
                token += query[pos]
                pos += 1
        tokens.append(token)
    return tokens

def parse_triple(triple_statements):
    if len(triple_statements) <=2:
        print('triple less than 3 tokens:' + str(triple_statements))
        return []

    triples = []
    pos = 0

    while pos < len(triple_statements):
        if triple_statements[pos] == '.':
            pos += 1
            continue

        sub = triple_statements[pos]
        pred = triple_statements[pos + 1]
        obj = triple_statements[pos + 2]
        triples.append([sub, pred, obj])

        pos = pos + 3
        while pos < len(triple_statements) and (triple_statements[pos]) == ';':
            pred = triple_statements[pos + 1]
            obj = triple_statements[pos + 2]
            triples.append([sub, pred, obj])
            pos = pos + 3

    return triples






def extract_triples_from_block(block):
    pos = 0
    triples = []
    while pos < len(block):
        if block[pos] in ['FILTER', 'filter', 'optional', 'OPTIONAL', 'union', 'UNION']:
            while pos < len(block) and block[pos] != '{' and block[pos] != '}':
                pos += 1
        elif block[pos] == '.':
            pos += 1
        elif block[pos] == '{':
            bracket_block = []
            bracket_layer = 1
            bracket_block.append(block[pos])
            while bracket_layer != 0:
                pos += 1
                if block[pos] == '{':
                    bracket_layer += 1
                if block[pos] == '}':
                    bracket_layer -= 1
                bracket_block.append(block[pos])
            triples.extend(extract_triples_from_block(bracket_block[1: -1]))
            pos = pos + 1
        else:
            triple_statements = []
            triple_statements.append(block[pos])
            pos += 1
            while pos < len(block) and block[pos] not in ['{', 'FILTER', 'filter', 'optional', 'OPTIONAL', 'union', 'UNION']:
                triple_statements.append(block[pos])
                pos += 1
            triples.extend(parse_triple(triple_statements))

    return triples


def extract_triples(query):
    tokens = tokenize(query)
    prefixes = {}
    triples = []

    pos = 0

    while pos < len(tokens):
        if tokens[pos].lower() == 'prefix':
            name = tokens[pos + 1][0:-1]
            value = tokens[pos + 2]
            prefixes[name] = value
            pos = pos + 3
        elif tokens[pos] == '{':
            bracket_block = []
            bracket_layer = 1
            bracket_block.append(tokens[pos])
            while bracket_layer != 0:
                pos += 1
                if tokens[pos] == '{':
                    bracket_layer += 1
                if tokens[pos] == '}':
                    bracket_layer -= 1
                bracket_block.append(tokens[pos])
            triples.extend(extract_triples_from_block(bracket_block[1: -1]))
            pos = pos + 1
        else:
            pos = pos + 1

    for triple in triples:
        if len(triple) != 3:
            continue
        for i in range(0, 3):
            item = triple[i]
            replaced_item = item
            if item.startswith('?'):
                replaced_item = 'VAR'
            elif item.find(':') != -1 and not item.startswith('<') and not item.startswith('\"'):
                pair = item.split(':')
                if pair[0] not in prefixes:
                    replaced_item = public_prefix[pair[0]][0:-1] + pair[1] + '>'
                else:
                    replaced_item = prefixes[pair[0]][0:-1] + pair[1] + '>'

            triple[i] = unify_triple_item_format(replaced_item)

    return triples

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
def slice_dbpedia(dbpedia_file, filter_result_file, query_triples):
    with open(dbpedia_file, encoding='utf-8') as fin, open(filter_result_file, encoding='utf-8', mode='w') as fout:
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue

            subj = l[0:l.find('>') + 1]
            l = l[l.find('>') + 1:].strip()
            pred = l[0:l.find('>') + 1]
            l = l[l.find('>') + 1:].strip().strip('.').strip()
            obj = l


            db_triple = [subj, pred, obj]
            for i in range(0, 3):
                db_triple[i] = unify_triple_item_format(db_triple[i])


            useful = False
            for q_triple in query_triples:
                match = True
                for i in range(0, 3):
                    if q_triple[i] != 'VAR' and q_triple[i] != db_triple[i]:
                        match=False
                        break
                if match:
                    useful = True
                    break
            if useful:
                fout.write(l)


if __name__ == '__main__':



    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    train_id_queries = extract_id_query(qald_multilingual_train, 'train')

    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'
    test_id_queries = extract_id_query(qald_multilingual_test, 'test')

    full_query_id_map = {}
    for query in train_id_queries:
        full_query_id_map[query] = train_id_queries[query]
    for query in test_id_queries:
        if query not in full_query_id_map:
            full_query_id_map[query] = []
        full_query_id_map[query].extend(test_id_queries[query])

    all_queries_file = './data/QALD/all_queries.txt'
    with open(all_queries_file, encoding='utf-8', mode='w') as fout:
        for q in full_query_id_map:
            fout.write(q + '\n')

    full_query_id_map_file = './data/QALD/all_queries_id_map.jsonl'
    with open(full_query_id_map_file, encoding='utf-8', mode='w') as fout:
        for q in full_query_id_map:
            fout.write(json.dumps({'query':q, 'id':full_query_id_map[q]}) + '\n')

    all_triples = []

    for query in full_query_id_map:
        triples = extract_triples(query)
        all_triples.extend(triples)

    all_triples.append(['VAR','VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>'])
    all_triples.append(['VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>', 'VAR'])


    dbpedia_data_dir = './data/DBPedia/core8/'
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

    for name in core_names:
        dbpedia_file = '%s/%s.ttl' % (dbpedia_data_dir, name)
        filter_file = '%s/%s.ttl' % (dbpedia_data_dir, name + '_filter')
        p = Process(target=slice_dbpedia, args=(dbpedia_file, filter_file, all_triples))
        p.start()