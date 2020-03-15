import json
import re
import urllib.parse
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
def extract_triple_id_map(file, idflag):

    triple_id_map = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']

            query = question['query']['sparql']

            triples = extract_triples(query)
            var_bindings = parse_var_bindings(question['answers'])
            triples = inflate_bindings(triples, var_bindings)

            for triple in triples:
                triple_tuple = (triple[0], triple[1], triple[2])
                if triple_tuple not in triple_id_map:
                    triple_id_map[triple_tuple] = set()
                triple_id_map[triple_tuple].add(idflag + '_' + str(id))

    return triple_id_map


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
            triples.extend(extract_triples_from_brace_block(bracket_block[1: -1]))
            pos = pos + 1
        else:
            pos = pos + 1

    for triple in triples:
        if len(triple) != 3:
            continue
        for i in range(0, 3):
            item = triple[i]
            replaced_item = item

            if item.find(':') != -1 and not item.startswith('<') and not item.startswith('\"'):
                pair = item.split(':')
                if pair[0] not in prefixes:
                    replaced_item = public_prefix[pair[0]][0:-1] + pair[1] + '>'
                else:
                    replaced_item = prefixes[pair[0]][0:-1] + pair[1] + '>'

            triple[i] = unify_triple_item_format(replaced_item)

    return triples


def tokenize(query):
    query = query.replace('\'', '\"')
    query = query.replace('\n', ' ')
    query = re.sub(r"\s+", " ", query)
    query = query.strip()

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


def extract_triples_from_brace_block(block):
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
            triples.extend(extract_triples_from_brace_block(bracket_block[1: -1]))
            pos = pos + 1
        else:
            triple_statements = []
            triple_statements.append(block[pos])
            pos += 1
            while pos < len(block) and block[pos] not in ['{', 'FILTER', 'filter', 'optional', 'OPTIONAL', 'union', 'UNION']:
                triple_statements.append(block[pos])
                pos += 1
            triples.extend(extract_triples_from_triple_statements(triple_statements))

    return triples


def extract_triples_from_triple_statements(triple_statements):
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


def parse_var_bindings(answer):
    parse_bindings = []
    head = answer['head']
    if 'vars' in head:

        bindings = answer['results']['bindings']
        for binding in bindings:
            if len(binding) > 0:
                parse_binding = {}
                for key in binding:
                    value = binding[key]['value']
                    value = urllib.parse.unquote(str(value), encoding='utf-8')
                    parse_binding[key] = value
                parse_bindings.append(parse_binding)

    return parse_bindings


def inflate_bindings(triples, var_bindings):
    inflated_triples = []
    for triple in triples:
        if triple[0].startswith('?') or triple[1].startswith('?') or triple[2].startswith('?'):
            for binding in var_bindings:
                new_triple = []
                for i in range(0, 3):
                    new_triple.append(triple[i])
                    if triple[i].startswith('?'):
                        var_name = triple[i][1:]
                        if var_name in binding:
                            new_triple[i] = binding[var_name]
                        else:
                            new_triple[i] = 'VAR'
                inflated_triples.append(new_triple)
    return inflated_triples


def slice_dbpedia(dbpedia_file, filter_result_file, query_triples):
    query_triples = set(query_triples.keys())
    with open(dbpedia_file, encoding='utf-8') as fin, open(filter_result_file, encoding='utf-8', mode='w') as fout:
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue
            if l.find('> a ') != -1:
                print (dbpedia_file + l)

            subj, pred, obj = parse_dbpedia_line(l)

            if ('VAR', pred, obj) in query_triples or (subj, pred, 'VAR') in query_triples or (subj, 'VAR', pred) in query_triples \
                or ('VAR', 'VAR', obj) in query_triples or (subj, 'VAR', 'VAR') in query_triples or ('VAR', pred, 'VAR') in query_triples:
                fout.write(l)


def parse_dbpedia_line(line):

    subj = line[0:line.find('>') + 1]
    line = line[line.find('>') + 1:].strip()
    pred = ''

    if line.startswith('a '):
        pred = 'a'
        line = line[2:].strip().strip('.').strip()
        print(line)
    else:
        pred = line[0:line.find('>') + 1]
        line = line[line.find('>') + 1:].strip().strip('.').strip()

    obj = line
    obj = unify_triple_item_format(obj)

    return subj, pred, obj




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

    '''
    for triple in all_triples:
        more_than_two_vars = False
        var_count = 0
        if triple[0] == 'VAR':
            var_count += 1
        if triple[1] == 'VAR':
            var_count += 1
        if triple[2] == 'VAR':
            var_count += 1
        if var_count >= 2:
            print(str(triple) + ':\t' + str(all_triples[triple]))
    '''


    all_triples[('VAR','VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>')] = set('exception')
    all_triples[('VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>', 'VAR')] = set('exception')

    all_triples_file = './data/QALD/all_triples.txt'
    with open(all_triples_file, encoding='utf-8', mode='w') as fout:
        for triple in all_triples:
            fout.write(str(triple))



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
        slice_dbpedia(dbpedia_file, filter_file, all_triples)
