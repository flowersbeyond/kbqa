import re
import urllib.parse
from utils.Rdf_Utils import unify_triple_item_format, public_prefix

def parse_triple_from_query_answer(query, answers):
    raw_triples = extract_triples(query)
    var_bindings = parse_var_bindings(answers)
    triples = inflate_bindings(raw_triples, var_bindings)

    return triples


def extract_prefixes(query):

    tokens = tokenize(query)
    prefixes = {}

    pos = 0
    while pos < len(tokens):
        if tokens[pos].lower() == 'prefix':
            name = tokens[pos + 1][0:-1]
            value = tokens[pos + 2]
            prefixes[name] = value
            pos = pos + 3
        else:
            pos += 1

    return prefixes


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

            if item == 'a':
                item = 'rdf:type'
            if item.endswith('.'):
                if item.find(':') != -1 or item.startswith('?'):
                    item = item.strip('.')
                    #print('triple_item_ending_with_dot:%s' + str(triple))
                #else:
                #    print(query + ':\t' + item)

            replaced_item = item
            if item.find(':') != -1 and not item.startswith('<') and not item.startswith('\"') and not item.startswith('\''):
                pair = item.split(':')
                if pair[0] not in prefixes:
                    replaced_item = public_prefix[pair[0]][0:-1] + pair[1] + '>'
                    #if item != 'rdf:type':
                    #    print(query)
                else:
                    replaced_item = prefixes[pair[0]][0:-1] + pair[1] + '>'

            triple[i] = unify_triple_item_format(replaced_item)

    return triples


def tokenize(query):
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
        elif query[pos] == '\'':
            token = query[pos]
            pos += 1
            while query[pos] != '\'':
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
            while pos < len(query) and query[pos] not in ['{', '}', '<', '>', '(', ')', ';', ' ']:
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
                    if value.find('http') != -1:
                        value = '<' + value + '>'
                    value = unify_triple_item_format(value)
                    parse_binding[key] = value
                parse_bindings.append(parse_binding)

    return parse_bindings


def inflate_bindings(triples, var_bindings):
    inflated_triples = []
    for triple in triples:
        if triple[0].startswith('?') or triple[1].startswith('?') or triple[2].startswith('?'):
            if len(var_bindings) != 0:
                for binding in var_bindings:
                    new_triple = [triple[0], triple[1], triple[2]]
                    for i in range(0, 3):
                        if triple[i].startswith('?'):
                            var_name = triple[i][1:]
                            if var_name in binding:
                                new_triple[i] = binding[var_name]
                            else:
                                new_triple[i] = 'VAR'
                    inflated_triples.append(new_triple)
            else:
                #print(triple)
                inflated_triples.append(triple)
        else:
            #print(triple)
            inflated_triples.append(triple)
    return inflated_triples