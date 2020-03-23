import json
import urllib.parse

RESULT_SAME = 'RESULT_SAME'
QUERY_PARSE_ERROR = 'QUERY_PARSE_ERROR'
LOCAL_ANS_EMPTY = 'LOCAL_ANSWER_EMPTY'
LOCAL_ANS_DECREASED = 'LOCAL_ANSWER_DECREASED'
LOCAL_ANS_INCREASED = 'LOCAL_ANS_INCREASED'
OTHERS = 'OTHERS'

def parse_jena_result(jena_result_file):
    #return {id: values[]}
    #{"id": 8, "answer": [{"uri": "http://dbpedia.org/resource/Yangtze"}]}
    local_results = {}
    with open(jena_result_file, encoding='utf-8') as fin:
        for l in fin:
            item = json.loads(l)
            id = item['id']
            result = item['result']
            if 'error' in result:
                local_results[id] = {'error': result['error']}
            else:
                values = parse_endpoint_result(result)
                local_results[id] = {'values': values}
    return local_results


def parse_official_result(official_result_file):
    #return {id: values[]}
    official_results = {}
    with open(official_result_file, encoding='utf-8') as fin:
        for l in fin:
            item = json.loads(l)
            id = item['id']
            result = item['answers']
            if 'error' in result:
                official_results[id] = {'error': result['error']}
            else:
                values = parse_endpoint_result(result)
                official_results[id] = {'values': values}
    return official_results

def parse_endpoint_result(result):
    values = []

    head = result['head']
    if 'vars' in head:
        vars = head['vars']
        if len(vars) > 1:
            print(str(id) + 'has more than 1 variables')
        bindings = result['results']['bindings']
        for binding in bindings:
            if len(binding) > 0:
                for key in binding:
                    value = binding[key]['value']
                    values.append(urllib.parse.unquote(str(value), encoding='utf-8').lower())
        return values
    else:
        values.append(str(result['boolean']).lower())
        return values

def parse_rdflib_result(local_result_file):
    # return {id: values[]}
    # {"id": 8, "answer": [{"uri": "http://dbpedia.org/resource/Yangtze"}]}
    local_results = {}
    with open(local_result_file, encoding='utf-8') as fin:
        for l in fin:
            if l.strip() == '':
                continue
            item = json.loads(l)
            id = item['id']
            result = item['result']
            if 'error' in result:
                local_results[id] = {'error': result['error']}
            else:
                values = parse_rdflib_result_inner(result)
                local_results[id] = {'values': values}
    return local_results

def parse_rdflib_result_inner(result):
    values = []
    if 'bool' in result:
        values.append(str(result['bool']).lower())
    else:
        for binding in result:
            for key in binding:
                value = binding[key]
                values.append(urllib.parse.unquote(str(value), encoding='utf-8').lower())

    return values

def compare_local_and_official_results(official_result_file, local_result_file, detail_file, summary_file, type):
    if type == 'jena':
        local_results = parse_jena_result(local_result_file)
    else:
        local_results = parse_rdflib_result(local_result_file)
    official_results = parse_official_result(official_result_file)

    # should follow format: {'id': id, 'diff_type': diff_type}
    compare_result = {}
    for id in local_results:
        left = local_results[id]
        right = official_results[id]

        if 'error' in left:
            compare_result[id] = {'diff_type': QUERY_PARSE_ERROR, 'error_msg': left['error']}
            continue

        left_values = set(left['values'])
        right_values = set(right['values'])
        left_only = left_values.difference(right_values)
        right_only = right_values.difference(left_values)
        if len(left_only) == 0 and len(right_only) == 0:
            # left & right are the same
            compare_result[id] = {'diff_type': RESULT_SAME}
        elif len(left_values) == 0:
            compare_result[id] = {'diff_type': LOCAL_ANS_EMPTY}
        elif len(left_only) == 0 and len(right_only) != 0:
            compare_result[id] = {'diff_type': LOCAL_ANS_DECREASED}
        elif len(left_only) != 0 and len(right_only) == 0:
            compare_result[id] = {'diff_type': LOCAL_ANS_INCREASED}
        else:
            compare_result[id] = {'diff_type': OTHERS}

        if len(left_only) != 0:
            compare_result[id]['local_only'] = list(left_only)
        if len(right_only) != 0:
            compare_result[id]['official_only'] = list(right_only)

    with open(detail_file, encoding='utf-8', mode='w') as fout:
        for id in compare_result:
            if compare_result[id]['diff_type'] != RESULT_SAME:
                compare_result[id]['id'] = id
                fout.write(json.dumps(compare_result[id]) + '\n')

    stats = {RESULT_SAME: 0, QUERY_PARSE_ERROR:0, LOCAL_ANS_EMPTY:0,
             LOCAL_ANS_DECREASED: 0,LOCAL_ANS_INCREASED: 0, OTHERS : 0}

    for id in compare_result:
        stats[compare_result[id]['diff_type']] += 1
    with open(summary_file, encoding='utf-8', mode='w') as fout:
        for diff_type in stats:
            fout.write('%s\t%d\n' % (diff_type, stats[diff_type]))
            print('%s\t%d\n' % (diff_type, stats[diff_type]))

    return


def read_compare_detail(compare_detail_file):
    details = {}
    with open(compare_detail_file, encoding='utf-8') as fin:
        for l in fin:
            detail = json.loads(l)
            id = detail['id']
            diff_type = detail['diff_type']
            details[id] = {'diff_type': diff_type}
            if diff_type == QUERY_PARSE_ERROR:
                details[id]['error_msg'] = detail['error_msg']
    return details


if __name__ == '__main__':
    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'

    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'


    modes = ['minicore', 'core', 'extend']
    types = ['jena', 'rdflib']
    for m in modes:
        for t in types:

            train_result = './data/QALD/train-multilingual-4-9_%s_%s_result.jsonl' % (t, m)
            train_local_result_compare_detail = './data/QALD/train-multilingual-4-9_%s_%s_compare_detail.jsonl' % (t, m)
            train_local_result_compare_summary = './data/QALD/train-multilingual-4-9_%s_%s_compare_summary.txt' % (t, m)


            test_result = './data/QALD/test-multilingual-4-9_%s_%s_result.jsonl' % (t, m)
            test_local_result_compare_detail = './data/QALD/test-multilingual-4-9_%s_%s_compare_detail.jsonl' % (t, m)
            test_local_result_compare_summary = './data/QALD/test-multilingual-4-9_%s_%s_compare_summary.txt' % (t, m)

            compare_local_and_official_results(qald_multilingual_train, train_result,
                                               train_local_result_compare_detail, train_local_result_compare_summary, type=t)
            compare_local_and_official_results(qald_multilingual_test, test_result,
                                               test_local_result_compare_detail, test_local_result_compare_summary, type=t)


    for m in modes:
        train_jena_detail_file = './data/QALD/train-multilingual-4-9_%s_%s_compare_detail.jsonl' % ('jena', m)
        train_rdflib_detail_file = './data/QALD/train-multilingual-4-9_%s_%s_compare_detail.jsonl' % ('rdflib', m)
        
        jena_detail = read_compare_detail(train_jena_detail_file)
        rdflib_detail = read_compare_detail(train_rdflib_detail_file)
        
        merge_detail = {}
        for id in jena_detail:
            if id in rdflib_detail:
                jena_diff_type = jena_detail[id]['diff_type']
                rdf_diff_type = rdflib_detail[id]['diff_type']
                if jena_diff_type == rdf_diff_type:
                    merge_detail[id] = jena_detail[id]
                else:
                    merge_detail[id] = {'jena':jena_detail[id], 'rdflib':rdflib_detail[id]}
        
        train_merge_detail_file = './data/QALD/train-multilingual-4-9_%s_%s_compare_detail.jsonl' % ('merge', m)
        with open(train_merge_detail_file, encoding='utf-8', mode= 'w') as fout:
            for id in merge_detail:
                merge_detail[id]['id'] = id
                fout.write(json.dumps(merge_detail[id]) + '\n')

        test_jena_detail_file = './data/QALD/test-multilingual-4-9_%s_%s_compare_detail.jsonl' % ('jena', m)
        test_rdflib_detail_file = './data/QALD/test-multilingual-4-9_%s_%s_compare_detail.jsonl' % (
        'rdflib', m)

        jena_detail = read_compare_detail(test_jena_detail_file)
        rdflib_detail = read_compare_detail(test_rdflib_detail_file)

        merge_detail = {}
        for id in jena_detail:
            if id in rdflib_detail:
                jena_diff_type = jena_detail[id]['diff_type']
                rdf_diff_type = rdflib_detail[id]['diff_type']
                if jena_diff_type == rdf_diff_type:
                    merge_detail[id] = jena_detail[id]
                else:
                    merge_detail[id] = {'jena': jena_detail[id], 'rdflib': rdflib_detail[id]}

        test_merge_detail_file = './data/QALD/test-multilingual-4-9_%s_%s_compare_detail.jsonl' % ('merge', m)
        with open(test_merge_detail_file, encoding='utf-8', mode='w') as fout:
            for id in merge_detail:
                merge_detail[id]['id'] = id
                fout.write(json.dumps(merge_detail[id]) + '\n')

