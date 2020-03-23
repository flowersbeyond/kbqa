import json


def merge_debug_info(data_file, local_exe_compare_detail, query_format_dbg_file):
    id_query_map = {}
    with open(data_file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']
            query = question['query']['sparql']
            id_query_map[id] = query

    ill_format_queries = []

    with open(local_exe_compare_detail, encoding='utf-8') as fin:
        for l in fin:
            #{"diff_type": "QUERY_PARSE_ERROR", "error_msg": "Expected {SelectQuery | ConstructQuery | DescribeQuery | AskQuery} (at char 7), (line:1, col:8)", "id": 7}
            detail = json.loads(l)
            if detail['diff_type'] == 'QUERY_PARSE_ERROR':
                id = detail['id']
                query = id_query_map[id]

                ill_format_queries.append({'id':id, 'query':query, 'error_msg': detail['error_msg']})


    with open(query_format_dbg_file, encoding='utf-8',mode='w') as fout:
        for item in ill_format_queries:
                fout.write(json.dumps(item) + '\n')


if __name__ == '__main__':

    train_data_file = './data/QALD/train-multilingual-4-9.jsonl'
    train_local_exe_compare_detail = './data/QALD/train-multilingual-4-9_local_result_compare_detail.jsonl'
    train_query_format_dbg_file = './data/QALD/train-multilingual-4-9_local_result_query_grammar_dbg.jsonl'


    test_data_file = './data/QALD/test-multilingual-4-9.jsonl'
    test_local_exe_compare_detail = './data/QALD/test-multilingual-4-9_local_result_compare_detail.jsonl'
    test_query_format_dbg_file = './data/QALD/test-multilingual-4-9_local_result_query_grammar_dbg.jsonl'

    merge_debug_info(train_data_file, train_local_exe_compare_detail, train_query_format_dbg_file)
    merge_debug_info(test_data_file, test_local_exe_compare_detail, test_query_format_dbg_file)

