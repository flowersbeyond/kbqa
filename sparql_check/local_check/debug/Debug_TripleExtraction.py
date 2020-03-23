import json
from sparql_check.local_check.Sparql_Utils import parse_triple_from_query_answer

RESULT_SAME = 'RESULT_SAME'
QUERY_PARSE_ERROR = 'QUERY_PARSE_ERROR'
LOCAL_ANS_EMPTY = 'LOCAL_ANSWER_EMPTY'
LOCAL_ANS_DECREASED = 'LOCAL_ANSWER_DECREASED'
LOCAL_ANS_INCREASED = 'LOCAL_ANS_INCREASED'
OTHERS = 'OTHERS'


def extract_id_query_answer(data_file):

    questions = {}
    with open(data_file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']

            query = question['query']['sparql']
            answer = question['answers']

            questions[id] = {'query':query, 'answer':answer}

    return questions

def get_error_ids(compare_detail_file, error_types):
    output = {}
    with open(compare_detail_file, encoding='utf-8') as fin:
        for l in fin:
            error = json.loads(l)
            if error['diff_type'] in error_types:
                output[error['id']] = {}
                for key in error:
                    if key != 'id':
                        output[error['id']][key] = error[key]

    return output


if __name__ == '__main__':
    test_data_file = './data/QALD/train-multilingual-4-9.jsonl'
    questions = extract_id_query_answer(test_data_file)
    compare_detail_file = './data/QALD/train-multilingual-4-9_local_result_compare_detail.jsonl'
    error_types = [LOCAL_ANS_EMPTY,LOCAL_ANS_INCREASED, OTHERS]
    target_ids = get_error_ids(compare_detail_file, error_types)

    debug_file = './data/QALD/train-multilingual-4-9_triple_debug.txt'
    with open(debug_file, encoding='utf-8', mode='w') as fout:
        for id in questions:
            if id in target_ids:
                triples = parse_triple_from_query_answer(questions[id]['query'], questions[id]['answer'])
                fout.write('id:\t%d\n' % id)
                fout.write('query:\t%s\n' % questions[id]['query'])
                fout.write('error:\t%s\n' % target_ids[id])
                #fout.write('triples:\n')
                #for triple in triples:
                #    fout.write('%s\n' % str(triple))
                fout.write('\n')

