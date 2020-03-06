import json
import re

import xmltodict
from rdflib.plugins.sparql import prepareQuery
'''
A Standard qald8 field format:
{
    "id" : "1",
    "answertype" : "resource",
    "aggregation" : false,
    "onlydbo" : false,
    "hybrid" : false,
    "question" : [ {
      "language" : "en",
      "string" : "What is the alma mater of the chancellor of Germany Angela Merkel?",
      "keywords" : "Angela Merkel"
    } ],
    "query" : {
      "sparql" : "PREFIX dbo: <http://dbpedia.org/ontology/> PREFIX dbr: <http://dbpedia.org/resource/> SELECT ?Almamater WHERE { dbr:Angela_Merkel dbo:almaMater ?Almamater }"
    },
    "answers" 
        : [ {
      "head" : {
        "vars" : [ "uri" ]
      },
      "results" : {
        "bindings" : [ {
          "uri" : {
            "type" : "uri",
            "value" : "http://dbpedia.org/resource/Leipzig_University"
          }
        } ]
      }
    } ]
  },
'''

def bool_unify(a):
    if isinstance(a,str):
        if a.lower() == 'true':
            return True
        else:
            return False
    elif isinstance(a, bool):
        return a
    else:
        assert False


def parse_and_filter_qald4(data_file, exe_res_file):
    results = load_exe_res(exe_res_file)

    transformed_questions = []
    with open(data_file, encoding='utf-8') as fd:
        data = xmltodict.parse(fd.read())
        data = data['dataset']['question']
        for q in data:
            trans_q = {}
            trans_q['id'] = str(q['@id'])
            if trans_q['id'] not in results:
                continue

            trans_q['answertype'] = q['@answertype']
            trans_q['aggregation'] = bool_unify(q['@aggregation'])
            if '@onlydbo' not in q:
                #print(trans_q['id'] + 'does not have onlydbo property')
                continue
            trans_q['onlydbo'] = bool_unify(q['@onlydbo'])
            trans_q['hybrid'] = False


            q_details = {}
            for item in q['string']:
                q_details[item['@lang']] = {'string': item['#text']}
            for item in q['keywords']:
                q_details[item['@lang']]['keywords'] = item['#text']
            trans_q['question'] = q_details

            trans_q['query'] = {'sparql': q['query']}

            trans_q['answers'] = results[trans_q['id']]
            trans_q['src'] = 4

            transformed_questions.append(trans_q)

    return transformed_questions

def parse_and_filter_qald5(data_file, exe_res_file):
    results = load_exe_res(exe_res_file)

    transformed_questions = []
    with open(data_file, encoding='utf-8') as fd:
        data = xmltodict.parse(fd.read())
        data = data['dataset']['question']
        for q in data:
            trans_q = {}
            trans_q['id'] = str(q['@id'])
            if trans_q['id'] not in results:
                continue

            trans_q['answertype'] = q['@answertype']
            trans_q['aggregation'] = bool_unify(q['@aggregation'])
            trans_q['onlydbo'] = bool_unify(q['@onlydbo'])
            trans_q['hybrid'] = False

            q_details = {}

            for item in q['string']:
                if '#text' not in item:
                    #print(trans_q['id'] + 'does not have text in question for language: '+ item['@lang'])
                    continue
                q_details[item['@lang']] = {'string': item['#text']}
            for item in q['keywords']:
                if item['@lang'] not in q_details: # their is not question text for the corrresponding language
                    continue
                if '#text' not in item:
                    #print(trans_q['id'] + 'does not have text in keyword for language: '+ item['@lang'])
                    item['#text'] = ''
                q_details[item['@lang']]['keywords'] = item['#text']

            trans_q['question'] = q_details

            trans_q['query'] = {'sparql': q['query']}

            trans_q['answers'] = results[trans_q['id']]
            trans_q['src'] = 5

            transformed_questions.append(trans_q)

    return transformed_questions

def parse_and_filter_qald6plus(data_file, exe_res_file, src_id):
    results = load_exe_res(exe_res_file)
    transformed_questions = []

    with open(data_file, encoding='utf-8') as fd:
        data = json.load(fd)
        data = data['questions']
        for q in data:
            trans_q = {}
            trans_q['id'] = str(q['id'])
            if trans_q['id'] not in results:
                continue

            trans_q['answertype'] = q['answertype']
            trans_q['aggregation'] = bool_unify(q['aggregation'])
            trans_q['onlydbo'] = bool_unify(q['onlydbo'])
            trans_q['hybrid'] = bool_unify(q['hybrid'])
            if trans_q['hybrid']:
                print('hybridTrue')

            q_details = {}
            for item in q['question']:
                if 'string' not in item and 'keywords' not in item:
                    continue

                if 'string' not in item:
                    #print(trans_q['id'] + 'does not have text in question for language: ' + item['language'])
                    item['string'] = ''

                if 'string' in item and 'keywords' not in item:
                    #print(trans_q['id'] + 'does not have text in keyword for language: ' + item['language'])
                    item['keywords'] = ''

                q_details[item['language']] = {'string':item['string'], 'keywords': item['keywords']}
            trans_q['question'] = q_details

            trans_q['query'] = {'sparql': q['query']['sparql']}

            trans_q['answers'] = results[trans_q['id']]
            trans_q['src'] = src_id
            transformed_questions.append(trans_q)

    return transformed_questions

def dedup(questions):

    unique = {i:True for i in range(0,len(questions))}
    for i in range(0, len(questions)):
        q0 = questions[i]
        q0['merge'] = '%s#%s' % (q0['src'], q0['id'])
        for j in range(0, i):
            if not unique[j]:
                continue

            q1 = questions[j]
            #compare the question text. if text is the same,then it is the same question.
            q0_q_details = q0['question']
            q1_q_details = q1['question']

            q_text_overlap = False
            for lang in q0_q_details:
                if lang in q1_q_details:
                    q0_q_text = q0_q_details[lang]['string']
                    q1_q_text = q1_q_details[lang]['string']
                    if q0_q_text != '' and q1_q_text != '' and q0_q_text.strip().lower() == q1_q_text.strip().lower():
                        q_text_overlap = True
                        break

            if q_text_overlap:

                # 1. compare if all overlapped language share the same q_text
                compatible_q = True
                q0_lang_set = set(q0_q_details.keys())
                q1_lang_set = set(q1_q_details.keys())
                incompatible_lang = ''

                for lang in q0_lang_set.intersection(q1_lang_set):
                    q0_q_text = q0_q_details[lang]['string']
                    q1_q_text = q1_q_details[lang]['string']
                    if q0_q_text != '' and q1_q_text != '' and q0_q_text.strip().lower() != q1_q_text.strip().lower():
                        incompatible_lang = lang
                        compatible_q = False
                        break

                if not compatible_q:
                    print('Ovelap question with different text on other languages:\n %s#%s:\t%s\n%s#%s:\t%s'
                          % (q0['src'], q0['id'], q0_q_details[incompatible_lang]['string'], q1['src'], q1['id'], q1_q_details[incompatible_lang]['string']))
                    continue

                # 2. compare if the sparql query are the same:
                query0 = q0['query']['sparql']
                query1 = q1['query']['sparql']
                query0 = query0.replace('\n', ' ').replace('. ', ' ')
                query1 = query1.replace('\n', ' ').replace('. ', ' ')
                query0 = re.sub(r"\s+", " ", query0)
                query1 = re.sub(r"\s+", " ", query1)
                unify_query0 = re.sub(r"\s+", "", query0).lower()
                unify_query1 = re.sub(r"\s+", "", query1).lower()


                if unify_query0 != '' and unify_query1 != '' and unify_query0 != unify_query1:  # .strip().lower() != query1.strip().lower():
                    # parse0 = prepareQuery(query0)
                    # parse1 = prepareQuery(query1)

                    # print('Overlap question, different query:')
                    # print('\n %s#%s:\t%s\n %s#%s:\t%s\n'
                    #      % (q0['src'], q0['id'], query0, q1['src'], q1['id'], query1))
                    continue
                '''
                if unify_query0 != unify_query1:
                    q0['query']['sparql'] = q0['query']['sparql'] if q0['src'] > q1['src'] else q1['query']['sparql']

                

                '''
                unique[i] = False

                merge_flag = False

                if q0['answertype'] != q1['answertype']:
                    #print('Compatible question & query, different ANSWERTYPE: %s#%s:%s, %s#%s:%s'
                    #    % (q0['src'], q0['id'], q0['answertype'], q1['src'], q1['id'], q1['answertype']))
                    merge_flag = True
                    q0['answertype'] = q0['answertype'] if q0['src'] > q1['src'] else q1['answertype']

                if q0['aggregation'] != q1['aggregation']:
                    #print('Compatible question & query, different AGGREGATION: %s#%s:%s, %s#%s:%s'
                    #      % (q0['src'], q0['id'], q0['aggregation'], q1['src'], q1['id'], q1['aggregation']))
                    merge_flag = True
                    q0['aggregation'] = q0['aggregation'] if q0['src'] > q1['src'] else q1['aggregation']

                if q0['onlydbo'] != q1['onlydbo']:
                    #print('Compatible question & query, different ONLYDBO: %s#%s:%s, %s#%s:%s'
                    #      % (q0['src'], q0['id'], q0['onlydbo'], q1['src'], q1['id'], q1['onlydbo']))
                    merge_flag = True
                    q0['onlydbo'] = q0['onlydbo'] if q0['src'] > q1['src'] else q1['onlydbo']

                if q0['hybrid'] != q1['hybrid']:
                    #print('Compatible question & query, different HYBRID: %s#%s:%s, %s#%s:%s'
                    #      % (q0['src'], q0['id'], q0['hybrid'], q1['src'], q1['id'], q1['hybrid']))
                    merge_flag = True
                    q0['hybrid'] = q0['hybrid'] if q0['src'] > q1['src'] else q1['hybrid']


                for lang in q1_lang_set:
                    if lang not in q0_lang_set:
                        q0_q_details[lang]= {'string':q1_q_details[lang]['string'], 'keywords':q1_q_details[lang]['keywords']}
                        merge_flag = True
                    else:
                        if q0_q_details[lang]['string'] == '' and q1_q_details[lang]['string'] != '':
                            q0_q_details[lang]['string'] = q1_q_details[lang]['string']
                            merge_flag= True
                        if len(q0_q_details[lang]['keywords']) < len(q1_q_details[lang]['keywords']):
                            q0_q_details[lang]['keywords'] = q1_q_details[lang]['keywords']
                            merge_flag = True


                if merge_flag:
                    q1['merge'] += '_M%s#%s' % (q0['src'], q0['id'])
                else:
                    q1['merge'] += '_D%s#%s' % (q0['src'], q0['id'])


    dedup_questions = []
    for i in range(0, len(questions)):
        if unique[i]:
            dedup_questions.append(questions[i])

    print('Question count: before dedup %d, after dedup %d' % (len(questions), len(dedup_questions)))

    return dedup_questions

def load_exe_res(exe_res_file):
    results = {}
    with open(exe_res_file, encoding='utf-8') as fin:
        for l in fin:
            res = json.loads(l)
            result =res['result']
            if "error" in result:
                continue

            head = result['head']
            values = []
            if 'vars' in head:
                bindings = result['results']['bindings']

                for binding in bindings:
                    if len(binding) > 0:
                        for key in binding:
                            value = binding[key]['value']
                        values.append(value)
            else:
                values.append(str(result['boolean']).lower())
            if len(values) > 0:
                results[str(res['id'])] = res['result'] #Non Empty
    #print("%s: Valid: %d" % (exe_res_file, len(results)))
    return results

def format_q_details(questions):
    for i in range(0, len(questions)):
        q = questions[i]
        q['id'] = i
        q_details = q['question']
        q['question'] = [{'language':lang,
                          'string':q_details[lang]['string'],
                          'keywords':q_details[lang]['keywords']}
                         for lang in q_details]


def save_to_file(questions, filename):
    with open(filename, encoding='utf-8', mode='w') as f:
        for q in questions:
            f.write(json.dumps(q) + '\n')



if __name__ == '__main__':

    data_dir = './data/QALD/4/data/'
    qald4_train = data_dir + 'qald-4_multilingual_train_withanswers.xml'
    qald4_train_exe_result = data_dir + 'qald-4_multilingual_train_db1610ans.json'
    qald4_test = data_dir + 'qald-4_multilingual_test_withanswers.xml'
    qald4_test_exe_result = data_dir + 'qald-4_multilingual_test_db1610ans.json'

    data_dir = './data/QALD/5/data/'
    qald5_train = data_dir + 'qald-5_train.xml'
    qald5_train_exe_result = data_dir + 'qald-5_train_db1610ans.json'
    qald5_test = data_dir + 'qald-5_test.xml'
    qald5_test_exe_result = data_dir + 'qald-5_test_db1610ans.json'

    qald6plus_train = []
    qald6plus_train_exe_result = []
    qald6plus_test = []
    qald6plus_test_exe_result = []

    for i in [6,7,8,9]:
        data_dir = './data/QALD/%d/data/' % i
        qald6plus_train.append(data_dir + '/qald-%d-train-multilingual.json' % i)
        qald6plus_train_exe_result.append(data_dir + '/qald-%d-train-multilingual_db1610ans.json' % i)
        qald6plus_test.append(data_dir + '/qald-%d-test-multilingual.json' % i)
        qald6plus_test_exe_result.append(data_dir + '/qald-%d-test-multilingual_db1610ans.json' % i)

    final_train = './data/QALD/train-multilingual-4-9.jsonl'
    final_test = './data/QALD/test-multilingual-4-9.jsonl'


    qald = parse_and_filter_qald4(qald4_train, qald4_train_exe_result)
    qald.extend(parse_and_filter_qald5(qald5_train, qald5_train_exe_result))

    for i in range(0,4):
        train = qald6plus_train[i]
        exe_result = qald6plus_train_exe_result[i]
        qaldi = parse_and_filter_qald6plus(train, exe_result, i + 6)
        qald.extend(qaldi)

    total_questions = dedup(qald)
    format_q_details(total_questions)
    save_to_file(total_questions, final_train)

    qald = parse_and_filter_qald4(qald4_test, qald4_test_exe_result)
    qald.extend(parse_and_filter_qald5(qald5_test, qald5_test_exe_result))

    for i in range(0,4):
        test = qald6plus_test[i]
        exe_result = qald6plus_test_exe_result[i]
        qaldi = parse_and_filter_qald6plus(test, exe_result, i + 6)
        qald.extend(qaldi)

    total_questions = dedup(qald)
    format_q_details(total_questions)
    save_to_file(total_questions, final_test)