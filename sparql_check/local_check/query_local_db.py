from rdflib.graph import Graph
import json


def load_all_core_data(data_dir, core_names):

    g = Graph()

    for name in core_names:
        print(name + '\t loading...')
        g.parse('%s/%s.ttl'%(data_dir, name), format='turtle')
        print(name + '\tfinished loading.')

    return g


def interpret_result(qres):
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


def extract_id_query(file):

    questions = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']
            query = question['query']['sparql']
            questions[id] = query
    return questions


if __name__ == '__main__':

    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'

    train_result = './data/QALD/train-multilingual-4-9_local_result.jsonl'
    test_result = './data/QALD/test-multilingual-4-9_local_result.jsonl'

    train_queries = extract_id_query(qald_multilingual_train)
    test_queries = extract_id_query(qald_multilingual_test)


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

    g = load_all_core_data(dbpedia_data_dir, core_names)

    with open(train_result, encoding='utf-8', mode='w') as fout:

        for id in train_queries:
            try:
                qres = g.query(train_queries[id])
                answers = interpret_result(qres)
                fout.write(json.dumps({'id':id, 'answer':answers}))
                fout.write('\n')

            except Exception as e:
                fout.write(json.dumps({'id':id, 'error':str(e)}))
                fout.write('\n')

    with open(test_result, encoding='utf-8', mode='w') as fout:

        for id in test_queries:
            try:
                qres = g.query(test_queries[id])
                answers = interpret_result(qres)
                fout.write(json.dumps({'id':id, 'answer':answers}))
                fout.write('\n')

            except Exception as e:
                fout.write(json.dumps({'id':id, 'error':str(e)}))
                fout.write('\n')

    while (True):
        query = input('query:')

        if query.startswith('Q:'):
            query = query[2:]
            print('begin query...')
            try:
                qres = g.query(query)
                if qres.bindings != None:
                    for binding in qres.bindings:
                        for var in qres.vars:
                            print('%s:\t%s\n' % (str(var), str(binding[var])))

                elif qres.askAnswer != None:
                    print(qres.askAnswer)

            except Exception as e:
                print(str(e))

            print('end query...')
        elif query == 'exit':
            break
