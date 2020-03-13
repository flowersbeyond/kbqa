import json
import re
def extract_id_query(file):

    queries = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']
            query = question['query']['sparql']
            query = query.replace('\n', ' ')
            query = re.sub(r"\s+", " ", query)
            query = query.strip()
            queries[id] = query
    return queries

def extract_rdf_ids(queries):
    all_rdf_ids = set()
    for id in queries:
        query = queries[id]

        [prefixs, query_graph] = query.split('WHERE')
        prefixs = prefixs.split()
        pre_dict = {}
        for i in range(1, len(prefixs)):
            if prefixs[i][0] == '<':
                if prefixs[i - 1][-1] == ':':
                    pre_dict[prefixs[i - 1].split(':')[0]] = prefixs[i]
        final_list = []
        triple_list = re.findall(r'[<](.*?)[>]', query_graph)
        if len(triple_list) != 0:
            for uri in triple_list:
                final_list.append("<{}>".format(uri))
        for per_string in query_graph.split():
            if ":" in per_string and ("<" not in per_string and ">" not in per_string):
                for pre in pre_dict.keys():
                    if pre + ":" in per_string:
                        if per_string[-1] == '.':
                            per_string = per_string[0:-1]
                        final_list.append(pre_dict[pre][0:-1] + per_string.split(":")[-1] + ">")
        all_rdf_ids.update(final_list)

    return all_rdf_ids





if __name__ == '__main__':

    all_rdf_ids = set()

    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'

    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'

    train_id_queries = extract_id_query(qald_multilingual_train)
    all_rdf_ids_train = extract_rdf_ids(train_id_queries)

    test_id_queries = extract_id_query(qald_multilingual_test)
    all_rdf_ids_test = extract_rdf_ids(test_id_queries)

    all_rdf_ids = all_rdf_ids_train.union(all_rdf_ids_test)

    all_rdf_ids_file = './data/QALD/all_rdf_ids.txt'
    with open(all_rdf_ids_file, encoding='utf-8', mode='w') as fout:
        for id in all_rdf_ids:
            fout.write(id.strip() + '\n')


    query_set = set()

    for id in train_id_queries:
        query_set.add(train_id_queries[id])
    for id in test_id_queries:
        query_set.add(test_id_queries[id])

    all_queries_file = './data/QALD/all_queries.txt'
    with open(all_queries_file, encoding='utf-8', mode='w') as fout:
        for q in query_set:
            fout.write(q + '\n')







