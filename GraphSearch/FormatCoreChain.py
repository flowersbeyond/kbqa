import json

def parse_golden_core_chain(golden_chain):
    if len(golden_chain) >= 3:
        return ()
    chain = []
    for tuple in golden_chain:
        chain.append(tuple['subject'])
        chain.append(tuple['predicate'])
        chain.append(tuple['object'])

    uniform_chain = []
    var_dict = {}
    for item in chain:
        if item.startswith('?'):
            if item not in var_dict:
                var_dict[item] = 0
            var_dict[item] += 1

        if item.find('http') >= 0:
            item = '<' + item + '>'

        uniform_chain.append(item)

    var_name_dict = {}
    for var in var_dict:
        if var_dict[var] == 1:
            if len(uniform_chain) == 3:
                var_name_dict[var] = 'VAR'
            else:
                var_name_dict[var] = 'VAR2'
        else:
            var_name_dict[var] = 'VAR1'

    final_chain = []

    for item in uniform_chain:
        if item.startswith('?'):
            final_chain.append(var_name_dict[item])
        else:
            final_chain.append(item)

    return final_chain


def get_all_topic_entities_from_qg_file(qg_file, category_type_entities):
    all_chains = {}
    with open(qg_file, encoding='utf-8') as fin:
        questions = json.load(fin)
        for question in questions:
            id = question['id']

            if 'core_chain_list' not in question or question['core_chain_list'] == None:
                continue

            chain_max_len = 0
            for item in question['core_chain_list']:
                chain = item['chain']
                chain_max_len = max(len(chain), chain_max_len)

            if chain_max_len >=3:
                print(question['query']['sparql'])
                continue

            valid_chains = []
            for item in question['core_chain_list']:
                entity = '<' + item['topic_entity'] + '>'
                if entity in category_type_entities:
                    continue
                chain = item['chain']
                if len(chain) < chain_max_len:
                    continue
                chain = parse_golden_core_chain(chain)
                valid_chains.append({'topic_entity': entity, 'core_chain': chain})

            if len(valid_chains) != 0:
                all_chains[int(id)] = valid_chains

    return all_chains


if __name__ == '__main__':


    dbfile_core_dir = 'D:/Research/kbqa/data/DBPedia/core/'
    category_type_entities_file = dbfile_core_dir + 'category_type/category_type_entities.txt'
    category_type_entities = set()

    with open(category_type_entities_file, encoding='utf-8') as fin:
        for l in fin:
            category_type_entities.add(l.strip())

    train_qg_file = './data/core_chain/train_query_graph.json'
    train_chain_file = './data/core_chain/train_chain.json'
    train_chains = get_all_topic_entities_from_qg_file(train_qg_file, category_type_entities)
    with open(train_chain_file, encoding='utf-8',mode='w') as fout:
        json.dump(train_chains, fout)

    test_qg_file = './data/core_chain/test_query_graph.json'
    test_chain_file = './data/core_chain/test_chain.json'
    test_chains = get_all_topic_entities_from_qg_file(test_qg_file, category_type_entities)
    with open(test_chain_file, encoding='utf-8', mode='w') as fout:
        json.dump(test_chains, fout)

    print('final_valid_train:%d'%len(train_chains))
    print('final_valid_test:%d'%len(test_chains))