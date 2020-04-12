import json

def parse_golden_core_chain(golden_chain):
    if len(golden_chain) >= 3:
        return ()
    chain = []
    for triple in golden_chain:
        chain.extend(triple)

    uniform_chain = []
    var_dict = {}
    for item in chain:
        if item.startswith('?'):
            if item not in var_dict:
                var_dict[item] = 0
            var_dict[item] += 1

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

def parse_topic_entity(chain):
    for triple in chain:
        if triple[0].startswith('<') and triple[0].endswith('>'):
            return triple[0]

        if triple[2].startswith('<') and triple[2].endswith('>'):
            return triple[2]

def get_all_topic_entities_from_qg_file(qg_file, category_type_entities):
    all_chains = {}
    with open(qg_file, encoding='utf-8') as fin:
        questions = json.load(fin)
        for id in questions:
            question = questions[id]
            if id == '115':
                print (id)
            chain_max_len = 0
            for chain in question['core_chain']:
                if isinstance(chain, dict):
                    if 'path' in chain:
                        chain = chain['path']
                chain_max_len = max(len(chain), chain_max_len)

            if chain_max_len >=3:
                continue
            if chain_max_len == 0:
                print(id)

            valid_chains = []
            category_type_chains = []
            for chain in question['core_chain']:
                if isinstance(chain,dict):
                    if 'path' in chain:
                        topic_entity = parse_topic_entity(chain['path'])
                        chain = parse_golden_core_chain(chain['path'])
                else:
                    topic_entity = parse_topic_entity(chain)
                    chain = parse_golden_core_chain(chain)
                assert topic_entity != None

                if topic_entity in category_type_entities:
                    category_type_chains.append({'topic_entity': topic_entity, 'core_chain': chain})
                else:
                    valid_chains.append({'topic_entity': topic_entity, 'core_chain': chain})

            if len(valid_chains) == 0:
                all_chains[int(id)] = category_type_chains
                print('%s:%d' %(id, chain_max_len))

            else:
                all_chains[int(id)] = valid_chains

    return all_chains


if __name__ == '__main__':


    dbfile_core_dir = 'D:/Research/kbqa/data/DBPedia/core/'
    category_type_entities_file = dbfile_core_dir + 'category_type/category_type_entities.txt'
    category_type_entities = set()

    with open(category_type_entities_file, encoding='utf-8') as fin:
        for l in fin:
            category_type_entities.add(l.strip())

    train_qg_file = './data/core_chain/cc_extraction/train-core_chain.json'
    train_chain_file = './data/core_chain/train_gold_chain.json'
    train_chains = get_all_topic_entities_from_qg_file(train_qg_file, category_type_entities)
    with open(train_chain_file, encoding='utf-8',mode='w') as fout:
        json.dump(train_chains, fout)

    test_qg_file = './data/core_chain/cc_extraction/test-core_chain.json'
    test_chain_file = './data/core_chain/test_gold_chain.json'
    test_chains = get_all_topic_entities_from_qg_file(test_qg_file, category_type_entities)
    with open(test_chain_file, encoding='utf-8', mode='w') as fout:
        json.dump(test_chains, fout)

    print('final_valid_train:%d'%len(train_chains))
    print('final_valid_test:%d'%len(test_chains))