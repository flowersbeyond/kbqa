import json
import random
from utils.DBPedia_Utils import parse_dbpedia_line
import math


def prepare_train_test_data(question_file, candidate_core_chains_file, question_language, output, label_dict, id_list=None, zero_f1_sample_rate = 1.0):
    questions = {}

    with open(question_file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']
            english_text = ''
            question_list = question['question']
            for q in question_list:
                if q['language'] == question_language:
                    english_text = q['string']
                    break

            if english_text != '':
                questions[id]= english_text

    candidate_core_chains = {}
    with open(candidate_core_chains_file, encoding='utf-8') as fin:
        for l in fin:
            item = json.loads(l)
            id = item['id']
            candidates = item['candidates']
            candidate_core_chains[id] = candidates

    with open(output, encoding='utf-8', mode='w') as fout:
        for id in candidate_core_chains:
            if id_list == None or id in id_list:
                english_text = questions[id]
                candidates = candidate_core_chains[id]
                for candidate in candidates:
                    chain = candidate['core_chain']
                    chain_str = format_chain_to_str(chain, label_dict)
                    f1 = candidate['max_f1']
                    is_gold = candidate['is_gold']

                    sample = True
                    if f1 <= 0.00001:
                        if random.random() > zero_f1_sample_rate:
                            sample = False
                    if sample:
                        fout.write(json.dumps({'id':id, 'question':english_text,
                                               'chain_str': chain_str,
                                               'f1': f1, 'is_gold': is_gold}) + '\n')

def format_chain_to_str(chain, label_dict):
    str = ''
    for i in range(0, len(chain)):
        item = chain[i]
        if item == 'VAR':
            item = 'variable'
        elif item in ['VAR1', 'VAR2']:
            item = item.replace('VAR', 'variable ')
        elif item.startswith('<') and item.endswith('>'):
            if item in label_dict:
                item = label_dict[item]

            else:
                item = item[1:-1]
                item = item[item.rfind('/')+1:]
                item = item[item.rfind(':')+1:]
                item = item[item.rfind('#')+1:]
                item = item[item.rfind('\\')+1:]
        if i == 3:
            str = str + ', '
        str = str + item + ' '

    return str.strip()


def load_label_dict(label_dbpedia_file):
    label_dict = {}
    with open(label_dbpedia_file, encoding='utf-8') as fin:
        for l in fin:
            subj, pred, obj = parse_dbpedia_line(l)
            label_dict[subj] = obj
    return label_dict

def split_train_dev_set(candidate_core_chains_file, train_id_list_file, dev_id_list_file, split_ratio = 0.15):
    full_id_list = set()
    with open(candidate_core_chains_file, encoding='utf-8') as fin:
        for l in fin:
            item = json.loads(l)
            id = item['id']
            full_id_list.add(id)

    dev_id_list = []
    train_id_list = []

    for id in full_id_list:
        if random.random() < split_ratio:
            dev_id_list.append(id)
        else:
            train_id_list.append(id)

    with open(train_id_list_file, encoding='utf-8', mode='w') as fout:
        json.dump(train_id_list, fout)

    with open(dev_id_list_file, encoding='utf-8', mode='w') as fout:
        json.dump(dev_id_list, fout)

    return train_id_list, dev_id_list


def load_idlist_from_file(idlist_file):
    idlist = []
    with open(idlist_file, encoding='utf-8') as fin:
        idlist = json.load(fin)

    return idlist


#configs:
ZERO_F1_SAMPLE = 0.1

if __name__ == '__main__':

    #label_dbpedia_file = 'D:/Research/kbqa/data/DBPedia/core/entity_label/labels_en.ttl'
    #label_dict = load_label_dict(label_dbpedia_file)
    label_dict_file = 'D:/Research/kbqa/data/DBPedia/core/entity_label/labels_en_dict.json'
    #with open(label_dict_file, encoding='utf-8', mode='w') as fout:
    #    json.dump(label_dict, fout)

    label_dict = {}
    with open(label_dict_file, encoding='utf-8') as fin:
        label_dict = json.load(fin)

    train_question_file = './data/QALD/train-multilingual-4-9.jsonl'
    train_candidate_core_chains_file = './data/core_chain/train_candidate_core_chains.jsonl'
    train_data_file = './data/core_chain/train_data%s.jsonl' % ('_05' if ZERO_F1_SAMPLE == 0.05 else '_10')
    dev_data_file = './data/core_chain/dev_data%s.jsonl' % ('_05' if ZERO_F1_SAMPLE == 0.05 else '_10')

    train_id_list_file = './data/core_chain/train_idlist.json'
    dev_id_list_file = './data/core_chain/dev_idlist.json'

    #train_id_list, dev_id_list = split_train_dev_set(train_candidate_core_chains_file, train_id_list_file, dev_id_list_file, split_ratio = 0.15)
    train_id_list = load_idlist_from_file(train_id_list_file)
    dev_id_list = load_idlist_from_file(dev_id_list_file)

    prepare_train_test_data(train_question_file, train_candidate_core_chains_file, 'en', train_data_file,
                            label_dict, id_list = train_id_list, zero_f1_sample_rate= ZERO_F1_SAMPLE)

    prepare_train_test_data(train_question_file, train_candidate_core_chains_file, 'en', dev_data_file,
                            label_dict, id_list=dev_id_list)

    test_question_file = './data/QALD/test-multilingual-4-9.jsonl'
    test_candidate_core_chains_file = './data/core_chain/test_candidate_core_chains.jsonl'
    test_data_file = './data/core_chain/test_data%s.jsonl' %('_05' if ZERO_F1_SAMPLE == 0.05 else '_10')
    prepare_train_test_data(test_question_file, test_candidate_core_chains_file, 'en', test_data_file,
                            label_dict)
