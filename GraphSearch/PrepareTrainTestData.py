import json
import random
from utils.DBPedia_Utils import parse_dbpedia_line


def prepare_train_test_data(question_file, candidate_core_chains_file, question_language, output, label_dict, id_list=None, zero_f1_sample_rate = 1.0):
    questions = {}
    questions_raw = []
    with open(question_file, encoding='utf-8') as fin:
        questions_raw = json.load(fin)
        for question in questions_raw:
            id = question['id']
            question_text = ''
            question_list = question['question']
            for q in question_list:
                if q['language'].lower() == question_language:
                    question_text = q['string']
                    break

            if question_text != '':
                questions[id]= question_text

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
                question_text = questions[id]
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
                        fout.write(json.dumps({'id':id, 'question':question_text,
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



if __name__ == '__main__':

    #label_dbpedia_file = 'D:/Research/kbqa/data/DBPedia/core/entity_label/labels_en.ttl'
    #label_dict = load_label_dict(label_dbpedia_file)
    label_dict_file = 'D:/Research/kbqa/data/DBPedia/core/entity_label/labels_en_dict.json'
    #with open(label_dict_file, encoding='utf-8', mode='w') as fout:
    #    json.dump(label_dict, fout)
    label_dict = {}
    with open(label_dict_file, encoding='utf-8') as fin:
        label_dict = json.load(fin)


    training_data_dir = './data/core_chain/trainingdata/'

    train_id_list_file = training_data_dir + '/train_idlist.json'
    dev_id_list_file = training_data_dir + '/dev_idlist.json'
    train_id_list = load_idlist_from_file(train_id_list_file)
    dev_id_list = load_idlist_from_file(dev_id_list_file)


    train_question_file = './data/QALD/train-multilingual-merged.json'
    test_question_file = './data/QALD/test-multilingual-merged.json'


    train_label_en_cc_file = './data/core_chain/candidates/train_label_en.jsonl'
    train_zeroshot_pred_en_cc_file = './data/core_chain/candidates/train_zeroshot_pred_en.jsonl'

    test_label_en_cc_file = './data/core_chain/candidates/test_label_en.jsonl'
    test_zeroshot_pred_en_cc_file = './data/core_chain/candidates/test_zeroshot_pred_en.jsonl'
    test_zeroshot_pred_ru_cc_file = './data/core_chain/candidates/test_zeroshot_pred_ru.jsonl'
    test_zeroshot_pred_de_cc_file = './data/core_chain/candidates/test_zeroshot_pred_de.jsonl'

    configs = ['label', 'zeroshot_pred']
    sample_rates = [0.1, 0.2]


    for sample_rate in sample_rates:
        label_dir = training_data_dir + str(int(sample_rate * 100))+ 'label/'
        zeroshot_dir = training_data_dir + str(int(sample_rate * 100)) + 'zeroshot/'
        '''
        train_label_en_data = label_dir + 'train_data.jsonl'
        dev_label_en_data = label_dir + 'dev_data.jsonl'
        prepare_train_test_data(train_question_file, train_label_en_cc_file, 'en', train_label_en_data,
                                label_dict, id_list=train_id_list, zero_f1_sample_rate=sample_rate)

        prepare_train_test_data(train_question_file, train_label_en_cc_file, 'en', dev_label_en_data,
                                label_dict, id_list=dev_id_list)

        
        train_zeroshot_pred_en_data = zeroshot_dir + '/train_data.jsonl'
        dev_zeroshot_pred_en_data = zeroshot_dir + '/dev_data.jsonl'
        prepare_train_test_data(train_question_file, train_zeroshot_pred_en_cc_file, 'en', train_zeroshot_pred_en_data,
                                label_dict, id_list=train_id_list, zero_f1_sample_rate=sample_rate)

        prepare_train_test_data(train_question_file, train_zeroshot_pred_en_cc_file, 'en', dev_zeroshot_pred_en_data,
                                label_dict, id_list=dev_id_list)
        '''
        target_dirs = [label_dir, zeroshot_dir]
        for target_dir in target_dirs:
            '''
            test_label_en_data = target_dir + '/test_data_label.jsonl'
            prepare_train_test_data(test_question_file, test_label_en_cc_file, 'en', test_label_en_data,
                                    label_dict)
            test_zeroshot_pred_en_data = target_dir + '/test_data_en.jsonl'
            prepare_train_test_data(test_question_file, test_zeroshot_pred_en_cc_file, 'en', test_zeroshot_pred_en_data,
                                    label_dict)
            '''
            test_zeroshot_pred_de_data = target_dir + '/test_data_de.jsonl'
            prepare_train_test_data(test_question_file, test_zeroshot_pred_de_cc_file, 'de', test_zeroshot_pred_de_data,
                                    label_dict)

            test_zeroshot_pred_ru_data = target_dir + '/test_data_ru.jsonl'
            prepare_train_test_data(test_question_file, test_zeroshot_pred_ru_cc_file, 'ru', test_zeroshot_pred_ru_data,
                                    label_dict)