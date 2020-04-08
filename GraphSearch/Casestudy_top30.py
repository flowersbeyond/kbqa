# given a training/test/dev file, and the prediction score,
# calculate the rec/prec/f1/ measures by the following configs
# 1. keep all top 10/20/50/100 candidates
# 2. keep all candidates with prediction score higher than 0.9, 0.8, 0.7
# 3. cal stats, on average how many golden core chains are covered of each question

import json


def gen_debug_data(data_file, pred_file, ranking_result):
    data_list = []
    with open(data_file, encoding='utf-8') as fin:
        for l in fin:
            data = json.loads(l)
            data_list.append(data)
    preds = []
    with open(pred_file, encoding='utf-8') as fin:
        for l in fin:
            pred = json.loads(l)[0]
            preds.append(pred)

    assert (len(data_list) == len(preds))
    for i in range(0, len(data_list)):
        data_list[i]['pred'] = preds[i]

    questions = {}
    for data in data_list:
        id = data['id']
        if id not in questions:
            questions[id] = []
        questions[id].append({'question': data['question'],
                              'chain_str': data['chain_str'],'chain':data['chain'],
                              'f1': data['f1'], 'is_gold': data['is_gold'], 'pred': data['pred']})

    for id in questions:
        questions[id].sort(key=lambda item: item['pred'], reverse=True)

    with open(ranking_result, encoding='utf-8', mode='w') as fout:
        for id in questions:
            items = questions[id]
            missing_hits = []
            for i in range(0, len(items)):
                item = items[i]
                if i >= 20 and item['is_gold'] == 1:
                    item['missing'] = 1
                    missing_hits.append(item)
            if len(missing_hits) > 0:
                top20_items = items[0:20]
                missing_hits.extend(top20_items)
                fout.write(json.dumps({'id': id, 'items': missing_hits},indent=2) + '\n')


if __name__ == '__main__':
    # configs = ['10label', '10zeroshot','20label', '20zeroshot']
    configs = ['10zeroshot_enrich']
    task_names = ['dev', 'test_data_label']#, 'test_data_en', 'test_data_ru', 'test_data_de']
    # task_names = ['test_data_en']
    for config in configs:
        dir = './data/core_chain/trainingdata/' + config + '/'
        full_stat_table = {}
        for task in task_names:
            if task == 'dev':
                data_file = dir + task + '_data.jsonl'
            else:
                data_file = dir + task + '.jsonl'
            pred_file = dir + task + '_results_detail.txt'

            ranking_result_debug = dir + task + '_ranking_result_debug.jsonl'
            gen_debug_data(data_file, pred_file, ranking_result_debug)
