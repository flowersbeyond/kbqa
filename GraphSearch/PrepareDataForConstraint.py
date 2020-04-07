# given a training/test/dev file, and the prediction score,
# calculate the rec/prec/f1/ measures by the following configs
# 1. keep all top 10/20/50/100 candidates
# 2. keep all candidates with prediction score higher than 0.9, 0.8, 0.7
# 3. cal stats, on average how many golden core chains are covered of each question

import json
from GraphSearch.FullDBGraph import DBGraph
from tqdm import tqdm

def search_var_values(core_chain, full_graph):
    result = set()
    if len(core_chain) == 3:
        if core_chain[2] == 'VAR':
            pairs = full_graph.search_1hop_obj(core_chain[0])
            if core_chain[1] in pairs:
                result = set(pairs[core_chain[1]])

            return list(result)
        else:
            pairs = full_graph.search_1hop_subj(core_chain[2])
            if core_chain[1] in pairs:
                result = set(pairs[core_chain[1]])

            return list(result)

    if len(core_chain) == 6:
        var1_set = set()
        pred1 = core_chain[1]
        if core_chain[0] == 'VAR1':
            pairs = full_graph.search_1hop_subj(core_chain[2])
            if pred1 in pairs:
                var1_set = set(pairs[pred1])
        else:
            pairs = full_graph.search_1hop_obj(core_chain[0])
            if pred1 in pairs:
                var1_set = set(pairs[pred1])

        var2_set = {}
        pred2 = core_chain[4]
        for var1 in var1_set:
            if core_chain[3] == 'VAR2':
                pairs = full_graph.search_1hop_subj(var1)
                if pred2 in pairs:
                    var2_set[var1] = list(pairs[pred2])
            else:
                pairs = full_graph.search_1hop_obj(var1)
                if pred2 in pairs:
                    var2_set[var1] =list(pairs[pred2])

        return var2_set



    return []

def retrieve_top_candidates(data_file, pred_file, ranking_result, rank_number, full_graph=None):
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

    assert(len(data_list) == len(preds))
    for i in range(0, len(data_list)):
        data_list[i]['pred'] = preds[i]

    questions = {}
    for data in data_list:
        id = data['id']
        if id not in questions:
            questions[id] = []
        questions[id].append({'question':data['question'],
                                               'chain_str': data['chain_str'],'chain':data['chain'],
                                               'f1': data['f1'], 'is_gold': data['is_gold'], 'pred': data['pred']})

    for id in questions:
        questions[id].sort(key=lambda item: item['pred'], reverse=True)

    with open(ranking_result, encoding='utf-8', mode='w') as fout:
        pbar = tqdm(questions)
        for id in pbar:
            items = questions[id]
            end_index = min(len(items), rank_number)
            items = items[0:end_index]
            if full_graph != None:
                for item in items:
                    chain = item['chain']
                    var_values = search_var_values(chain, full_graph)
                    item['var_values'] = var_values

                fout.write(json.dumps({'id':id, 'items':items}) + '\n')
            else:
                for item in items:
                    item['id'] = id
                    fout.write(json.dumps(item) + '\n')

if __name__ == '__main__':
    graph = DBGraph()
    dbfile_output_2hop = './data/core_chain/2hop_closure.ttl'
    dbfiles = [dbfile_output_2hop]
    graph.load_full_graph(dbfiles)
    configs = ['10zeroshot']
    #task_names = ['train_data_all','dev','test_data_label', 'test_data_en', 'test_data_ru', 'test_data_de']
    task_names = ['train_data_all', 'test_data_en']

    for task in task_names:
        dir = './data/core_chain/trainingdata/' + configs[0] + '/'
        if task == 'dev':
            data_file = dir + task + '_data.jsonl'
        else:
            data_file = dir + task + '.jsonl'
        pred_file = dir + task + '_results_detail.txt'

        ranking_result = dir + 'top100/' + task + '.jsonl'
        retrieve_top_candidates(data_file, pred_file, ranking_result, rank_number=100, full_graph=graph)
