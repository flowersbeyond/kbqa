# given a training/test/dev file, and the prediction score,
# calculate the rec/prec/f1/ measures by the following configs
# 1. keep all top 10/20/50/100 candidates
# 2. keep all candidates with prediction score higher than 0.9, 0.8, 0.7
# 3. cal stats, on average how many golden core chains are covered of each question

import json


def compute_metrics(data_file, pred_file, stat_file, ranking_result):
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
                                               'chain_str': data['chain_str'],
                                               'f1': data['f1'], 'is_gold': data['is_gold'], 'pred': data['pred']})

    for id in questions:
        questions[id].sort(key=lambda item: item['pred'], reverse=True)
    with open(ranking_result, encoding='utf-8', mode='w') as fout:
        for id in questions:
            items = questions[id]
            end_index = min(len(items), 20)
            fout.write(json.dumps({'id':id, 'items':items[0:end_index]}))

    stat_configs = ['top10', 'top20', 'top50', 'top100','thres_50','thres_70', 'thres_80', 'thres_90']
    stat_table = {}
    for config in stat_configs:
        stat_table[config] = {'rec':0.0, 'prec':0.0, 'f1':0.0, 'two_hop': 0}
    stat_table['gold_ratio'] = 0.0
    non_zero_gold_question_count = 0
    two_hop_question_count = 0
    for id in questions:
        items = questions[id]

        stat_table_item = {}
        for config in stat_configs:
            stat_table_item[config] = {'rec':0.0, 'prec':0.0, 'f1':0.0, 'two_hop': 0}

        thres_50_count = -1
        thres_70_count = -1
        thres_80_count = -1
        thres_90_count = -1
        total_gold_count = 0
        is_two_hop = False
        for i in range(0, len(items)):
            item = items[i]
            pred = item['pred']
            if pred < 0.9 and thres_90_count == -1:
                thres_90_count = i
            if pred < 0.8 and thres_80_count == -1:
                thres_80_count = i
            if pred < 0.7 and thres_70_count == -1:
                thres_70_count = i
            if pred < 0.5 and thres_50_count == -1:
                thres_50_count = i

            if item['is_gold'] == 1:
                total_gold_count += 1
                if item['chain_str'].find(' , ') >= 0:
                    is_two_hop = True

                if i < 10:
                    stat_table_item['top10']['rec'] += 1
                    stat_table_item['top10']['prec'] += 1
                    stat_table_item['top10']['two_hop'] += 1 if is_two_hop else 0
                if i < 20:
                    stat_table_item['top20']['rec'] += 1
                    stat_table_item['top20']['prec'] += 1
                    stat_table_item['top20']['two_hop'] += 1 if is_two_hop else 0
                if i < 50:
                    stat_table_item['top50']['rec'] += 1
                    stat_table_item['top50']['prec'] += 1
                    stat_table_item['top50']['two_hop'] += 1 if is_two_hop else 0
                if i < 100:
                    stat_table_item['top100']['rec'] += 1
                    stat_table_item['top100']['prec'] += 1
                    stat_table_item['top100']['two_hop'] += 1 if is_two_hop else 0
                if pred >= 0.9:
                    stat_table_item['thres_90']['rec'] += 1
                    stat_table_item['thres_90']['prec'] += 1
                    stat_table_item['thres_90']['two_hop'] += 1 if is_two_hop else 0
                if pred >= 0.8:
                    stat_table_item['thres_80']['rec'] += 1
                    stat_table_item['thres_80']['prec'] += 1
                    stat_table_item['thres_80']['two_hop'] += 1 if is_two_hop else 0
                if pred >= 0.7:
                    stat_table_item['thres_70']['rec'] += 1
                    stat_table_item['thres_70']['prec'] += 1
                    stat_table_item['thres_70']['two_hop'] += 1 if is_two_hop else 0

                if pred >= 0.5:
                    stat_table_item['thres_50']['rec'] += 1
                    stat_table_item['thres_50']['prec'] += 1
                    stat_table_item['thres_50']['two_hop'] += 1 if is_two_hop else 0

        if total_gold_count != 0:
            non_zero_gold_question_count += 1
            for config in stat_configs:
                if total_gold_count != 0:
                    stat_table_item[config]['rec'] /= total_gold_count
                else:
                    stat_table_item[config]['rec'] = 0
            prec_base = {'top10':10, 'top20':20, 'top50':50, 'top100':100,
                         'thres_90':thres_90_count, 'thres_80':thres_80_count, 'thres_70':thres_70_count, 'thres_50':thres_50_count}
            for config in stat_configs:
                if prec_base[config] != 0:
                    stat_table_item[config]['prec'] /= prec_base[config]
                else:
                    stat_table_item[config]['prec'] = 0

            for config in stat_configs:
                rec = stat_table_item[config]['rec']
                prec = stat_table_item[config]['prec']
                if rec + prec == 0:
                    stat_table_item[config]['f1'] = 0
                else:
                    stat_table_item[config]['f1'] = 2 * prec * rec / (prec + rec)

            gold_ratio = total_gold_count / len(items)
            stat_table_item['gold_ratio'] = gold_ratio

            if is_two_hop:
                two_hop_question_count += 1

            for config in stat_configs:
                stat_table[config]['rec'] += stat_table_item[config]['rec']
                stat_table[config]['prec'] += stat_table_item[config]['prec']
                stat_table[config]['f1'] += stat_table_item[config]['f1']
                stat_table[config]['two_hop'] += 1 if stat_table_item[config]['two_hop'] > 0 else 0
            stat_table['gold_ratio'] += stat_table_item['gold_ratio']

    for config in stat_configs:
        stat_table[config]['rec'] /= non_zero_gold_question_count#len(questions)
        stat_table[config]['prec'] /= non_zero_gold_question_count#len(questions)
        stat_table[config]['f1'] /= non_zero_gold_question_count#len(questions)
    stat_table['gold_ratio'] /= non_zero_gold_question_count#len(questions)

    with open(stat_file, encoding='utf-8', mode='w') as fout:
        fout.write('total questions\t%d\n' % len(questions))
        fout.write('total question with gold cc\t%d\n' % non_zero_gold_question_count)
        fout.write('gold_ratio\t%f\n' % stat_table['gold_ratio'])
        fout.write('total twohop questions\t%d\n' % two_hop_question_count)
        fout.write('\trec\tprec\tf1\ttwohop\n')


        for config in stat_configs:
            fout.write('%s\t%f\t%f\t%f\t%d\n' % (
                config,
                stat_table[config]['rec'],
                stat_table[config]['prec'],
                stat_table[config]['f1'],
                stat_table[config]['two_hop']
                )
            )


if __name__ == '__main__':
    configs = ['05', '10']
    for config in configs:
        dir = './data/core_chain/trainingdata/' + config + '/'
        eval_data_file = dir + 'dev_data.jsonl'
        eval_pred_file = dir + 'Eval_results_detail.txt'
        eval_ranking_result = dir + 'eval_ranking_result.txt'
        eval_stat_file = dir + 'eval_stat.txt'
        compute_metrics(eval_data_file, eval_pred_file, eval_stat_file, eval_ranking_result)

        test_data_file = dir + 'test_data.jsonl'
        test_pred_file = dir + 'Test_results_detail.txt'
        test_ranking_result = dir + 'test_ranking_result.txt'
        test_stat_file = dir + 'test_stat.txt'
        compute_metrics(test_data_file, test_pred_file, test_stat_file, test_ranking_result)
