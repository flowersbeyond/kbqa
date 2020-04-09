# given a training/test/dev file, and the prediction score,
# calculate the rec/prec/f1/ measures by the following configs
# 1. keep all top 10/20/50/100 candidates
# 2. keep all candidates with prediction score higher than 0.9, 0.8, 0.7
# 3. cal stats, on average how many golden core chains are covered of each question

import json


def compute_metrics(data_file, pred_file, ranking_result):
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

    thres_configs = ['top10', 'top20', 'top50', 'top100','top1000','top2000','thres_10','thres_30','thres_60', 'thres_80']
    stat_table = {}
    for config in thres_configs:
        stat_table[config] = {'rec':0.0, 'qrec':0.0, 'prec':0.0, 'f1':0.0, 'two_hop': 0}

    non_zero_gold_question_count = 0

    for id in questions:
        items = questions[id]

        stat_table_item = {}
        for config in thres_configs:
            stat_table_item[config] = {'rec':0.0, 'qrec':0.0, 'prec':0.0, 'f1':0.0, 'two_hop': 0}

        thres_10_count = -1
        thres_30_count = -1
        thres_60_count = -1
        thres_80_count = -1
        total_gold_count = 0

        is_two_hop = False
        for i in range(0, len(items)):
            item = items[i]
            pred = item['pred']
            if pred < 0.8 and thres_80_count == -1:
                thres_80_count = i
            if pred < 0.6 and thres_60_count == -1:
                thres_60_count = i
            if pred < 0.3 and thres_30_count == -1:
                thres_30_count = i
            if pred < 0.1 and thres_10_count == -1:
                thres_10_count = i

            if item['is_gold'] == 1: #or item['f1'] >= 0.99999:
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

                if i < 1000:
                    stat_table_item['top1000']['rec'] += 1
                    stat_table_item['top1000']['prec'] += 1
                    stat_table_item['top1000']['two_hop'] += 1 if is_two_hop else 0

                if i < 2000:
                    stat_table_item['top2000']['rec'] += 1
                    stat_table_item['top2000']['prec'] += 1
                    stat_table_item['top2000']['two_hop'] += 1 if is_two_hop else 0


                if pred >= 0.8:
                    stat_table_item['thres_80']['rec'] += 1
                    stat_table_item['thres_80']['prec'] += 1
                    stat_table_item['thres_80']['two_hop'] += 1 if is_two_hop else 0
                if pred >= 0.6:
                    stat_table_item['thres_60']['rec'] += 1
                    stat_table_item['thres_60']['prec'] += 1
                    stat_table_item['thres_60']['two_hop'] += 1 if is_two_hop else 0

                if pred >= 0.3:
                    stat_table_item['thres_30']['rec'] += 1
                    stat_table_item['thres_30']['prec'] += 1
                    stat_table_item['thres_30']['two_hop'] += 1 if is_two_hop else 0

                if pred >= 0.1:
                    stat_table_item['thres_10']['rec'] += 1
                    stat_table_item['thres_10']['prec'] += 1
                    stat_table_item['thres_10']['two_hop'] += 1 if is_two_hop else 0

        if total_gold_count != 0:
            non_zero_gold_question_count += 1
            for config in thres_configs:
                if total_gold_count != 0:
                    stat_table_item[config]['qrec'] = 1 if stat_table_item[config]['rec'] > 0 else 0
                    stat_table_item[config]['rec'] /= total_gold_count
                else:
                    stat_table_item[config]['rec'] = 0
            prec_base = {'top10':10, 'top20':20, 'top50':50, 'top100':100, 'top1000':1000,'top2000':2000,
                         'thres_80':thres_80_count, 'thres_60':thres_60_count, 'thres_30':thres_30_count, 'thres_10': thres_10_count}
            for config in thres_configs:
                if prec_base[config] != 0:
                    stat_table_item[config]['prec'] /= prec_base[config]
                else:
                    stat_table_item[config]['prec'] = 0

            for config in thres_configs:
                rec = stat_table_item[config]['rec']
                prec = stat_table_item[config]['prec']
                if rec + prec == 0:
                    stat_table_item[config]['f1'] = 0
                else:
                    stat_table_item[config]['f1'] = 2 * prec * rec / (prec + rec)

            for config in thres_configs:
                stat_table[config]['qrec'] += stat_table_item[config]['qrec']
                stat_table[config]['rec'] += stat_table_item[config]['rec']
                stat_table[config]['prec'] += stat_table_item[config]['prec']
                stat_table[config]['f1'] += stat_table_item[config]['f1']
                stat_table[config]['two_hop'] += 1 if stat_table_item[config]['two_hop'] > 0 else 0

    for config in thres_configs:
        stat_table[config]['qrec'] /= non_zero_gold_question_count
        stat_table[config]['rec'] /= non_zero_gold_question_count
        stat_table[config]['prec'] /= non_zero_gold_question_count
        stat_table[config]['f1'] /= non_zero_gold_question_count


    return stat_table

def compute_meta_metrics(data_file):
    data_list = []
    with open(data_file, encoding='utf-8') as fin:
        for l in fin:
            data = json.loads(l)
            data_list.append(data)

    questions = {}
    for data in data_list:
        id = data['id']
        if id not in questions:
            questions[id] = []
        questions[id].append({'question': data['question'],
                              'chain_str': data['chain_str'],
                              'f1': data['f1'], 'is_gold': data['is_gold']})

    stat_table = {}

    stat_table['gold_ratio'] = 0.0
    non_zero_gold_question_count = 0
    two_hop_question_count = 0
    for id in questions:
        items = questions[id]

        total_gold_count = 0
        is_two_hop = False
        for i in range(0, len(items)):
            item = items[i]
            if item['is_gold'] == 1:
                total_gold_count += 1
                if item['chain_str'].find(' , ') >= 0:
                    is_two_hop = True

        if total_gold_count != 0:
            non_zero_gold_question_count += 1
            gold_ratio = total_gold_count / len(items)
            if is_two_hop:
                two_hop_question_count += 1
            stat_table['gold_ratio'] += gold_ratio

    stat_table['gold_ratio'] /= non_zero_gold_question_count
    stat_table['total questions'] = len(questions)
    stat_table['non_zero_gold_cc_question_count'] = non_zero_gold_question_count
    stat_table['two_hop_questions_count'] = two_hop_question_count

    return stat_table


if __name__ == '__main__':
    #configs = ['10label', '10zeroshot','20label', '20zeroshot']
    configs = ['10zeroshot_enrich_add_category_info']
    task_names = ['dev','test_data_label', 'test_data_en', 'test_data_ru', 'test_data_de']
    #task_names = ['dev','test_data_en']
    meta_metric_file = './data/core_chain/trainingdata/meta_metrics.txt'

    full_meta_metric_table = {}

    for task in task_names:
        dir = './data/core_chain/trainingdata/' + configs[0] + '/'
        if task == 'dev':
            data_file = dir + task + '_data.jsonl'
        else:
            data_file = dir + task + '.jsonl'

        meta_metrics = compute_meta_metrics(data_file)
        full_meta_metric_table[task] = meta_metrics
    

    with open(meta_metric_file, encoding='utf-8', mode='w') as fout:
        header_str = ''
        for task in task_names:
            header_str += ('\t' + task)
        header_str += '\n'
        fout.write(header_str)

        metric_names = ['gold_ratio','total questions','non_zero_gold_cc_question_count','two_hop_questions_count']
        for metric in metric_names:
            data_line_str = metric
            for task in task_names:
                data_line_str += '\t' + str(full_meta_metric_table[task][metric])
            data_line_str += '\n'
            fout.write(data_line_str)


    thres_configs = ['top10', 'top20', 'top50', 'top100', 'top1000','top2000', 'thres_80', 'thres_60','thres_30','thres_10']
    for config in configs:
        dir = './data/core_chain/trainingdata/' + config + '/'
        full_stat_table = {}
        for task in task_names:
            if task == 'dev':
                data_file = dir + task + '_data.jsonl'
            else:
                data_file = dir + task + '.jsonl'
            pred_file = dir + task + '_results_detail.txt'

            ranking_result = dir + task + '_ranking_result.txt'
            stats = compute_metrics(data_file, pred_file, ranking_result)
            full_stat_table[task] = stats

        stat_summary_file = dir + 'stat_summary.txt'
        with open(stat_summary_file, encoding='utf-8', mode='w') as fout:
            header1_str = '\t'
            for task in task_names:
                header1_str += task + '\t\t\t\t\t'
            header1_str += '\n'
            fout.write(header1_str)

            header2_str = 'thres\t'
            header2_str += ('qrec\trec\tprec\tf1\t2hop\t' * len(task_names))
            header2_str += '\n'
            fout.write(header2_str)

            for thres_config in thres_configs:
                data_line_str = thres_config + '\t'
                for task in task_names:
                    data_line_str += '%f\t%f\t%f\t%f\t%d\t' % (full_stat_table[task][thres_config]['qrec'],
                                                           full_stat_table[task][thres_config]['rec'],
                                                           full_stat_table[task][thres_config]['prec'],
                                                           full_stat_table[task][thres_config]['f1'],
                                                           full_stat_table[task][thres_config]['two_hop'])
                data_line_str += '\n'
                fout.write(data_line_str)