from GraphSearch.FullDBGraph import DBGraph
import json
import urllib.parse
from os import listdir
from os.path import isfile, join
from tqdm import tqdm
import pickle

def search_core_chain(topic_entity, full_graph, category_type_entity_list, do_2hop=True):
    if topic_entity in category_type_entity_list:
        return {}

    dbgraph = full_graph
    one_hops_objs = dbgraph.search_1hop_obj(topic_entity)
    one_hop_subjs = dbgraph.search_1hop_subj(topic_entity)

    core_chains = {}

    for pred in one_hops_objs:
        objs = one_hops_objs[pred]
        core_chains[(topic_entity, pred,'VAR')] = set(objs)

        if do_2hop:
            for obj in objs:
                if obj.startswith('<') and obj.endswith('>'):
                    second_hops_objs = dbgraph.search_1hop_obj(obj)
                    for pred2 in second_hops_objs:
                        obj2s = second_hops_objs[pred2]
                        chain_pattern = (topic_entity, pred, 'VAR1', 'VAR1', pred2, 'VAR2')
                        if chain_pattern not in core_chains:
                            core_chains[chain_pattern] = set()

                        core_chains[chain_pattern].update(set(obj2s))

                    if obj not in category_type_entity_list:
                        second_hop_subjs = dbgraph.search_1hop_subj(obj)
                        for pred2 in second_hop_subjs:
                            subj2s = second_hop_subjs[pred2]
                            chain_pattern = (topic_entity, pred, 'VAR1', 'VAR2', pred2, 'VAR1')
                            if chain_pattern not in core_chains:
                                core_chains[chain_pattern] = set()
                            core_chains[chain_pattern].update(set(subj2s))

    for pred in one_hop_subjs:
        subjs = one_hop_subjs[pred]
        core_chains[('VAR', pred, topic_entity)] = set(subjs)

        if do_2hop:
            for subj in subjs:
                if subj.startswith('<') and subj.endswith('>'):
                    second_hops_objs = dbgraph.search_1hop_obj(subj)
                    for pred2 in second_hops_objs:
                        obj2s = second_hops_objs[pred2]
                        chain_pattern = ('VAR1', pred, topic_entity, 'VAR1', pred2, 'VAR2')
                        if chain_pattern not in core_chains:
                            core_chains[chain_pattern] = set()

                        core_chains[chain_pattern].update(set(obj2s))

                    if subj not in category_type_entity_list:
                        second_hop_subjs = dbgraph.search_1hop_subj(subj)
                        for pred2 in second_hop_subjs:
                            subj2s = second_hop_subjs[pred2]
                            chain_pattern = ('VAR1', pred, topic_entity, 'VAR2', pred2, 'VAR1')
                            if chain_pattern not in core_chains:
                                core_chains[chain_pattern] = set()
                            core_chains[chain_pattern].update(set(subj2s))


    return core_chains

def pattern_match(core_chain, full_graph):
    result = set()
    if len(core_chain) == 3:
        if core_chain[2] == 'VAR':
            pairs = full_graph.search_1hop_obj(core_chain[0])
            if core_chain[1] in pairs:
                result = set(pairs[core_chain[1]])

            return result
        else:
            pairs = full_graph.search_1hop_subj(core_chain[2])
            if core_chain[1] in pairs:
                result = set(pairs[core_chain[1]])

            return result

    if len(core_chain) == 6:
        var1_set = set()
        pred1 = core_chain[1]
        if core_chain[0] == 'VAR1':
            pairs = full_graph.search_1hop_subj(core_chain[2])
            if pred1 in var1_set:
                var1_set = set(pairs[pred1])
        else:
            pairs = full_graph.search_1hop_obj(core_chain[0])
            if pred1 in var1_set:
                var1_set = set(pairs[pred1])

        var2_set = set()
        pred2 = core_chain[4]
        for var1 in var1_set:
            if core_chain[3] == 'VAR2':
                pairs = full_graph.search_1hop_subj(var1)
                if pred2 in pairs:
                    var2_set.update(pairs[pred2])
            else:
                pairs = full_graph.search_1hop_obj(var1)
                if pred2 in pairs:
                    var2_set.update(pairs[pred2])

        return var2_set



    return set()


def get_f1_score(partial_ans, golden_ans):
    true_positive_answers = partial_ans.intersection(golden_ans)

    if len(partial_ans) == 0:
        precision = 0
    else:
        precision = len(true_positive_answers) / len(partial_ans)
    if len(golden_ans) == 0:
        recall = 0
    else:
        recall = len(true_positive_answers) / len(golden_ans)

    if precision + recall == 0:
        return 0
    f1 = 2 * precision * recall / (precision + recall)
    return f1

def parse_golden_answer(result):
    values = set()

    head = result['head']
    if 'vars' in head:
        vars = head['vars']
        if len(vars) > 1:
            print(str(id) + 'has more than 1 variables')
        bindings = result['results']['bindings']
        for binding in bindings:
            if len(binding) > 0:
                for key in binding:
                    value = binding[key]['value']
                    value = urllib.parse.unquote(str(value), encoding='utf-8')
                    if value.find('http') >= 0:
                        value = '<' + value + '>'
                    values.add(value)
        return values
    else:
        values.add(str(result['boolean']).lower())
        return values

def get_all_entities_from_entity_linking_file(entity_linking_file, category_type_entities):
    all_entities = {}
    all_entity_count = 0
    empty_entity_linking_count = 0
    with open(entity_linking_file, 'rb') as fin:
        entity_linkings = pickle.load(fin)

        for id in entity_linkings:
            if 'search_result_per_keyword' not in entity_linkings[id]:
                all_entities[id] = {}
                empty_entity_linking_count += 1
                continue

            search_result = entity_linkings[id]['search_result_per_keyword']
            entities = {}
            for keyword in search_result:
                for item in search_result[keyword]:
                    entity_str = item[1]
                    entity_split = entity_str.split(' ')
                    for entity in entity_split:
                        if entity not in category_type_entities:
                            entities[entity] = keyword
            if len(entities) == 0:
                all_entities[id] = {}
                empty_entity_linking_count += 1
                continue

            all_entities[id] = entities
            all_entity_count += len(entities)
    print(entity_linking_file)
    print('total_question_number:%d' % len(all_entities))
    print('empty entity linking number:%d' % empty_entity_linking_count)
    print('avg linked entity number:%f' % (all_entity_count/(len(all_entities) - empty_entity_linking_count)))

    return all_entities

def generate_core_chain_candidate_stats(
        golden_core_chains, gen_linked_entities, graph, category_type_entities, outputfile):
    golden_core_chain_answers = {}
    for id in golden_core_chains:
        if id not in gen_linked_entities or gen_linked_entities[id] == {}:
            continue
        golden_core_chain_answers[id] = {}
        for item in golden_core_chains[id]:
            chain = item['core_chain']
            answer = pattern_match(chain, graph)
            if len(chain) == 3:
                chain_tuple = (chain[0], chain[1], chain[2])
            else:
                chain_tuple = (chain[0], chain[1], chain[2], chain[3], chain[4], chain[5])
            golden_core_chain_answers[id][chain_tuple] = answer

    print('successfull parsed golden core chain:%d' % len(golden_core_chains))
    print('can find entity linking results:%d' % len(golden_core_chain_answers))

    total_onehop_count = 0
    non_0_f1_onehop_count = 0
    total_twohop_count = 0
    non_0_f1_twohop_count = 0
    covered_onehop = 0
    golden_onehop = 0
    covered_twohop = 0
    golden_twohop = 0

    high_f1_non_gold_count = 0

    covered_onehop_entity_count = 0
    covered_twohop_entity_count = 0

    candidate_core_chain_stat = {}

    pbar = tqdm(gen_linked_entities)

    for id in pbar:
        if id not in golden_core_chain_answers:
            continue

        golden_core_chain_answer_id = golden_core_chain_answers[id]
        golden_topic_entities_id = [item['topic_entity'] for item in golden_core_chains[id]]
        candidate_core_chain_stat[id] = []

        cover_atleast_one = False
        cover_topic_entity = False
        candidate_entites = gen_linked_entities[id]
        for entity in candidate_entites:
            if entity in category_type_entities:
                continue
            if entity in golden_topic_entities_id:
                cover_topic_entity = True

            # core chain candidate: dict:(tuple) -> set()
            core_chain_candidates = search_core_chain(entity, graph, category_type_entities, do_2hop=True)
            for candidate in core_chain_candidates:
                candidate_answer = core_chain_candidates[candidate]
                candidate_is_gold = False
                if candidate in golden_core_chain_answer_id:
                    candidate_is_gold = True
                    cover_atleast_one = True

                max_f1 = 0
                reference_gold_cc = None
                for golden_chain in golden_core_chain_answer_id:
                    golden_answer = golden_core_chain_answer_id[golden_chain]
                    f1 = get_f1_score(candidate_answer, golden_answer)
                    if f1 > max_f1:
                        max_f1 = f1
                        reference_gold_cc = golden_chain

                if max_f1 > 0.8 and max_f1 < 0.999999:
                    high_f1_non_gold_count += 1

                keyword = candidate_entites[entity]
                candidate_core_chain_stat[id].append(
                    {'core_chain': list(candidate), 'keyword': keyword, 'max_f1': max_f1,
                     'is_gold': 1 if candidate_is_gold else 0,
                     'reference': list(reference_gold_cc) if reference_gold_cc != None else ''})

                if len(list(candidate)) == 3:
                    total_onehop_count += 1
                    if max_f1 > 0:
                        non_0_f1_onehop_count += 1
                else:
                    total_twohop_count += 1
                    if max_f1 > 0:
                        non_0_f1_twohop_count += 1

        golden_core_chain_sample = list(golden_core_chain_answer_id.keys())[0]
        if len(golden_core_chain_sample) == 3:
            golden_onehop += 1
            if cover_atleast_one:
                covered_onehop += 1
            if cover_topic_entity:
                covered_onehop_entity_count += 1
        else:
            golden_twohop += 1
            if cover_atleast_one:
                covered_twohop += 1
            if cover_topic_entity:
                covered_twohop_entity_count += 1

        # if not cover_atleast_one:
        #    print('not covered' + str(golden_core_chains.keys()))

    total_question_with_valid_ccs = golden_onehop + golden_twohop
    print('avg_onehop: %f\tavg_twohop: %f' % (total_onehop_count / total_question_with_valid_ccs,
                                              total_twohop_count / total_question_with_valid_ccs))
    print('avg_onehop_non_0: %f\tavg_twohop_non_0: %f' % (non_0_f1_onehop_count / total_question_with_valid_ccs,
                                                          non_0_f1_twohop_count / total_question_with_valid_ccs))

    print('golden 1 hop:\t%d\t 2 hop:\t%d' % (golden_onehop, golden_twohop))
    print('covered 1 hop:\t%d\t 2 hop:\t%d' % (covered_onehop, covered_twohop))
    print('entity covered 1 hop:\t%d\t 2 hop:\t%d' % (covered_onehop_entity_count, covered_twohop_entity_count))
    print('avg high f1 (>0.8):\t%f' % (high_f1_non_gold_count / total_question_with_valid_ccs))

    with open(outputfile, encoding='utf-8', mode='w') as fout:
        for id in candidate_core_chain_stat:
            item = candidate_core_chain_stat[id]
            fout.write(json.dumps({'id':id, 'candidates':item}) + '\n')


if __name__ == '__main__':

    graph = DBGraph()
    dbfile_core_dir = 'D:/Research/kbqa/data/DBPedia/core/'
    category_type_entities_file = dbfile_core_dir + 'category_type/category_type_entities_all.txt'
    category_type_entities = set()

    with open(category_type_entities_file, encoding='utf-8') as fin:
        for l in fin:
            category_type_entities.add(l.strip())


    train_golden_cc_file = './data/core_chain/train_gold_chain.json'
    train_core_chains = {}
    with open(train_golden_cc_file, encoding='utf-8') as fin:
        train_core_chains_temp = json.load(fin)
        for id in train_core_chains_temp:
            train_core_chains[int(id)] = train_core_chains_temp[id]

    test_golden_cc_file = './data/core_chain/test_gold_chain.json'
    test_core_chains = {}
    with open(test_golden_cc_file, encoding='utf-8') as fin:
        test_core_chains_temp = json.load(fin)
        for id in test_core_chains_temp:
            test_core_chains[int(id)] = test_core_chains_temp[id]


    entity_linking_dir = './data/core_chain/entity_linking/'

    train_label_en_entity_linking_file = entity_linking_dir + '/train.en.label.BM25.top10.pk'
    test_label_en_entity_linking_file = entity_linking_dir + '/test.en.label.BM25.top10.pk'
    train_predict_en_zeroshot_entity_linking_file = entity_linking_dir + '/train.zeroshotpred.BM25.top10.pk'
    test_predict_en_zeroshot_entity_linking_file = entity_linking_dir + '/test.zeroshotpred.BM25.top10.pk'

    train_label_en_linked_entities = get_all_entities_from_entity_linking_file(train_label_en_entity_linking_file, category_type_entities)
    test_label_en_linked_entities = get_all_entities_from_entity_linking_file(test_label_en_entity_linking_file, category_type_entities)

    train_zeroshot_en_pred_linked_entities = get_all_entities_from_entity_linking_file(train_predict_en_zeroshot_entity_linking_file,
                                                                                       category_type_entities)
    test_zeroshot_en_pred_linked_entities = get_all_entities_from_entity_linking_file(test_predict_en_zeroshot_entity_linking_file,
                                                                                      category_type_entities)

    test_zeroshot_de_pred_linked_entity_tile = entity_linking_dir + '/test.de.zeroshotpred.BM25.top10.pk'

    test_zeroshot_de_pred_linked_entities = get_all_entities_from_entity_linking_file(
        test_zeroshot_de_pred_linked_entity_tile,
        category_type_entities)
    

    test_zeroshot_ru_pred_linked_entity_tile = entity_linking_dir + '/test.ru.zeroshotpred.BM25.top10.pk'
    test_zeroshot_ru_pred_linked_entities = get_all_entities_from_entity_linking_file(
        test_zeroshot_ru_pred_linked_entity_tile,
        category_type_entities)

    dbfile_output_1hop = './data/core_chain/1hop_closure.ttl'
    dbfile_output_2hop = './data/core_chain/2hop_closure.ttl'

    do_slice = False
    if do_slice:

        train_qg_topic_entities = set()
        for id in train_core_chains:
            chains = train_core_chains[id]
            for chain in chains:
                train_qg_topic_entities.add(chain['topic_entity'])

        test_qg_topic_entities = set()
        for id in test_core_chains:
            chains = test_core_chains[id]
            for chain in chains:
                test_qg_topic_entities.add(chain['topic_entity'])

        topic_entities = set()
        topic_entities.update(train_qg_topic_entities)
        topic_entities.update(test_qg_topic_entities)

        for id in train_label_en_linked_entities:
            if id in train_core_chains:
                entities = train_label_en_linked_entities[id].keys()
                topic_entities.update(entities)

        for id in test_label_en_linked_entities:
            if id in test_core_chains:
                entities = test_label_en_linked_entities[id].keys()
                topic_entities.update(entities)

        for id in train_zeroshot_en_pred_linked_entities:
            if id in train_core_chains:
                entities = train_zeroshot_en_pred_linked_entities[id].keys()
                topic_entities.update(entities)

        for id in test_zeroshot_en_pred_linked_entities:
            if id in test_core_chains:
                entities = test_zeroshot_en_pred_linked_entities[id].keys()
                topic_entities.update(entities)

        for id in test_zeroshot_de_pred_linked_entities:
            if id in test_core_chains:
                entities = test_zeroshot_de_pred_linked_entities[id].keys()
                topic_entities.update(entities)

        for id in test_zeroshot_ru_pred_linked_entities:
            if id in test_core_chains:
                entities = test_zeroshot_ru_pred_linked_entities[id].keys()
                topic_entities.update(entities)

        dbfiles = [(dbfile_core_dir + f) for f in listdir(dbfile_core_dir) if
                   isfile(join(dbfile_core_dir, f)) and f.endswith('.ttl')]

        topic_onehop_triples, topic_onehop_lines = graph.slice_1hop(topic_entities, dbfiles, category_type_entities)
        with open(dbfile_output_1hop, encoding='utf-8', mode='w') as fout:
            pbar = tqdm(topic_onehop_lines)
            for l in pbar:
                fout.write(l)

        print('total %d one hop lines' % len(topic_onehop_lines))

        onehop_entities = set()
        for triple in topic_onehop_triples:
            subj = triple[0]
            obj = triple[2]
            if subj.startswith('<') and subj.endswith('>'):
                onehop_entities.add(subj)
            if obj.startswith('<') and obj.endswith('>'):
                onehop_entities.add(obj)
        print('total %d one hop entites' % len(onehop_entities))

        twohop_triples, twohop_lines = graph.slice_1hop(onehop_entities, dbfiles, category_type_entities)

        with open(dbfile_output_2hop, encoding='utf-8', mode='w') as fout:
            pbar = tqdm(twohop_lines)
            for l in pbar:
                fout.write(l)

        print('total %d two hop lines' % len(twohop_lines))


    graph = DBGraph()
    
    dbfiles = [dbfile_output_2hop]
    graph.load_full_graph(dbfiles)
    #graph.dump_to_json('./data/core_chain/2hop_closure.json')
    #graph.load_from_json('./data/core_chain/2hop_closure.json')

    
    train_label_en_output_file = './data/core_chain/candidates/train_label_en.jsonl'
    generate_core_chain_candidate_stats(train_core_chains, train_label_en_linked_entities, graph,
                                        category_type_entities, train_label_en_output_file)
    
    test_label_en_output_file = './data/core_chain/candidates/test_label_en.jsonl'
    generate_core_chain_candidate_stats(test_core_chains, test_label_en_linked_entities, graph, category_type_entities,
                                        test_label_en_output_file)

    train_zeroshot_pred_en_output_file = './data/core_chain/candidates/train_zeroshot_pred_en.jsonl'
    generate_core_chain_candidate_stats(train_core_chains, train_zeroshot_en_pred_linked_entities, graph,
                                        category_type_entities, train_zeroshot_pred_en_output_file)

    test_zeroshot_en_pred_output_file = './data/core_chain/candidates/test_zeroshot_pred_en.jsonl'
    generate_core_chain_candidate_stats(test_core_chains, test_zeroshot_en_pred_linked_entities, graph, category_type_entities,
                                        test_zeroshot_en_pred_output_file)
    
    test_zeroshot_ru_pred_output_file = './data/core_chain/candidates/test_zeroshot_pred_ru.jsonl'
    generate_core_chain_candidate_stats(test_core_chains, test_zeroshot_ru_pred_linked_entities, graph,
                                        category_type_entities,
                                        test_zeroshot_ru_pred_output_file)

    test_zeroshot_de_pred_output_file = './data/core_chain/candidates/test_zeroshot_pred_de.jsonl'
    generate_core_chain_candidate_stats(test_core_chains, test_zeroshot_de_pred_linked_entities, graph,
                                        category_type_entities,
                                        test_zeroshot_de_pred_output_file)
