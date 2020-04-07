from utils.DBPedia_Utils import parse_dbpedia_line
from tqdm import tqdm


def getPref(item):
    if item.startswith('<') and item.endswith('>'):
        item = item[1:-1]
        entity_name = item[item.rfind('/') + 1:]
        # entity_name = entity_name[entity_name.rfind(':') + 1:]
        # entity_name = entity_name[entity_name.rfind('#') + 1:]
        # entity_name = entity_name[entity_name.rfind('\\') + 1:]
        prefix = item[0:item.rfind(entity_name)]
        return prefix
    return ''

if __name__ == '__main__':

    dbpedia_file = './data/core_chain/2hop_closure.ttl'
    entity_prefix_file = './data/core_chain/entity_prefixes.txt'
    pred_prefix_file = './data/core_chain/pred_prefixes.txt'

    pred_prefix_set = {}
    entity_prefix_set = {}
    with open(dbpedia_file, encoding='utf-8') as fin:
        pbar = tqdm(fin)
        for l in pbar:
            subj, pred, obj = parse_dbpedia_line(l)

            subj_pref = getPref(subj)
            if subj_pref not in entity_prefix_set:
                entity_prefix_set[subj_pref] = 0
            else:
                entity_prefix_set[subj_pref] += 1

            obj_pref = getPref(obj)
            if obj_pref not in entity_prefix_set:
                entity_prefix_set[obj_pref] = 0
            else:
                entity_prefix_set[obj_pref] += 1

            pred_pref = getPref(pred)
            if pred_pref not in pred_prefix_set:
                pred_prefix_set[pred_pref] = 0
            else:
                pred_prefix_set[pred_pref] += 1

    sorted_entity_prefixes = sorted(entity_prefix_set.items(), key=lambda x: x[1], reverse=True)
    sorted_pred_prefixes = sorted(pred_prefix_set.items(), key=lambda x:x[1], reverse=True)
    with open(entity_prefix_file, encoding='utf-8', mode='w') as fout:
        for pair in sorted_entity_prefixes:
            fout.write(pair[0] + ':' + str(pair[1]) + '\n')
    with open(pred_prefix_file, encoding='utf-8', mode='w') as fout:
        for pair in sorted_pred_prefixes:
            fout.write(pair[0] + ':' + str(pair[1]) + '\n')