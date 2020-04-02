from utils.DBPedia_Utils import  parse_dbpedia_line
from os import listdir
from os.path import isfile, join
from tqdm import tqdm


dbpedia_data_dir = 'D:/Research/kbqa/data/DBPedia/core/category_type/'
file_names = [f for f in listdir(dbpedia_data_dir) if
                         isfile(join(dbpedia_data_dir, f)) and f.endswith('.ttl')]

pred_obj_pair = {}

for file in file_names:
    with open(dbpedia_data_dir + file, encoding='utf-8') as fin:
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue
            subj, pred, obj = parse_dbpedia_line(l)
            if not (obj.startswith('<') and obj.endswith('>')):
                print(l)
                continue
            if pred not in pred_obj_pair:
                pred_obj_pair[pred] = {}
            if obj not in pred_obj_pair[pred]:
                pred_obj_pair[pred][obj] = 0

            pred_obj_pair[pred][obj] += 1

with open(dbpedia_data_dir + 'category_type_entities_all.txt', encoding='utf-8', mode='w') as fout:
    for pred in pred_obj_pair:
        for obj in pred_obj_pair[pred]:
            #if pred_obj_pair[pred][obj] >= 20:
            fout.write(obj + '\n')
