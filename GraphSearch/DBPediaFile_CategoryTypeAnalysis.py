from utils.DBPedia_Utils import  parse_dbpedia_line,parse_dbpedia_line_with_obj_type
from os import listdir
from os.path import isfile, join
from tqdm import tqdm
import json
import re

dbpedia_data_dir = 'D:/Research/kbqa/data/DBPedia/core/category_type/'
file_names = [f for f in listdir(dbpedia_data_dir) if
                         isfile(join(dbpedia_data_dir, f)) and f.endswith('.ttl')]

'''
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

'''

obj_count = {}
subj_obj_pair = {}

category_file = dbpedia_data_dir + 'article_categories_en.ttl'

with open(category_file, encoding='utf-8') as fin:
    pbar = tqdm(fin)
    for l in pbar:
        if l.startswith('#'):
            continue
        subj, pred, obj = parse_dbpedia_line(l)
        if not (obj.startswith('<') and obj.endswith('>')):
            print(l)
            continue
        if obj not in obj_count:
            obj_count[obj] = 1
        else:
            obj_count[obj] += 1
        subj_obj_pair[subj] = obj

filtered_categories = {}
uncovered_subjs = set()
for subj in subj_obj_pair:
    obj = subj_obj_pair[subj]
    if obj_count[obj] >= 100:
        filtered_categories[subj] = obj
    else:
        uncovered_subjs.add(subj)


type_count = {}
type_map = {}

type_files = [dbpedia_data_dir + 'instance_types_transitive_en.ttl', dbpedia_data_dir + 'instance_types_en.ttl']
for type_file in type_files:
    with open(type_file, encoding='utf-8') as fin:
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue
            subj, pred, obj = parse_dbpedia_line(l)
            if not (obj.startswith('<') and obj.endswith('>')):
                print(l)
                continue
            if obj == '<http://www.w3.org/2002/07/owl#Thing>':
                continue
            if obj not in type_count:
                type_count[obj] = 1
            else:
                type_count[obj] += 1

            type_map[subj] = obj

for subj in type_map:
    obj = type_map[subj]
    if type_count[obj] >= 100:
        filtered_categories[subj] = obj

        if subj in uncovered_subjs:
            uncovered_subjs.remove(subj)

uncovered_subj_file = dbpedia_data_dir + 'uncovered_subjs.txt'
with open(uncovered_subj_file, encoding='utf-8', mode='w') as fout:
    for subj in uncovered_subjs:
        fout.write(subj + '\n')


category_type_map_file = dbpedia_data_dir + 'category_type_map.json'
with open(category_type_map_file, encoding='utf-8',mode='w') as fout:
    json.dump(filtered_categories,fout, indent=2)


labels = {}
label_files = [dbpedia_data_dir + 'category_labels_en.ttl', dbpedia_data_dir + 'labels_en.ttl']
for label_file in label_files:
    with open(label_file, encoding='utf-8') as fin:
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue
            subj, pred, obj = parse_dbpedia_line(l)
            labels[subj] = obj


category_type_label_map = {}
for subj in filtered_categories:
    cat = filtered_categories[subj]
    label = ''
    if cat in labels:
        label = labels[cat]
    else:
        label = cat[1:-1]
        label = label[label.rfind('/') + 1:]
        label = label[label.rfind(':') + 1:]
        label = label[label.rfind('#') + 1:]
        label = label.replace('_', ' ')

    match_result = re.match( r'Q[0-9]+', label)
    if match_result == None or match_result.group() != label:
        category_type_label_map[subj] = label
    else:
        print(subj + ' ' + cat)


category_type_label_map_file = dbpedia_data_dir + 'category_type_label_map.json'
with open(category_type_label_map_file, encoding='utf-8', mode='w') as fout:
    json.dump(category_type_label_map, fout, indent=2)

'''

core_dbpedia_dir = 'D:/Research/kbqa/data/DBPedia/core/'
core_dbpedia_files = [f for f in listdir(core_dbpedia_dir) if
                         isfile(join(core_dbpedia_dir, f)) and f.endswith('.ttl')]
obj_type_map = {}
all_possible_obj_types = set()
for file in core_dbpedia_files:
    with open(core_dbpedia_dir + file, encoding='utf-8') as fin:
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue
            subj, pred, (obj, obj_type) = parse_dbpedia_line_with_obj_type(l)
            if obj_type != 'entity':
                if obj not in obj_type_map:
                    obj_type_map[obj] = set()
                obj_type_map[obj].add(obj_type)
                all_possible_obj_types.add(obj_type)


for obj in obj_type_map:
    obj_type_map[obj] = list(obj_type_map[obj])
obj_type_map_file = dbpedia_data_dir + 'obj_type_map.json'
with open(obj_type_map_file, encoding='utf-8',mode='w') as fout:
    json.dump(obj_type_map,fout,indent=2)

all_possible_obj_type_file = dbpedia_data_dir + 'obj_type_all.txt'
with open(all_possible_obj_type_file, encoding='utf-8',mode='w')as fout:
    for item in all_possible_obj_types:
        fout.write(item + '\n')
'''