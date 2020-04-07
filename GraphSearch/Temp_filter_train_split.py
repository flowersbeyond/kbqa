import json


training_data_dir = './data/core_chain/trainingdata/'

train_id_list_file = training_data_dir + '/train_idlist.json'

train_data = training_data_dir + '10zeroshot/top2000/train_data_all.jsonl'


filtered = training_data_dir + '10zeroshot/top2000/train_data.jsonl'

idlist = []
with open(train_id_list_file, encoding='utf-8') as fin:
    idlist = json.load(fin)

with open(train_data, encoding='utf-8') as fin, open(filtered, encoding='utf-8', mode='w')as fout:
    for l in fin:
        item = json.loads(l)
        if item['id'] in idlist:
            fout.write(l)
