from tqdm import tqdm
from os import listdir
from os.path import isfile, join
from multiprocessing import Process
import bz2

def parse_dbpedia_line(line):

    subj = line[0:line.find('>') + 1]
    line = line[line.find('>') + 1:].strip()
    pred = ''

    if line.startswith('a '):
        pred = 'a'
        line = line[2:].strip().strip('.').strip()
        print(line)
    else:
        pred = line[0:line.find('>') + 1]
        line = line[line.find('>') + 1:].strip().strip('.').strip()

    obj = line
    obj = unify_triple_item_format(obj)

    return subj, pred, obj


def unify_triple_item_format(item):
    item = item.replace('\'', '\"')
    if item.startswith('<'):
        item = item[0: item.rfind('>') + 1]
    elif item.startswith('\"'):
        pos = 1
        token = ''
        while item[pos] != '\"':
            token += item[pos]
            pos += 1
        item = token
    return item


def parse_dbpedia_bz2_file(dbpedia_file, predicate_set_file):

    with bz2.open(dbpedia_file, mode='rt', encoding='utf-8') as fin, open(predicate_set_file, encoding='utf-8', mode='w') as fout:
        #predicate_set = set()
        count = 0
        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue

            subj, pred, obj = parse_dbpedia_line(l)

            if not (pred.startswith('<') and pred.endswith('>')):
                print(dbpedia_file + ':\t' + l)
                break
            if l.find('> a ') != -1:
                print(dbpedia_file + ':\t' + l)
                count += 1
                if count >= 20:
                    break

            #if pred not in predicate_set:
            #    fout.write(l)
            #    predicate_set.add(pred)

def parse_multiple_bz2_file(data_dir, sec):
    for name in sec:
        dbpedia_file = '%s/%s'%(data_dir, name)
        predicate_set_file = '%s/%s.txt'%(data_dir, name + '_predicate_set')
        parse_dbpedia_bz2_file(dbpedia_file,predicate_set_file)






if __name__ == '__main__':
    full_dbpedia_data_dir = './data/DBPedia/EN1610/'

    onlyfiles = [f for f in listdir(full_dbpedia_data_dir) if isfile(join(full_dbpedia_data_dir, f)) and f.endswith('.bz2')]

    file_sections = []
    for i in range( 0, 7):
        file_sections.append([])

    for i in range(0, len(onlyfiles)):
        file_sections[i % 7].append(onlyfiles[i])

    for sec in file_sections:
        p = Process(target=parse_multiple_bz2_file, args=(full_dbpedia_data_dir, sec))
        p.start()