from utils.DBPedia_Utils import parse_dbpedia_line
from tqdm import tqdm
from os import listdir
from os.path import isfile, join
import json
import urllib.parse

def tokenize(item):

    tokens = []
    if item.find('http:') >= 0:
        item = item[item.rfind('http:') + 5:]
    elif item.startswith('ftp://'):
        item = item[6:]
    elif item.startswith('mms://'):
        item = item[6:]
    elif item.startswith('https:/'):
        item = item[7:]
    else:
        print(item)
        return tokens

    if item.find('?') >= 0:
        item = item[0:item.find('?')]

    item = urllib.parse.unquote(item, encoding='utf-8')

    item = item.strip('/').strip('_').strip('.')
    tokens1 = item.split('/')
    for i in range(0, len(tokens1)):
        token = tokens1[i]
        if i == 0:
            if token.find(':') >= 0:
                token = token[0:token.find(':')]
            domain_tokens = token.split('.')
            tokens.extend(domain_tokens)
            continue

        if i == len(tokens1) - 1:
            if token.find('&') >=0:
                token = token[0:token.find('&')]
            last_tokens = [token]
            if token.find(':') >= 0:
                last_tokens = token.split(':')
            elif token.find('#') >= 0:
                last_tokens = token.split('#')
            elif token.find('\\') >= 0:
                last_tokens = token.split('\\')

            for t in last_tokens:
                tokens.append(clean_token(t))
            continue
        tokens.append(clean_token(token))

    final_tokens = []
    for token in tokens:
        if token != '':
            final_tokens.append(token)

    return final_tokens

def clean_token(token):
    if len(token) >= 50 and (token.find('&') >= 0 or token.find('=') >= 0):
        return ''
    if token.find('=') >= 0:
        token = token[0:token.find('=')]
    all_digit = True
    for c in token:
        if not(ord(c) >=0 and ord(c) <= 9):
            all_digit = False
            break
    if all_digit:
        return ''
    else:
        return token

def isValidEntPred(item):
    if item.startswith('<') and item.endswith('>') and (not item.startswith('<!')):
        return True
    return False
if __name__ == '__main__':

    dbpedia_file_dir = 'D:/Research/kbqa/data/DBPedia/core/'
    entity_file = 'D:/Research/kbqa/data/DBPedia/core/entities.txt'
    pred_file = 'D:/Research/kbqa/data/DBPedia/core/preds.txt'

    do_entity_pred_summary = False
    if do_entity_pred_summary:
        dbpedia_files = [f for f in listdir(dbpedia_file_dir) if
                          isfile(join(dbpedia_file_dir, f)) and f.endswith('.ttl')]

        pred_set = set()
        entity_set = set()

        for dbpedia_file in dbpedia_files:
            with open(dbpedia_file_dir + '/' + dbpedia_file, encoding='utf-8') as fin:
                pbar = tqdm(fin)
                for l in pbar:
                    subj, pred, obj = parse_dbpedia_line(l)
                    if isValidEntPred(subj):
                        entity_set.add(subj[1:-1])
                    if isValidEntPred(obj):
                        entity_set.add(obj[1:-1])
                    if isValidEntPred(pred):
                        pred_set.add(pred[1:-1])


        with open(entity_file, encoding='utf-8', mode='w') as fout:
            for ent in entity_set:
                fout.write(ent + '\n')
        with open(pred_file, encoding='utf-8', mode='w') as fout:
            for pred in pred_set:
                fout.write(pred + '\n')


    do_gen_idf_score = True
    if do_gen_idf_score:
        entity_list = []
        with open(entity_file, encoding='utf-8') as fin:
            for l in fin:
                entity_list.append(l.strip())
        df = {}
        pbar = tqdm(entity_list)
        for entity in pbar:
            tokens = set(tokenize(entity))
            for token in tokens:
                if token not in df:
                    df[token] = 1
                else:
                    df[token] += 1

        entity_df_file = 'D:/Research/kbqa/data/DBPedia/core/entity_token_df.json'
        with open(entity_df_file, encoding='utf-8', mode='w') as fout:
            json.dump(df, fout)

        pred_list = []
        with open(pred_file, encoding='utf-8') as fin:
            for l in fin:
                pred_list.append(l.strip())
        df = {}
        pbar = tqdm(pred_list)
        for pred in pbar:
            tokens = set(tokenize(pred))
            for token in tokens:
                if token not in df:
                    df[token] = 1
                else:
                    df[token] += 1

        pred_df_file = 'D:/Research/kbqa/data/DBPedia/core/pred_token_df.json'
        with open(pred_df_file, encoding='utf-8', mode='w') as fout:
            json.dump(df, fout)
