from tqdm import tqdm
from os import listdir
from os.path import isfile, join

from sparql_check.local_check.DBPedia_Utils import parse_dbpedia_line

def extract_core_8_notcovered_triples(all_missing_triples_file):
    triples = set()
    with open(all_missing_triples_file, encoding='utf-8') as fin:
        for l in fin:
            triple = l.strip().split('\t')
            triples.add((triple[0], triple[1], triple[2]))
    return triples




def slice_single_ttl_file(filtered_dbpedia_file, slice_result_file_name, target_triples):

    covered_triples = set()
    with open(filtered_dbpedia_file, encoding='utf-8') as fin, open(slice_result_file_name, encoding='utf-8', mode='w') as fout:

        pbar = tqdm(fin)
        for l in pbar:
            if l.startswith('#'):
                continue

            subj, pred, obj = parse_dbpedia_line(l)

            possible_hits = [(subj, pred, obj), ('VAR', pred, obj), (subj, pred, 'VAR'), (subj, 'VAR', pred),
                             ('VAR', 'VAR', obj), (subj, 'VAR', 'VAR'), ('VAR', pred, 'VAR')]

            for hit in possible_hits:
                if hit in target_triples:
                    covered_triples.add(hit)
                    fout.write(l)

    return covered_triples




if __name__ == '__main__':


    core8_missing_triples = './data/QALD/all_missing_triples.txt'

    all_target_triples = extract_core_8_notcovered_triples(core8_missing_triples)


    full_dbpedia_data_dir = './data/DBPedia/extend/'

    all_filtered_files = [f for f in listdir(full_dbpedia_data_dir) if isfile(join(full_dbpedia_data_dir, f)) and f.endswith('.ttl')]


    total_covered_triples = set()
    for i in range(0, len(all_filtered_files)):
        filtered_dbfile_name = all_filtered_files[i]
        covered_triples = slice_single_ttl_file('%s/%s' % (full_dbpedia_data_dir, filtered_dbfile_name),
                              '%s/%s' % (full_dbpedia_data_dir, filtered_dbfile_name + '.strict'),
                                all_target_triples)
        total_covered_triples.update(covered_triples)

    total_uncovered = total_covered_triples.difference(total_covered_triples)

    still_not_covered_file = './data/QALD/missing_triples_with_extend_dbfiles.txt'
    with open(still_not_covered_file, mode='w', encoding='utf-8') as fout:
        for triple in total_uncovered:
            fout.write('%s\t%s\t%s\n' % (triple[0], triple[1], triple[2]))
