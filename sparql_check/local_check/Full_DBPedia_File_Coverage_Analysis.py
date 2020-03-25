import json
from utils.Sparql_Utils import parse_triple_from_query_answer
from utils.DBPedia_Utils import parse_dbpedia_line
from os import listdir
from os.path import isfile, join


def extract_triple_id_map(file, idflag):

    triple_id_map = {}
    with open(file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)

            id = question['id']
            query = question['query']['sparql']
            triples = parse_triple_from_query_answer(query, question['answers'])

            for triple in triples:
                triple_tuple = (triple[0], triple[1], triple[2])
                if triple_tuple not in triple_id_map:
                    triple_id_map[triple_tuple] = set()
                triple_id_map[triple_tuple].add(idflag + '_' + str(id))

    return triple_id_map


def parse_question_source(src_str):
    #'6#4_D9#164'
    sources = src_str.split('_')
    result = []
    for src in sources:
        if src.startswith('D') or src.startswith('M'):
            result.append(src[1])
        else:
            result.append(src[0])
    return result


def get_question_triple_coverage(data_file, covered_triples):
    question_coverage_stat = {'covered':[], 'unknown':[]}
    with open(data_file, encoding='utf-8') as fin:
        for l in fin:
            question = json.loads(l)
            id = question['id']

            query = question['query']['sparql']

            triples = parse_triple_from_query_answer(query, question['answers'])

            fail = False
            unknown = False
            for triple in triples:
                triple_tuple = (triple[0], triple[1], triple[2])
                if triple_tuple not in covered_triples:
                    fail = True
                    ##debug
                    #src_ids = parse_question_source(question['merge'])
                    #if '9' in src_ids:
                    #    print(query + ':\t' + str(triple_tuple))
                    ##enddebug

                    break
                if triple[0] == 'VAR' or triple[1] == 'VAR' or triple[2] == 'VAR':
                    unknown = True

            if fail or unknown:
                question_coverage_stat['unknown'].append(id)
            else:
                question_coverage_stat['covered'].append(id)
    return question_coverage_stat



if __name__ == '__main__':

    qald_multilingual_train = './data/QALD/train-multilingual-4-9.jsonl'
    train_triple_ids = extract_triple_id_map(qald_multilingual_train, 'train')

    qald_multilingual_test = './data/QALD/test-multilingual-4-9.jsonl'
    test_triple_ids = extract_triple_id_map(qald_multilingual_test, 'test')

    all_triples_qid_map = train_triple_ids

    for triple in test_triple_ids:
        if not triple in all_triples_qid_map:
            all_triples_qid_map[triple] = test_triple_ids[triple]
        else:
            all_triples_qid_map[triple].update(test_triple_ids[triple])

    all_triples_qid_map[('VAR', 'VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>')] = set('exception')
    all_triples_qid_map[('VAR', '<http://dbpedia.org/resource/Daniel_Jurafsky>', 'VAR')] = set('exception')
    all_triples_qid_map[('<http://dbpedia.org/resource/Daniel_Jurafsky>', 'VAR', 'VAR')] = set('exception')


    dbpedia_root_dir = 'D:/Research/kbqa/data/DBPedia/'
    dbpedia_original_files_dir = dbpedia_root_dir + '/EN1610/'
    dbpedia_slices_dir = dbpedia_root_dir + '/slices/'

    minicore = dbpedia_slices_dir + 'minicore/'
    core = dbpedia_slices_dir + 'core/'
    extend = dbpedia_slices_dir + 'extend/'

    configs = {'minicore':minicore,'core':core, 'extend':extend}

    covered_triple_patterns = set()
    covered_triples = set()
    file_coverage_data = {}
    config_coverage_data = {}
    all_triples = set(all_triples_qid_map.keys())
    for config_name in configs:
        config_dir = configs[config_name]

        dbfile_names = [f[0: -4] for f in listdir(config_dir) if
                         isfile(join(config_dir, f)) and f.endswith('.ttl')]
        file_coverage_data[config_name] = {}
        config_coverage_data[config_name] = {}
        config_newly_covered_triples = set()
        config_newly_covered_triple_patterns = set()

        for name in dbfile_names:
            stat = {}

            original_name = name[0:name.find('_filter')]
            print(name + ':' + original_name)
            original_dbfile = dbpedia_original_files_dir + original_name + '.ttl'
            with open(original_dbfile, encoding='utf-8') as fin:
                lines = fin.readlines()
                # 1. each file original size
                stat['original_size'] = len(lines)

            filtered_dbfile = config_dir + name + '.ttl'

            single_file_newly_covered_triple_patterns = set()
            single_file_newly_covered_triples = set()
            with open(filtered_dbfile, encoding='utf-8') as fin:
                covered_num = 0
                for l in fin:
                    covered_num += 1
                    subj, pred, obj = parse_dbpedia_line(l)
                    possible_hits = [(subj, pred, obj), ('VAR', pred, obj), (subj, pred, 'VAR'), (subj, 'VAR', pred),
                                     ('VAR', 'VAR', obj), (subj, 'VAR', 'VAR'), ('VAR', pred, 'VAR')]

                    for hit in possible_hits:
                        if hit in all_triples and hit not in covered_triple_patterns:
                            single_file_newly_covered_triple_patterns.add(hit)
                            single_file_newly_covered_triples.add(l.strip())

            # 2. each file covered size
            stat['useful_size'] = covered_num
            # 3. each file newly covered size
            stat['newly_covered_line_size'] = len(single_file_newly_covered_triples)
            stat['newly_covered_pattern_size'] = len(single_file_newly_covered_triple_patterns)

            file_coverage_data[config_name][name] = stat

            # 4. config newly covered size
            config_newly_covered_triple_patterns.update(single_file_newly_covered_triple_patterns)
            config_newly_covered_triples.update(single_file_newly_covered_triples)

        config_coverage_data[config_name]['newly_covered_pattern_size'] = len(config_newly_covered_triple_patterns)
        config_coverage_data[config_name]['newly_covered_line_size'] = len(config_newly_covered_triples)

        covered_triple_patterns.update(config_newly_covered_triple_patterns)
        covered_triples.update(config_newly_covered_triples)


        # 5. write down config missing items:
        config_missing_triples_file = './data/QALD/%s_missing_triples.txt' % config_name
        config_missing_triples = all_triples.difference(covered_triple_patterns)
        with open(config_missing_triples_file, encoding='utf-8', mode='w') as fout:
            for triple in config_missing_triples:
                fout.write('%s\t%s\t%s\n' % (triple[0], triple[1], triple[2]))

        # 6. with this triple coverage how is the question coverage

        train_question_coverage = get_question_triple_coverage(qald_multilingual_train, covered_triple_patterns)
        test_question_coverage = get_question_triple_coverage(qald_multilingual_test, covered_triple_patterns)
        config_coverage_data[config_name]['train_question_coverage'] = train_question_coverage
        config_coverage_data[config_name]['test_question_coverage'] = test_question_coverage


    triple_coverage_stat_file = dbpedia_root_dir + '/slices/triple_coverage_summary.txt'
    with open(triple_coverage_stat_file, encoding='utf-8', mode='w') as fout:
        fout.write('config_coverage\n')
        fout.write('config_name\tnewly_covered_triples\tnewly_covered_patterns\n')
        for config_name in configs:
            fout.write('%s\t%d\t%d\n' %
                       (config_name,
                        config_coverage_data[config_name]['newly_covered_line_size'],
                        config_coverage_data[config_name]['newly_covered_pattern_size'])
                       )
        fout.write('single_file_stats\n')
        fout.write('config_name\tfile_name\toriginal_size\tcover_size\tnewly_cover_triples\tnewly_cover_patterns\n')
        for config_name in configs:
            fout.write('%s' % config_name)
            for file_name in file_coverage_data[config_name]:
                stat = file_coverage_data[config_name][file_name]
                fout.write('\t%s\t%d\t%d\t%d\t%d\n' % (
                    file_name, stat['original_size'], stat['useful_size'],
                    stat['newly_covered_line_size'],stat['newly_covered_pattern_size']))

    question_coverage_stat_file = dbpedia_root_dir + '/slices/question_coverage_stat_file.txt'
    with open(question_coverage_stat_file, encoding='utf-8', mode='w') as fout:
        fout.write('train')

        fout.write('config_name\tcovered\tunknown\n')
        for config_name in config_coverage_data:
            train_question_coverage = config_coverage_data[config_name]['train_question_coverage']
            fout.write('%s\t%d\t%d\n' % (config_name, len(train_question_coverage['covered']),
                                            len(train_question_coverage['unknown'])
                                            ))

        fout.write('test')

        fout.write('config_name\tcovered\tunknown\n')
        for config_name in config_coverage_data:
            test_question_coverage = config_coverage_data[config_name]['test_question_coverage']
            fout.write('%s\t%d\t%d\n' % (config_name, len(test_question_coverage['covered']),
                                             len(test_question_coverage['unknown'])
                                             ))



    for config_name in config_coverage_data:
        question_coverage_detail_file = './data/QALD/%s_question_coverage_%s.txt' % ('train', config_name)
        with open(question_coverage_detail_file, encoding='utf-8', mode='w') as fout:
            fout.write(json.dumps(config_coverage_data[config_name]['train_question_coverage']))

    for config_name in config_coverage_data:
        question_coverage_detail_file = './data/QALD/%s_question_coverage_%s.txt' % ('test', config_name)
        with open(question_coverage_detail_file, encoding='utf-8', mode='w') as fout:
            fout.write(json.dumps(config_coverage_data[config_name]['test_question_coverage']))
