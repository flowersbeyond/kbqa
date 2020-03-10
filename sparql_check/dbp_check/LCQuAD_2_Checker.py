from collections import OrderedDict
import urllib.parse
import json
from sparql_check.dbp_check.DBPedia1610Checker import DBPediaResultChecker

class LCQuAD2Checker(DBPediaResultChecker):

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = json.load(fd)

            for q in data:
                question = {}
                question['id'] = int(q['uid'])
                question['query'] = q['sparql_dbpedia18']
                questions.append(question)

        return questions



    def isValidQuery(self, query):
        return not query == 'OUT OF SCOPE'


def main(run_sparql):

    data_dir = './data/LC-QuAD-2.0/'
    train = data_dir + 'train.json'
    train_db1610_ans = data_dir + 'train_db1610ans.json'
    test = data_dir + 'test-data.json'
    test_db1610_ans = data_dir + 'test_db1610.json'


    checker = LCQuAD2Checker()
    if run_sparql:
        #split train to 5 splits:
        '''
        with open(train, encoding='utf-8') as f:
            questions = json.load(f)
            for i in range(0, 5):
                train_i = data_dir + 'train_%d.json' % i
                with open(train_i, encoding='utf-8', mode='w') as fi:
                    slice_i = questions[i * 5000: min(len(questions), (i + 1) * 5000)]
                    json.dump(slice_i, fi)
        '''
        train_splits = []
        train_splits_ans = []
        for i in range(0, 5):
            train_splits.append(data_dir + 'train_%d.json' % i)
            train_splits_ans.append(data_dir + 'train_%d_db1610ans.json'% i)


        checker.run_gold_query(train_splits[0], train_splits_ans[0])
            
        #checker.run_gold_query(train, train_db1610_ans)
        #checker.run_gold_query(test, test_db1610_ans)

    '''
    train_result = checker.summarize_result(train_db1610_ans)
    train_detail = data_dir + 'train_detail.txt'
    train_summary = data_dir + 'train_summary.txt'
    checker.print_result_summary(train_detail, train_summary, train_result)

    test_result = checker.summarize_result(test_db1610_ans)
    test_detail = data_dir + 'test_detail.txt'
    test_summary = data_dir + 'test_summary.txt'
    checker.print_result_summary(test_detail, test_summary, test_result)
    '''


if __name__ == '__main__':
    main(True)