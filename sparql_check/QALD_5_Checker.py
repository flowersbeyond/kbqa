from collections import OrderedDict

import xmltodict
import urllib.parse
from sparql_check.DBPedia1610Checker import DBPediaResultChecker

class QALD5Checker(DBPediaResultChecker):

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = xmltodict.parse(fd.read())
            data = data['dataset']['question']
            for q in data:
                if q['@hybrid'] == 'true':
                    continue
                question = {}
                question['id'] = int(q['@id'])
                question['query'] = q['query']
                questions.append(question)

        return questions



    def isValidQuery(self, query):
        return not query == 'OUT OF SCOPE'

    def extract_old_result(self, data_file_name):
        old_results = {}

        with open(data_file_name, encoding='utf-8') as fd:
            data = xmltodict.parse(fd.read())
            data = data['dataset']['question']
            for q in data:
                if q['@hybrid'] == 'true':
                    continue

                id = int(q['@id'])
                values = []

                if id == '101':
                    print("debug")
                query = q['query']
                if query == 'OUT OF SCOPE':
                    old_results[id]={'oos':True}
                    continue

                #var_name = q['query']
                #var_type = q['@answertype']
                if q['answers'] != None:
                    answers = q['answers']['answer']
                    if isinstance(answers, list):
                        for ans in answers:
                            values.append(urllib.parse.unquote(str(ans),encoding='utf-8').lower())
                    else:
                        values.append(urllib.parse.unquote(str(answers),encoding='utf-8').lower())

                old_results[id]={ #'name':var_name, 'type':var_type,
                                    'values':values}
        return old_results


def main():
    data_dir = './data/QALD/5/data/'
    train = data_dir + 'qald-5_train.xml'
    train_db1610_ans = data_dir + 'qald-5_train_db1610ans.json'
    test = data_dir + 'qald-5_test.xml'
    test_db1610_ans = data_dir + 'qald-5_test_db1610ans.json'
    checker = QALD5Checker()
    #checker.run_gold_query(train, train_db1610_ans)
    #checker.run_gold_query(test, test_db1610_ans)

    train_result = checker.compare_result(train, train_db1610_ans)
    train_detail = data_dir + 'qald-5_train_detail.txt'
    train_summary = data_dir + 'qald-5_train_summary.txt'
    checker.print_compare_result(train_detail, train_summary, train_result)

    test_result = checker.compare_result(test, test_db1610_ans)
    test_detail = data_dir + 'qald-5_test_detail.txt'
    test_summary = data_dir + 'qald-5_test_summary.txt'
    checker.print_compare_result(test_detail, test_summary, test_result)

if __name__ == '__main__':
    main()