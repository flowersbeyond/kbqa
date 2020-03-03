from collections import OrderedDict

from sparql_check.DBPedia1610Checker import DBPediaResultChecker
import json
import urllib.parse


class QALD6PlusChecker(DBPediaResultChecker):

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = json.load(fd)
            data = data['questions']
            for q in data:
                question = {}
                question['id'] = int(q['id'])
                if 'sparql' not in q['query']:
                    continue
                question['query'] = q['query']['sparql']

                questions.append(question)

        return questions

    def isValidQuery(self, query):
        return not query == 'OUT OF SCOPE'

    def extract_old_result(self, data_file_name):
        old_results = {}

        with open(data_file_name, encoding='utf-8') as fd:
            data = json.load(fd)
            data = data['questions']
            for q in data:
                id = int(q['id'])

                values = []
                if len(q['query'])== 0:#'sparql' not in q['query']:
                    old_results[id] = {'oos': True}
                    continue

                answers = q['answers']
                for ans in answers:
                    head = ans['head']
                    if 'vars' in head:
                        if len(head['vars'])> 1:
                            print("debug:%d has more than 1 vars" % id)

                        var_name = ''
                        if len(head['vars']) == 0:
                            if len(ans['results']['bindings']) != 0:
                                print("debug " + str(id) + " no var name, but return more than 0 values")
                        else:
                            var_name = head['vars'][0]

                        if len(ans['results']) != 0:
                            bindings = ans['results']['bindings']
                            for binding in bindings:
                                for key in binding:
                                    value = binding[key]['value']
                                values.append(urllib.parse.unquote(str(value), encoding='utf-8').lower())
                    else:
                        values.append(str(ans['boolean']).lower())

                old_results[id] = {  # 'name':var_name, 'type':var_type,
                    'values': values}
        return old_results

def main(run_sparql):
    for i in [6,7,8,9]:
        data_dir = './data/QALD/%d/data/' % i
        train = data_dir + '/qald-%d-train-multilingual.json' % i
        train_db1610_ans = data_dir + '/qald-%d-train-multilingual_db1610ans.json' % i
        test = data_dir + '/qald-%d-test-multilingual.json' %i
        test_db1610_ans = data_dir + '/qald-%d-test-multilingual_db1610ans.json' % i
        checker = QALD6PlusChecker()
        if run_sparql:
            checker.run_gold_query(train, train_db1610_ans)
            checker.run_gold_query(test, test_db1610_ans)

        train_result = checker.compare_result(train, train_db1610_ans)
        train_detail = data_dir + '/qald-%d-train-multilingual_detail.txt' % i
        train_summary = data_dir + '/qald-%d-train-multilingual_summary.txt' % i
        checker.print_compare_result(train_detail, train_summary, train_result)

        test_result = checker.compare_result(test, test_db1610_ans)
        test_detail = data_dir + '/qald-%d-test-multilingual_detail.txt' % i
        test_summary = data_dir + '/qald-%d-test-multilingual_summary.txt' % i
        checker.print_compare_result(test_detail, test_summary, test_result)


if __name__ == '__main__':
    main(True)