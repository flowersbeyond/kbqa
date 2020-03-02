from sparql_check.DBPedia1610Checker import DBPediaResultChecker
import json


class QALD6Checker(DBPediaResultChecker):

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = json.load(fd)
            data = data['questions']
            for q in data:
                question = {}
                question['id'] = q['id']
                if 'sparql' not in q['query']:
                    print(q['id'])
                    continue
                question['query'] = q['query']['sparql']

                questions.append(question)

        return questions

    def isValidQuery(self, query):
        return not query == 'OUT OF SCOPE'


    def extract_old_result(self, data_file_name):
        return []

def main():
    qald_6_train = './data/QALD/6/data/qald-6-train-multilingual.json'
    qald_6_train_db1610_ans = './data/QALD/6/data/qald-6-train-multilingual_db1610ans.json'
    qald_6_test = './data/QALD/6/data/qald-6-test-multilingual.json'
    qald_6_test_db1610_ans = './data/QALD/6/data/qald-6-test-multilingual_db1610ans.json'
    qald_6_checker = QALD6Checker()
    qald_6_checker.run_gold_query(qald_6_train, qald_6_train_db1610_ans)
    qald_6_checker.run_gold_query(qald_6_test, qald_6_test_db1610_ans)


if __name__ == '__main__':
    main()