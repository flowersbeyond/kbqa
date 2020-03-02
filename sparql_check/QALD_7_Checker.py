from sparql_check.DBPedia1610Checker import DBPediaResultChecker
import json


class QALD7Checker(DBPediaResultChecker):

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = json.load(fd)
            data = data['questions']
            for q in data:
                question = {}
                question['id'] = q['id']
                question['query'] = q['query']['sparql']
                questions.append(question)

        return questions

    def isValidQuery(self, query):
        return True

    def extract_old_result(self, data_file_name):
        return []

def main():
    qald_7_train = './data/QALD/7/data/qald-7-train-multilingual.json'
    qald_7_train_db1610_ans = './data/QALD/7/data/qald-7-train-multilingual_db1610ans.json'
    qald_7_test = './data/QALD/7/data/qald-7-test-multilingual.json'
    qald_7_test_db1610_ans = './data/QALD/7/data/qald-7-test-multilingual_db1610ans.json'
    qald_7_checker = QALD7Checker()
    qald_7_checker.run_gold_query(qald_7_train, qald_7_train_db1610_ans)
    qald_7_checker.run_gold_query(qald_7_test, qald_7_test_db1610_ans)


if __name__ == '__main__':
    main()