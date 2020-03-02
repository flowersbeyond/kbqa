import xmltodict
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
                question['id'] = q['@id']
                question['query'] = q['query']
                questions.append(question)

        return questions



    def isValidQuery(self, query):
        return not query == 'OUT OF SCOPE'

    def extract_old_result(self, data_file_name):
        return []


def main():
    qald_5_train = './data/QALD/5/data/qald-5_train.xml'
    qald_5_train_db1610_ans = './data/QALD/5/data/qald-5_train_db1610ans.json'
    qald_5_test = './data/QALD/5/data/qald-5_test.xml'
    qald_5_test_db1610_ans = './data/QALD/5/data/qald-5_test_db1610ans.json'
    qald_5_checker = QALD5Checker()
    qald_5_checker.run_gold_query(qald_5_train, qald_5_train_db1610_ans)
    qald_5_checker.run_gold_query(qald_5_test, qald_5_test_db1610_ans)


if __name__ == '__main__':
    main()