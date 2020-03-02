import xmltodict
from sparql_check.DBPedia1610Checker import DBPediaResultChecker

class QALD4Checker(DBPediaResultChecker):

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = xmltodict.parse(fd.read())
            data = data['dataset']['question']
            for q in data:
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

    qald_4_data_dir = './data/QALD/4/data/'
    qald_4_train = qald_4_data_dir + 'qald-4_multilingual_train_withanswers.xml'
    qald_4_train_db1610_ans = qald_4_data_dir + 'qald-4_multilingual_train_db1610ans.json'
    qald_4_test = qald_4_data_dir + 'qald-4_multilingual_test_withanswers.xml'
    qald_4_test_db1610_ans = qald_4_data_dir + 'qald-4_multilingual_test_db1610ans.json'

    qald_4_checker = QALD4Checker()
    qald_4_checker.run_gold_query(qald_4_train, qald_4_train_db1610_ans)
    qald_4_checker.run_gold_query(qald_4_test, qald_4_test_db1610_ans)

    train_result = qald_4_checker.compare_result(qald_4_train, qald_4_train_db1610_ans)

    train_detail = qald_4_data_dir + 'qald-4_multilingual_train_detail.txt'
    train_summary = qald_4_data_dir + 'qald-4_multilingual_train_summary.txt'
    qald_4_checker.print_compare_result(train_detail, train_summary, train_result)


if __name__ == '__main__':
    main()