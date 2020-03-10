import json
from sparql_check.dbp_check.DBPedia1610Checker import DBPediaResultChecker

class LCQuAD1Checker(DBPediaResultChecker):
    NON_EMPTY = 'NON_EMPTY'

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = json.load(fd)

            for q in data:
                question = {}
                question['id'] = int(q['_id'])
                question['query'] = q['sparql_query']
                questions.append(question)

        return questions



    def isValidQuery(self, query):
        return not query == 'OUT OF SCOPE'

    def summarize_result(self, result_file):
        summary = {}
        results = self.extract_new_result(result_file)
        for id in results:
            value = results[id]
            if 'error' in value:
                summary[id] = self.QUERY_EXE_ERROR
            else:
                value = value['values']
                if len(value) == 0:
                    summary[id] = self.NEW_ANS_EMPTY
                else:
                    summary[id] = self.NON_EMPTY
        return summary

    def print_result_summary(self, detail_file, summary_file, result_summary):
        with open(detail_file, encoding='utf-8', mode='w') as fout:
            for id in result_summary:
                if result_summary[id] != self.NON_EMPTY:
                    fout.write(json.dumps({'id':id, 'type':result_summary[id]}) + '\n')

        stats = {self.QUERY_EXE_ERROR: 0,
                 self.NEW_ANS_EMPTY: 0,
                 self.NON_EMPTY: 0}
        for id in result_summary:
            stats[result_summary[id]] += 1

        with open(summary_file, encoding='utf-8', mode='w') as fout:
            for type in stats:
                fout.write('%s\t%d\n' %(type, stats[type]))

def main(run_sparql):

    data_dir = './data/LC-QuAD-1.0/'
    train = data_dir + 'train-data.json'
    train_db1610_ans = data_dir + 'train-data_db1610ans.json'
    test = data_dir + 'test-data.json'
    test_db1610_ans = data_dir + 'test-data_db1610.json'

    checker = LCQuAD1Checker()
    if run_sparql:
        checker.run_gold_query(train, train_db1610_ans)
        checker.run_gold_query(test, test_db1610_ans)


    train_result = checker.summarize_result(train_db1610_ans)
    train_detail = data_dir + 'train-data_detail.txt'
    train_summary = data_dir + 'train-data_summary.txt'
    checker.print_result_summary(train_detail, train_summary, train_result)

    test_result = checker.summarize_result(test_db1610_ans)
    test_detail = data_dir + 'test-data_detail.txt'
    test_summary = data_dir + 'test-data_summary.txt'
    checker.print_result_summary(test_detail, test_summary, test_result)



if __name__ == '__main__':
    main(False)