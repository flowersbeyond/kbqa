import csv

from sparql_check.dbp_check.DBPedia1610Checker import DBPediaResultChecker
import json
import urllib.parse


class QALD_7LS_Checker(DBPediaResultChecker):

    NON_EMPTY = 'NON_EMPTY'

    def extract_id_query(self, data_file_name):

        questions = []

        with open(data_file_name, encoding='utf-8') as fd:
            data = json.load(fd)
            for q in data:
                question = {}
                question['id'] = int(q['id'])
                question['query'] = q['sparql']

                questions.append(question)

        return questions

    def isValidQuery(self, query):
        return not query == 'OUT OF SCOPE'




    def load_test(self, data_file):
        data = []
        with open(data_file, encoding='utf-8') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';')
            for row in spamreader:

                question = {'temp_id': int(row[0]), 'question': row[1], 'sparql': row[2]}
                data.append(question)

        return data
    def sample(self, all_data, output_file, sample_stats_file):
        sample= []
        temp_id_map = {}
        for question in all_data:
            temp_id = question['temp_id']
            if temp_id not in temp_id_map:
                temp_id_map[temp_id] = 0

            if temp_id_map[temp_id] < 20:
                question['id'] = len(sample)
                sample.append(question)

            temp_id_map[temp_id] += 1

        with open(output_file, encoding='utf-8', mode='w') as fout:
            json.dump(sample, fout)
        with open(sample_stats_file, encoding='utf-8', mode='w') as fout:
            for temp_id in temp_id_map:
                fout.write('%s\t%d\n' % (temp_id , temp_id_map[temp_id]))




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
        data_dir = './data/QALD/7/data/large-scale/'

        checker = QALD_7LS_Checker()

        #all_tests = []
        #for i in range(1, 4):
        #    all_tests.extend(checker.load_test(data_dir + '/qald-7-test-largescale_part%d.csv' % i))

        sample_file = data_dir + '/qald-7-test-largescale-sample.json'
        sample_stats = data_dir + '/qald-7-test-largescale-sample_stats.txt'
        #checker.sample(all_tests, sample_file, sample_stats)

        sample_db1610_ans = data_dir + '/qald-7-test-largescale-sample_db1610ans.json'

        if run_sparql:
            checker.run_gold_query(sample_file, sample_db1610_ans)

        sample_result = checker.summarize_result(sample_db1610_ans)
        detail = data_dir + 'qald-7-test-largescale-sample_detail.txt'
        summary = data_dir + 'qald-7-test-largescale-sample_summary.txt'
        checker.print_result_summary(detail, summary, sample_result)


if __name__ == '__main__':
    main(False)