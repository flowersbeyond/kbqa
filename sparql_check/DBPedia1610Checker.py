from SPARQLWrapper import SPARQLWrapper
import json

class DBPediaResultChecker:

    RESULT_SAME = 'RESULT_SAME'
    GOLDEN_QUERY_OOS = 'GOLDEN_QUERY_OOS'
    GOLDEN_QUERY_MISSING = 'GOLDEN_QUERY_MISSING'
    QUERY_EXE_ERROR = 'QUERY_EXE_ERROR'
    QUERY_PARSE_ERROR = 'QUERY_PARSE_ERROR'
    NEW_ANS_EMPTY = 'NEW_ANS_EMPTY'
    NEW_ANS_INCREASED = 'NEW_ANS_INCREASED'
    OTHERS = 'OTHERS'


    def query_dbpedia(self, query):
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        sparql.setQuery(query)
        sparql.setReturnFormat('json')
        try:
            results = sparql.query().convert()
        except Exception as e:
            results = {'error':str(e)}

        return results

        #for result in results["results"]["bindings"]:
        #    print(result["label"]["value"])


    def run_gold_query(self, data_file_name, ans_output_file):
        questions = self.extract_id_query(data_file_name)

        with open(ans_output_file,mode='w', encoding='utf-8') as fout:
            for q in questions:
                query = q['query']
                if self.isValidQuery(query):
                    result = self.query_dbpedia(query)

                    result_dict = {}
                    result_dict['id'] = q['id']
                    result_dict['result'] = result

                    fout.write(json.dumps(result_dict).strip() + '\n')


    def extract_id_query(self, data_file_name):
        return []


    def isValidQuery(self, query):
        return True

    def compare_result(self, data_file_name, ans_output_file):
        old_result = self.extract_old_result(data_file_name)
        new_result = self.extract_new_result(ans_output_file)
        #should follow format: {'id': id, 'diff_type': diff_type}
        compare_result = []
        for id in old_result:
            left = old_result[id]
            if left['oos']:
                compare_result.append({'id':id, 'diff_type':self.GOLDEN_QUERY_OOS})
                continue
            if left['empty']:
                compare_result.append({'id':id, 'diff_type':self.GOLDEN_QUERY_MISSING})
                continue

            assert id in new_result
            right = new_result[id]

            if right['error']:
                compare_result.append({'id':id, 'diff_type':self.QUERY_EXE_ERROR})
                continue

            if right['empty']:
                compare_result.append({'id':id, 'diff_type':self.NEW_ANS_EMPTY})

            if left['vars'] != right['vars'] or left['type'] != right['type']:
                compare_result.append({'id':id, 'diff_type':self.QUERY_PARSE_ERROR})

            left_values = set(left['values'])
            right_values = set(right['values'])
            if left_values.issubset(right_values):
                if len(left_values) < len(right_values):
                    compare_result.append({'id':id, 'diff_type':self.NEW_ANS_INCREASED})
                else:
                    compare_result.append({'id': id, 'diff_type': self.RESULT_SAME})
            else:
                compare_result.append({'id': id, 'diff_type': self.OTHERS})

        return compare_result


    def extract_old_result(self, data_file_name):
        return []


    def extract_new_result(self, ans_output_file):
        '''TODO
        new_results = {}
        with open(ans_output_file, encoding='utf-8') as fin:
            for l in fin:
                item = json.loads(l)
                id = item['id']
                result = item['result']

                if 'error' in result:
                    new_results[id] = {'error':True}
                    continue
                #TODO:
                var_name = result['head']['vars'][0]
                values = []
                bindings = result['results']['bindings']
                for binding in bindings:
                    value = binding[var_name]['type']



        '''
        return []



    def print_compare_result(self, detail_output_file, summary_output_file, compare_results):
        with open(detail_output_file, encoding='utf-8', mode='w') as fout:
            for result in compare_results:
                fout.write('%s\t%s\n' % (result['id'], result['diff_type']))

        summary = {self.RESULT_SAME:0, self.GOLDEN_QUERY_MISSING:0, self.QUERY_EXE_ERROR:0, self.NEW_ANS_EMPTY:0, self.NEW_ANS_INCREASED:0, self.OTHERS:0}
        for result in compare_results:
            summary[result['diff_type']] += 1
        with open(summary_output_file, encoding='utf-8', mode='w') as fout:
            for diff_type in summary:
                fout.write('%s\t%d\n' % (diff_type, summary[diff_type]))

