
import json
from sparql_check.Sparql_Execute_Utils import query_dbpedia
from tqdm import tqdm
import urllib.parse

class DBPediaResultChecker:

    RESULT_SAME = 'RESULT_SAME'
    GOLDEN_QUERY_OOS = 'GOLDEN_QUERY_OOS'
    QUERY_EXE_ERROR = 'QUERY_EXE_ERROR'
    QUERY_PARSE_ERROR = 'QUERY_PARSE_ERROR'
    NEW_ANS_EMPTY = 'NEW_ANS_EMPTY'
    NEW_ANS_INCREASED = 'NEW_ANS_INCREASED'
    NEW_ANS_DECREASED = 'NEW_ANS_DECREASED'
    OTHERS = 'OTHERS'

        #for result in results["results"]["bindings"]:
        #    print(result["label"]["value"])


    def run_gold_query(self, data_file_name, ans_output_file):
        print("Processing %s" % data_file_name)
        questions = self.extract_id_query(data_file_name)

        with open(ans_output_file,mode='w', encoding='utf-8') as fout:
            pbar = tqdm(questions)
            for q in pbar:
                query = q['query']
                if self.isValidQuery(query):
                    result = query_dbpedia(query, "http://dbpedia.org/sparql")

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
        compare_result = {}
        for id in old_result:
            left = old_result[id]
            if 'oos' in left:
                compare_result[id] = {'diff_type':self.GOLDEN_QUERY_OOS}
                continue

            assert id in new_result
            right = new_result[id]

            if 'error' in right:
                compare_result[id]={'diff_type':self.QUERY_EXE_ERROR, 'error_msg':right['error']}
                continue

            '''TODO: type & var_name have too big gaps between the older datafiles and the new response format
            we temporarily don't compare them
            
            if left['var'] != right['var'] or left['type'] != right['type']:
                compare_result.append({'id':id, 'diff_type':self.QUERY_PARSE_ERROR})
            '''
            left_values = set(left['values'])
            right_values = set(right['values'])
            left_only = left_values.difference(right_values)
            right_only = right_values.difference(left_values)
            if len(left_only) == 0 and len(right_only) == 0:
                #left & right are the same
                compare_result[id]={'diff_type': self.RESULT_SAME}
            elif len(left_only) == 0 and len(right_only) != 0:
                compare_result[id]={'diff_type': self.NEW_ANS_INCREASED}
            elif len(right_values) == 0:
                compare_result[id]={'diff_type': self.NEW_ANS_EMPTY}
            elif len(left_only) != 0 and len(right_only) == 0:
                compare_result[id]={'diff_type': self.NEW_ANS_DECREASED}
            else:
                compare_result[id]={'diff_type': self.OTHERS}

            if len(left_only) !=0:
                compare_result[id]['left_only'] = list(left_only)
            if len(right_only) != 0:
                compare_result[id]['right_only'] = list(right_only)

        return compare_result


    def extract_old_result(self, data_file_name):
        return []


    def extract_new_result(self, ans_output_file):

        new_results = {}
        with open(ans_output_file, encoding='utf-8') as fin:
            for l in fin:
                item = json.loads(l)
                id = item['id']
                values = []

                result = item['result']
                if 'error' in result:
                    new_results[id] = {'error':result['error']}
                    continue
                #TODO:
                head = result['head']
                if 'vars' in head:
                    vars = head['vars']
                    if len(vars) > 1:
                        print(str(id) + 'has more than 1 variables')
                    var_name = vars[0]
                    var_type = ''

                    bindings = result['results']['bindings']
                    for binding in bindings:
                        if len(binding) > 0:
                            var_type = binding[var_name]['type']
                            value = binding[var_name]['value']
                            values.append(urllib.parse.unquote(str(value), encoding='utf-8').lower())
                    new_results[id]= {'name':var_name, 'type':var_type, 'values':values}
                else:
                    values.append(str(result['boolean']).lower())
                    new_results[id] = {'values': values}

        return new_results



    def print_compare_result(self, detail_output_file, summary_output_file, compare_results):
        with open(detail_output_file, encoding='utf-8', mode='w') as fout:
            for id in compare_results:
                compare_results[id]['id'] = id
                if compare_results[id]['diff_type'] != self.RESULT_SAME:
                    fout.write(json.dumps(compare_results[id]) + '\n')

        summary = {self.RESULT_SAME:0,
                   self.GOLDEN_QUERY_OOS: 0,
                   self.QUERY_EXE_ERROR: 0,
                   self.NEW_ANS_EMPTY: 0,
                   self.NEW_ANS_INCREASED: 0,
                   self.NEW_ANS_DECREASED: 0,
                   self.OTHERS: 0}

        for id in compare_results:
            summary[compare_results[id]['diff_type']] += 1

        with open(summary_output_file, encoding='utf-8', mode='w') as fout:
            print(summary_output_file)
            for diff_type in summary:
                fout.write('%s\t%d\n' % (diff_type, summary[diff_type]))
                print('%s\t%d' % (diff_type, summary[diff_type]))

