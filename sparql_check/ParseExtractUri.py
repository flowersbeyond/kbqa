import json
import re

def extract_uri(sparql_query):
    [prefixs, query_graph] = sparql_query.split('WHERE')
    prefixs = prefixs.split()
    pre_dict = {}
    for i in range(1, len(prefixs)):
        if prefixs[i][0] == '<':
            if prefixs[i - 1][-1] == ':':
                pre_dict[prefixs[i - 1].split(':')[0]] = prefixs[i]
    final_list = []
    triple_list = re.findall(r'[<](.*?)[>]', query_graph)
    if len(triple_list) != 0:
        for uri in triple_list:
            final_list.append("<{}>".format(uri))
    for per_string in query_graph.split():
        if ":" in per_string and ("<" not in per_string and ">" not in per_string):
            for pre in pre_dict.keys():
                if pre + ":" in per_string:
                    if per_string[-1] == '.':
                        per_string = per_string[0:-1]
                    final_list.append(pre_dict[pre][0:-1] + per_string.split(":")[-1] + ">")
    return final_list


def main(input_data_file, output_data_file):
    with open(input_data_file, encoding='utf-8') as fin, open(output_data_file,encoding='utf-8') as fout:
        for l in input_data_file:
            question = json.loads(l)
            sparql_query = question['query']['sparql']
            uris = extract_uri(sparql_query)
            for uri in uris:
                fout.write(uri + '\n')


if __name__ == '__main__':
    file1 = ''
    file1_output = ''
    main(file1, file1_output)

