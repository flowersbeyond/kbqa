from GraphSearch.FullDBGraph import DBGraph
import json
from utils.Sparql_Execute_Utils import query_dbpedia

def search_core_chain(topic_entity, full_graph):
    dbgraph = full_graph
    one_hops = dbgraph.search_1hop_obj(topic_entity)
    one_hop_triples = []
    for pred in one_hops:
        one_hop_triples.append((topic_entity, pred, 'VAR'))

    two_hop_triples = []
    for pred in one_hops:
        triple1 = (topic_entity, pred, 'VAR1')
        objs = one_hops[pred]
        for obj in objs:
            second_hops = dbgraph.search_1hop_obj(obj)
            for pred2 in second_hops:
                triple2 = ('VAR1', pred2, 'VAR2')
                two_hop_triples.append((triple1, triple2))

    return one_hop_triples, two_hop_triples

def parse_qg_file(qg_file):
    with open(qg_file, encoding='utf-8') as fin:
        questions = json.load(fin)
        return questions


def cal_sim(str1, str2):
    return 1

def get_question_pattern(question_text, topic_entity, keywords):
    keywords = keywords.split(',')
    topic_entity = topic_entity.strip().strip('>')
    start = topic_entity.rfind('/')
    topic_entity = topic_entity[start + 1:]

    max_sim = 0
    entity_mention = ''
    for keyword in keywords:
        keyword = keyword.lower().strip()
        similarity = cal_sim(topic_entity, keyword)
        if similarity > max_sim:
            entity_mention = keyword
            max_sim = similarity

    pattern = question_text.replace(entity_mention, '<e>')
    return pattern

def translate_2_query(triple_list):
    if len(triple_list) == 1:
        triple = triple_list[0]
        return 'SELECT ?VAR WHERE {%s %s %s .}' % (triple[0], triple[1], '?VAR')

    if len(triple_list) == 2:
        triple1 = triple_list[0]
        triple2 = triple_list[1]
        return 'SELECT ?VAR WHERE {%s %s ?VAR1 . ?VAR1 %s ?VAR}' % (triple1[0], triple1[1], triple2[1])

    return ''

def get_f1_score(partial_ans, golden_ans):
    true_positive_answers = []
    for ans in partial_ans:
        if ans in golden_ans:
            true_positive_answers.append(ans)

    precision = len(true_positive_answers) / len(partial_ans)
    recall = len(true_positive_answers) / len(golden_ans)
    if precision + recall == 0:
        return 0
    f1 = 2 * precision * recall / (precision + recall)
    return f1


if __name__ == '__main__':
    graph = DBGraph()
    dbfile_dir = ''
    #dbfiles = [f[0: -4] for f in listdir(dbfile_dir) if
    #                isfile(join(dbfile_dir, f)) and f.endswith('.ttl')]
    dbfiles = []
    graph.load_full_graph(dbfiles)

    qg_file = './data/QALD/train-multilingual-4-9_in_QueryGraph_for_observing.json'
    train_examples = parse_qg_file(qg_file)




    core_chains = []
    for question in train_examples:
        id = question['id']
        #core_chain = question['core_inference_chain']
        topic_entity = question['topic_entity']
        keywords = question['keywords']
        question_text = question['question']
        question_pattern = get_question_pattern(question_text, topic_entity, keywords)

        core_chain = {'id':id, 'chains':[]}
        one_hop_triples, two_hop_triples = search_core_chain(topic_entity, graph)
        for triple in one_hop_triples:
            core_chain['chains'].append([triple[0], triple[1], triple[2]])
        for triples in two_hop_triples:
            for triple in triples:
                core_chain['chains'].append([triple[0], triple[1], triple[2]])

        core_chains.append(core_chain)

    core_chain_file = './data/QALD/core_chain/partial_queries.json'
    with open(core_chain_file, encoding='utf-8', mode='w') as fout:
        for core_chain in core_chains:
            fout.write(json.dumps(core_chain) + '\n')


    official_answers = load_official_answers('')

    training_data = []
    with open(core_chain_file, encoding='utf-8') as fin:
        for l in fin:
            item = json.loads(l)
            id = item['id']
            chains = item['chains']
            for chain in chains:
                partial_query = translate_2_query(chain)
                partial_ans = query_dbpedia(partial_query, "http://dbpedia.org/sparql")
                f1_score = get_f1_score(partial_ans, official_answers[id])
                training_data.append({'id':id, 'partial_query':partial_query, 'f1': f1_score})


    training_data_file = './data/core_chain/partial_query_f1_score.txt'
    with open(training_data_file, encoding='utf-8', mode='w') as fout:
        for item in training_data:
            fout.write(json.dumps(item) + '\n')




