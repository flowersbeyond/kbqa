from rdflib.graph import Graph, ConjunctiveGraph
from rdflib.plugins.memory import IOMemory

import multiprocessing as mp


def load_core_data(graph, file, name):
    print(name + '\t loading...')
    graph.parse(file, format='turtle')
    print(name + '\tfinished loading.')

def load_all_core_data(data_dir, core_names):
    store = IOMemory()

    g = Graph()#store=store)
    p_list = []
    for name in core_names:
        #gi = Graph(store=store, identifier=name)

        p = mp.Process(target=load_core_data, args=(g, '%s/%s.ttl'%(data_dir, name), name))
        p_list.append(p)
        p.start()

    for p in p_list:
        p.join()

    # enumerate contexts
    #for c in g.contexts():
    #    print("-- %s " % c)


    try:
        print('begin sample query:')
        qres = g.query(
            """PREFIX res: <http://dbpedia.org/resource/> PREFIX dbp: <http://dbpedia.org/property/> 
            SELECT DISTINCT ?uri WHERE { res:Salt_Lake_City <http://dbpedia.org/ontology/timeZone> ?uri }"""
        )

        print('end sample query')
        for row in qres:
            print(row)
    except Exception as e:
        print (str(e))

    return g


if __name__ == '__main__':
    data_dir = './data/DBPedia/core8'
    core_names = [
        'labels_en',
        'category_labels_en',
        'article_categories_en',
        'instance_types_en',
        'infobox_properties_en',
        'mappingbased_literals_en',
        'mappingbased_objects_en',
        'persondata_en'
    ]

    g = load_all_core_data(data_dir, core_names)

    while(True):
        query = input('query:')

        if query.startswith('Q:'):
            query = query[2:]
            print('begin query...')
            try:
                qres = g.query(query)
                for row in qres:
                    print(row)
            except Exception as e:
                print(str(e))

            print('end query...')
        elif query == 'exit':
            break

