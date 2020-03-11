from rdflib.graph import Graph, ConjunctiveGraph
from rdflib.plugins.memory import IOMemory


def load_all_core_data(data_dir, core_names):
    store = IOMemory()

    g = ConjunctiveGraph(store=store)

    for name in core_names:
        gi = Graph(store=store, identifier=name)
        print (name + '\t loading...')
        gi.parse('%s/%s.ttl'%(data_dir, name), format='turtle')
        print (name + '\tfinished loading.')

    # enumerate contexts
    for c in g.contexts():
        print("-- %s " % c)


    qres = g.query(
        """sparql" : "PREFIX res: <http://dbpedia.org/resource/> PREFIX dbp: <http://dbpedia.org/property/> 
        SELECT DISTINCT ?uri WHERE { res:Salt_Lake_City <http://dbpedia.org/ontology/timeZone> ?uri }"""
    )

    for row in qres:
        print(row)



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

    load_all_core_data(data_dir, core_names)
