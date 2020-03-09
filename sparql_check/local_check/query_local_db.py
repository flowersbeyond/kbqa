from rdflib.graph import Graph, ConjunctiveGraph
from rdflib.plugins.memory import IOMemory


def load_all_core_data(data_dir, core_names):
    store = IOMemory()

    g = ConjunctiveGraph(store=store)

    for name in core_names:
        gi = Graph(store=store, identifier=name)
        gi.parse('%s/%s.ttl'%(data_dir, name), format='turtle')

    # enumerate contexts
    for c in g.contexts():
        print("-- %s " % c)


    qres = g.query(
        """SELECT DISTINCT ?aname ?bname
           WHERE {
              ?a foaf:knows ?b .
              ?a foaf:name ?aname .
              ?b foaf:name ?bname .
           }""")

    for row in qres:
        print("%s knows %s" % row)



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
