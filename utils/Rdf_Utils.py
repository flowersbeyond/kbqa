
public_prefix = {
                    'owl': '<http://www.w3.org/2002/07/owl#>',
                    'xsd': '<http://www.w3.org/2001/XMLSchema#>',
                    'rdfs': '<http://www.w3.org/2000/01/rdf-schema#>',
                    'rdf':'<http://www.w3.org/1999/02/22-rdf-syntax-ns#>',
                    'foaf': '<http://xmlns.com/foaf/0.1/>',
                    'yago': '<http://dbpedia.org/class/yago/>',
                    'dbo': '<http://dbpedia.org/ontology/>',
                    'dbp': '<http://dbpedia.org/property/>',
                    'dbr': '<http://dbpedia.org/resource/>',
                    'dct': '<http://purl.org/dc/terms/>',
                    'dbc': '<http://dbpedia.org/resource/Category:>'
                 }

def unify_triple_item_format(item):
    item = item.strip()
    if item.startswith('<'):
        item = item[0: item.rfind('>') + 1]
    else:
        if item.endswith('>'):
            if item.rfind('^^') >= 0:
                item = item[0:item.rfind('^^')]

        else:
            if item.rfind('@') >= 0:
                item = item[0:item.rfind('@')]

        item = item.strip('\'').strip('\"').strip()

    return item
