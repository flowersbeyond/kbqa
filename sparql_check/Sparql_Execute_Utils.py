from SPARQLWrapper import SPARQLWrapper
import time

def query_dbpedia(query, endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat('json')

    try:
        results = sparql.query().convert()
    except:
        time.sleep(5)
        try:
            results = sparql.query().convert()
        except Exception as e:
            if str(e).find('urlopen error [WinError 10051]'):
                time.sleep(10)
                try:
                    results = sparql.query().convert()
                except Exception as e:
                    results = {'error': str(e)}

    return results