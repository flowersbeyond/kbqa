from tqdm import tqdm

if __name__ == '__main__':
    dbpedia_data_dir = './data/DBPedia/core8/'
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

    '''
    all_rdf_ids_file = './data/QALD/all_rdf_ids.txt'

    all_rdf_ids = set()
    with open(all_rdf_ids_file, encoding='utf-8') as fin:
        for l in fin:
            all_rdf_ids.add(l.strip())
    '''

    for name in core_names:
        dbpedia_file = '%s/%s.ttl'%(dbpedia_data_dir, name)
        filter_file = '%s/%s.ttl'%(dbpedia_data_dir, name + '_filter')
        with open(dbpedia_file, encoding='utf-8') as fin, open(filter_file, encoding='utf-8', mode='w') as fout:
            predicate_set = set()
            pbar = tqdm(fin)
            for l in pbar:
                if l.startswith('#'):
                    continue

                triples = l.strip().split()
                predicate = triples[1]
                if not predicate.startswith('<') and predicate.endswith('>'):
                    print(predicate)

                if predicate not in predicate_set:
                    fout.write(l)
                    predicate_set.add(predicate)

