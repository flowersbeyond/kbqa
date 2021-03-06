from tqdm import tqdm

def unify_triple_item_format(item):
    item = item.replace('\'', '\"')
    if item.startswith('<'):
        item = item[0: item.rfind('>') + 1]
    elif item.startswith('\"'):
        pos = 1
        token = ''
        while item[pos] != '\"':
            token += item[pos]
            pos += 1
        item = token
    return item

def parse_dbpedia_line(line):

    subj = line[0:line.find('>') + 1]
    line = line[line.find('>') + 1:].strip()
    pred = ''

    if line.startswith('a '):
        pred = 'a'
        line = line[2:].strip().strip('.').strip()
        print(line)
    else:
        pred = line[0:line.find('>') + 1]
        line = line[line.find('>') + 1:].strip().strip('.').strip()

    obj = line
    obj = unify_triple_item_format(obj)

    return subj, pred, obj

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

    entity_file_map = {}
    map_file = '%s/%s.txt' % (dbpedia_data_dir, 'entity_filename_map')
    for name in core_names:
        dbpedia_file = '%s/%s.ttl'%(dbpedia_data_dir, name)

        with open(dbpedia_file, encoding='utf-8') as fin:

            pbar = tqdm(fin)
            for l in pbar:
                if l.startswith('#'):
                    continue

                subj, pred, obj = parse_dbpedia_line(l)
                if subj.startswith('<') and subj.endswith('>'):

                    if subj not in entity_file_map:
                        entity_file_map[subj] = set()
                    entity_file_map[subj].add(name)

                if obj.startswith('<') and obj.endswith('>'):
                    if obj not in entity_file_map:
                        entity_file_map[obj] = set()
                    entity_file_map[obj].add(name)

    with open(map_file, encoding='utf-8', mode='w') as fout:
        for entity in entity_file_map:
            line = entity + ':'
            for filename in entity_file_map[entity]:
                line += '\t' + filename

            fout.write(line + '\n')


