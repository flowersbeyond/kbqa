from os import listdir
from os.path import isfile, join

dbpedia_root_dir = 'D:/Research/kbqa/data/DBPedia/'
dbpedia_slices_dir = dbpedia_root_dir + '/slices/'

core = dbpedia_slices_dir + 'core/'
extend = dbpedia_slices_dir + 'extend/'
minicore = dbpedia_slices_dir + 'minicore/'
configs = {'minicore':minicore,'core': core, 'extend': extend}


total_distinct_lines = set()
for config_name in configs:
    config_dir = configs[config_name]

    dbfile_names = [f[0: -4] for f in listdir(config_dir) if
                    isfile(join(config_dir, f)) and f.endswith('.ttl')]

    config_distinct_lines = set()
    for name in dbfile_names:
        filtered_dbfile = config_dir + name + '.ttl'

        with open(filtered_dbfile, encoding='utf-8') as fin:
            for l in fin:
                if l.strip() not in config_distinct_lines:
                    config_distinct_lines.add(l.strip())

    total_distinct_lines.update(config_distinct_lines)

    config_merge_dbfile = dbpedia_root_dir + 'slices/merge/' + config_name + '.ttl'
    with open(config_merge_dbfile, encoding='utf-8', mode='w') as fout:
        for l in total_distinct_lines:
            fout.write(l + '\n')

