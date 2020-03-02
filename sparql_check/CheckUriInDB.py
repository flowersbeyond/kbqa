import bz2
from os import listdir
from os.path import isfile, join





def extract_all_uris_inner(db_file):
    bz_file = bz2.BZ2File(db_file)
    lines = bz_file.readlines()
    uris = set()
    for l in lines:
        l_str = l.decode('utf-8')
        if not l_str.startswith('<'):
            continue
        ids = l_str.strip().rstrip('.').split()
        uris = uris.update()union(ids)

    return uris

def extract_all_uris(db_file_dir):
    triplet_uris = set()

    all_db_files = [join(db_file_dir, f) for f in listdir(db_file_dir) if isfile(join(db_file_dir, f))]
    for f in all_db_files:
        single_db_uris = extract_all_uris_inner(f)
        triplet_uris = triplet_uris.union(single_db_uris)

    return triplet_uris


def check_all_targets_inner(all_db_uris, target_uri_file, output_file):
    with open(target_uri_file, encoding='utf-8') as fin, open(output_file, encoding='utf-8', mode='w') as fout:
        for l in fin:
            if l not in all_db_uris:
                fout.write(l)


def check_all_targets(all_db_uris, target_uri_dir, output_uri_dir):
    all_target_file_names = [f for f in listdir(target_uri_dir) if isfile(join(target_uri_dir, f))]

    for f in all_target_file_names:
        check_all_targets_inner(all_db_uris, target_uri_dir + f, output_uri_dir+f)



def main():

    db_file_dir = './data/DBPedia/Test/'#EN1610/'
    db_uris = extract_all_uris(db_file_dir)
    '''
    target_uri_dir = './data/qald/temp/train_test_uri/'
    output_dir = './data/qald/temp/train_test_uri_info/'
    check_all_targets(db_uris, target_uri_dir, output_dir)
'''

if __name__ == '__main__':

    main()