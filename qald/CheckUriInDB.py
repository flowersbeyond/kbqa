import bz2

db_files = ''

triplet_uris = set()


def load_triplets(db_file)

with open(db_files) as dir:
    for f in dir:
        single_db_uris = load_triplets(f, triplet_uris)


target_uris = ''

with open(target_uris) as fin:
    for uri in fin:
        if uri not in triplet_uris:
            print (uri.location)
