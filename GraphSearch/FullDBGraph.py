from utils.DBPedia_Utils import parse_dbpedia_line
from tqdm import tqdm
import json
class DBGraph:
    def __init__(self):
        self.subj_index = {}
        self.obj_index = {}

    def load_full_graph(self, graph_files, debug=False):
        count = 0
        for f in graph_files:
            with open(f, encoding='utf-8') as fin:
                pbar = tqdm(fin)
                for l in pbar:
                    if l.startswith('#'):
                        continue
                    subj, pred, obj = parse_dbpedia_line(l)
                    if subj not in self.subj_index:
                        self.subj_index[subj] = {}
                    if pred not in self.subj_index[subj]:
                        self.subj_index[subj][pred] = []
                    self.subj_index[subj][pred].append(obj)

                    if obj not in self.obj_index:
                        self.obj_index[obj] = {}
                    if pred not in self.obj_index[obj]:
                        self.obj_index[obj][pred] = []
                    self.obj_index[obj][pred].append(subj)

                    count += 1
                    if debug and count >= 1000000:
                        break

    def dump_to_json(self, filename):
        with open(filename, encoding='utf-8', mode='w') as fout:
            graph_dict = {'subj_index':self.subj_index, 'obj_index':self.obj_index}
            json.dump(graph_dict, fout)

    def load_from_json(self, filename):
        with open(filename, encoding='utf-8') as fin:
            graph_dict = json.load(fin)
            self.subj_index = graph_dict['subj_index']
            self.obj_index = graph_dict['obj_index']


    def search_1hop_entities(self, entity):
        as_subj = self.search_1hop_obj(entity)
        as_obj = self.search_1hop_subj(entity)
        return as_subj, as_obj

    def search_1hop_obj(self, entity):
        if entity not in self.subj_index:
            #print(entity + 'not found as subj')
            return {}
        else:
            #print(entity + 'found as subj')
            return self.subj_index[entity]

    def search_1hop_subj(self, entity):
        if entity not in self.obj_index:
            #print(entity + 'not found as obj')
            return {}
        return self.obj_index[entity]

    def slice_1hop(self, entities, dbfiles, category_type_entities):
        sliced_tuples = set()
        sliced_lines = set()
        for f in dbfiles:
            with open(f, encoding='utf-8') as fin:
                pbar = tqdm(fin)
                for l in pbar:
                    if l.startswith('#'):
                        continue
                    subj, pred, obj = parse_dbpedia_line(l)
                    if subj in category_type_entities or obj in category_type_entities:
                        continue
                    if subj in entities or obj in entities:
                        sliced_tuples.add((subj, pred, obj))
                        sliced_lines.add(l)

        return sliced_tuples, sliced_lines

