from utils.DBPedia_Utils import parse_dbpedia_line
class DBGraph:
    def __init__(self):
        self.subj_index = {}
        self.obj_index = {}

    def load_full_graph(self, graph_files):
        for f in graph_files:
            with open(f, encoding='utf-8') as fin:
                for l in fin:
                    subj, pred, obj = parse_dbpedia_line(l)
                    if subj not in self.subj_index:
                        self.subj_index[subj] = {}
                    if pred not in self.subj_index[subj]:
                        self.subj_index[subj][pred] = []
                    self.subj_index[subj][pred].append(obj)

                    if obj not in self.obj_index:
                        self.obj_index[obj] = {}
                    if pred not in self.obj_index[obj]:
                        self.obj_index[pred] = []
                    self.obj_index[obj][pred].append(subj)


    def search_1hop_entities(self, entity):
        as_subj = self.search_1hop_obj(entity)
        as_obj = self.search_1hop_subj(entity)
        return as_subj, as_obj

    def search_1hop_obj(self, entity):
        return self.subj_index[entity]

    def search_1hop_subj(self, entity):
        return self.obj_index[entity]
