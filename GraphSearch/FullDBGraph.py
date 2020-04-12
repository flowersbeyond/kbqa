from utils.DBPedia_Utils import parse_dbpedia_line_with_obj_type, parse_dbpedia_line
from tqdm import tqdm
import json
class DBGraph:
    def __init__(self):
        self.subj_index = {}
        self.obj_index = {}
        self.obj_value_type_index = {}
        self.category_type_name_map = {}
        self.subj_obj_name_map = {}



    def load_full_graph(self, graph_files, category_type_map_file, debug=False):
        count = 0
        for f in graph_files:
            with open(f, encoding='utf-8') as fin:
                pbar = tqdm(fin)
                for l in pbar:
                    if l.startswith('#'):
                        continue
                    subj, pred, obj = parse_dbpedia_line_with_obj_type(l)
                    if subj not in self.subj_index:
                        self.subj_index[subj] = {}
                    if pred not in self.subj_index[subj]:
                        self.subj_index[subj][pred] = []
                    self.subj_index[subj][pred].append(obj)

                    if obj.startswith('<') and obj.endswith('>'):
                        if obj not in self.obj_index:
                            self.obj_index[obj] = {}
                        if pred not in self.obj_index[obj]:
                            self.obj_index[obj][pred] = []
                        self.obj_index[obj][pred].append(subj)
                    else:
                        obj_value = obj[0:obj.rfind('###')]
                        obj_type = obj[obj.rfind('###') + 3:]
                        self.obj_value_type_index[obj] = [obj_value, obj_type]
                    count += 1
                    if debug and count >= 1000000:
                        break

        self.cache_category_type_map(category_type_map_file)

    def dump_to_json(self, filename):
        with open(filename, encoding='utf-8', mode='w') as fout:
            graph_dict = {'subj_index': self.subj_index,
                          'obj_index': self.obj_index,
                          'obj_type_value_index': self.obj_value_type_index,
                          'category_type_name_map': self.category_type_name_map,
                          'subj_obj_name_map': self.subj_obj_name_map}
            json.dump(graph_dict, fout,indent=2)

    def load_from_json(self, filename):
        with open(filename, encoding='utf-8') as fin:
            graph_dict = json.load(fin)
            self.subj_index = graph_dict['subj_index']
            self.obj_index = graph_dict['obj_index']
            self.obj_value_type_index = graph_dict['obj_type_value_index']
            self.category_type_name_map = graph_dict['category_type_name_map']
            self.subj_obj_name_map = graph_dict['subj_obj_name_map']

    def cache_category_type_map(self,category_type_map_file):
        category_type_map = {}
        with open(category_type_map_file, encoding='utf-8') as fin:
            category_type_map = json.load(fin)

        category_type_count = {}
        all_entities = set(self.subj_index.keys()).union(set(self.obj_index.keys()))
        for ent in all_entities:
            if ent in category_type_map:
                type_name = category_type_map[ent]
                if type_name not in category_type_count:
                    category_type_count[type_name] = 1
                else:
                    category_type_count[type_name] += 1

        for obj in self.obj_value_type_index:
            obj_type = self.obj_value_type_index[obj][1]
            if obj_type not in category_type_count:
                category_type_count[obj_type] = 1
            else:
                category_type_count[obj_type] += 1

        self.category_type_name_map = {}
        self.subj_obj_name_map = {}

        for ent in all_entities:
            if ent in category_type_map:
                type_name = category_type_map[ent]
                count = category_type_count[type_name]
                if count > 1:
                    self.category_type_name_map[ent] = type_name
                else:
                    self.subj_obj_name_map[ent] = type_name

        for obj in self.obj_value_type_index:
            obj_type = self.obj_value_type_index[obj][1]
            count = category_type_count[obj_type]
            if count > 1:
                self.category_type_name_map[obj] = obj_type
            else:
                self.subj_obj_name_map[obj] = obj_type

    '''
    def get_entity_name(self, entity):
        assert entity.startswith('<') and entity.endswith('>')
        entity = entity[1:-1]
        entity = entity[entity.rfind('/'):]
        entity = entity[entity.rfind(':'):]
        entity = entity[entity.rfind('#'):]
        entity = entity.replace('_', ' ').strip()
        return entity
    '''





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

    def slice_2hop(self, entities, dbfiles, category_type_entities):
        onehop_sliced_entities = set()
        for f in dbfiles:
            with open(f, encoding='utf-8') as fin:
                pbar = tqdm(fin)
                for l in pbar:
                    if l.startswith('#'):
                        continue
                    subj, pred, obj = parse_dbpedia_line(l)

                    if subj in entities:
                        if obj.startswith('<') and obj.endswith('>'):
                            onehop_sliced_entities.add(obj)
                    if obj in entities:
                        if subj.startswith('<') and subj.endswith('>'):
                            onehop_sliced_entities.add(subj)

        sliced_lines = set()
        for f in dbfiles:
            with open(f, encoding='utf-8') as fin:
                pbar = tqdm(fin)
                for l in pbar:
                    if l.startswith('#'):
                        continue
                    subj, pred, obj = parse_dbpedia_line(l)

                    if subj in entities or obj in entities:
                        sliced_lines.add(l)
                    elif subj in onehop_sliced_entities or obj in onehop_sliced_entities:
                        sliced_lines.add(l)

        return sliced_lines

    def debug_category_type_parsing(self, output_result_file):
        with open(output_result_file, encoding='utf-8',mode='w') as fout:
            for obj in self.obj_value_type_index:
                type = self.obj_value_type_index[obj][1]
                if type == '':
                    fout.write('obj:' + obj + '\n')
            for ent in self.category_type_name_map:
                if self.category_type_name_map[ent] == '':
                    fout.write('ent' + ent + '\n')
