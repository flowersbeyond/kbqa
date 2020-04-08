from utils.Rdf_Utils import unify_triple_item_format
from dateutil.parser import parse


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

def parse_dbpedia_line_with_obj_type(line):

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
    obj,type = parse_obj_type(obj)
    if type != None:
        obj = obj + '###' + type

    return subj, pred, obj

def parse_obj_type(item):
    item = item.strip()
    type = None
    if item.startswith('<'):
        obj = item[0: item.find('>') + 1]

    else:
        if item.endswith('>'):
            if item.rfind('^^') >= 0:
                obj = item[0:item.rfind('^^')]
                type = item[item.rfind('^^') + 2:].strip()
                if type.startswith('<http://dbpedia.org/datatype/'):
                    type = type[type.rfind('/') + 1: -1]
                elif type == '<http://www.w3.org/2001/XMLSchema#anyURI>':
                    type = 'URI'
                elif type == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>':
                    type = 'string'
                elif type in ['<http://www.w3.org/2001/XMLSchema#date>','<http://www.w3.org/2001/XMLSchema#gMonthDay>',
                              '<http://www.w3.org/2001/XMLSchema#gYear>' ,'<http://www.w3.org/2001/XMLSchema#gYearMonth>']:
                    type = 'time'
                elif type in ['<http://www.w3.org/2001/XMLSchema#double>', '<http://www.w3.org/2001/XMLSchema#float>',
                              '<http://www.w3.org/2001/XMLSchema#integer>','<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>',
                              '<http://www.w3.org/2001/XMLSchema#positiveInteger>']:
                    type = 'number'
            else:
                print(item)

        else:
            if item.rfind('@') >= 0:
                obj = item[0:item.rfind('@')]


        obj = item.strip('\'').strip('\"').strip()
        if type == None:
            if all_number(obj):
                type = 'number'
            elif is_date(obj):
                type = 'time'
            else:
                type = 'string'

    return obj, type


def all_number(obj):
    for c in obj:
        if c not in ['0','1','2','3','4','5','6','7','8','9',',','.','+','-']:
            return False
    return True


def is_date(string):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string)
        return True

    except:
        return False
