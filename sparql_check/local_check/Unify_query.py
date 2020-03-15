import re


def unify_query(query):
    query = query.replace('\'', '\"')
    query = query.replace('\n', ' ')
    query = re.sub(r"\s+", " ", query)
    query = query.strip()


    # sort prefix:
    prefix = ''
    body = ''
    if query.find("ASK") != -1:
        prefix = query[0: query.find('ASK')].strip()
        body = query[query.find('ASK'):].strip()

    elif query.find("SELECT") != -1:
        prefix = query[0:query.find('SELECT')].strip()
        body = query[query.find('SELECT'):].strip()
    else:
        print("ill format query: %s" % query)

    if prefix != '' or body != '':
        prefixes = prefix.split('PREFIX')

        pre_list = []
        for pre in prefixes:
            if pre.strip() != '':
                pre_list.append(pre.strip())

        sorted_pre_list = sorted(pre_list)

        final_prefix_str = ''
        for pre in sorted_pre_list:
            final_prefix_str = final_prefix_str + 'PREFIX ' + pre.strip() + ' '
        final_prefix_str = final_prefix_str.strip()

        final_query = final_prefix_str + ' ' + body

        return final_query


    return None