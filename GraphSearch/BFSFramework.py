from GraphSearch.QueryGraph import QueryGraph, QGEdge, QGNode
import GraphSearch.FullDBGraph as fulldbgraph
import heapq

EXTEND_CORE_CHAIN = 'EXTEND_CORE_CHAIN'

all_actions = [EXTEND_CORE_CHAIN]


def bfs_search(question_text, entity_mention):
    candidate_graphs = []
    ending_stage_graphs = []
    for entity in entity_mention:
        graph = QueryGraph(QGNode(entity))
        candidate_graphs.append(graph)

    heapq.heapify(candidate_graphs)
    while len(candidate_graphs) > 0:
        g = candidate_graphs.pop()
        available_actions = get_available_actions(g)

        if available_actions == []:
            ending_stage_graphs.append(g)
            continue

        # stage 1: search core chain
        if EXTEND_CORE_CHAIN in available_actions:
            tail_core_entity = g.get_tail_core_entity()
            extended_core_chain_entities = fulldbgraph.search_1hop_entities(tail_core_entity)
            for edge in extended_core_chain_entities:
                new_graph = g.extend_core_chain(edge)
                heapq.heappush(candidate_graphs, new_graph)
        '''
        # stage2: search constrains
        if g_stage == QueryGraph.STAGE_CORE_CHAIN or g_stage == QueryGraph.STAGE_CONSTRAINT:
            ##?? add all possible 
            core_entities = []
            extended_core_chain_entities = search_extentions(fullgraph, core_entities, entity_mention)
            for edge in extended_core_chain_entities:
                new_graph = g.extend_core_chain(edge)
                heapq.heappush(candidate_graphs, new_graph)
    
        # stage3: search aggregations
        if g_stage == QueryGraph.STAGE_TOPIC_ENTITY or g_stage == QueryGraph.STAGE_CORE_CHAIN:
            tail_core_entity = g.get_tail_core_entity()
            extended_core_chain_entities = search_next_hop(fullgraph, tail_core_entity)
            for edge in extended_core_chain_entities:
                new_graph = g.extend_core_chain(edge)
                heapq.heappush(candidate_graphs, new_graph)
        '''