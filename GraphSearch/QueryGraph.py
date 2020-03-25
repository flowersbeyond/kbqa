class QueryGraph:

    STAGE_TOPIC_ENTITY = 'TOPIC_ENTITY'
    STAGE_CORE_CHAIN = 'CORE_CHAIN'
    STAGE_CONSTRAINT = 'CONSTRAINT'
    STAGE_AGGREGATION = 'AGGREGATION'

    def __init__(self, topic_entity):
        self.topic_entity = topic_entity
        self.stage = self.STAGE_TOPIC_ENTITY

        self.core_chain_nodes = [topic_entity]
        self.constraint_nodes = []
        self.aggr_nodes = []

        self.core_chain_edges = []
        self.constraint_edges = []
        self.aggr_edges = []

    def get_reward_value(self):
        return 0
    @classmethod
    def from_sparql(cls, sparql_query):
        return None
    def to_sparql(self):
        return ''

    def get_topic_entity(self):
        return self.topic_entity

    def get_core_chain(self):
        return []

    def extend_core_chain(self, new_edge):
        assert new_edge.s_node in self.constraint_nodes
        self.core_chain_nodes.append(new_edge.t_node)
        self.core_chain_edges.append(new_edge)

    def extend_constraint(self, new_edge):
        assert new_edge.s_node in self.core_chain_nodes
        self.constraint_edges.append(new_edge)
        self.constraint_nodes.append(new_edge.t_node)

    def extend_aggr(self, new_edge):
        assert new_edge.s_node in self.core_chain_nodes
        self.aggr_edges.append(new_edge)
        self.aggr_nodes.append(new_edge.t_node)

    def copy(self):
        return None


class QGNode:
    TOPIC_ENTITY = 'TOPIC_ENTITY'
    GROUNDED_ENTITY = 'GROUNDED_ENTITY'
    EXISTENTIAL_VARIABLE = 'EXISTENTIAL_VARIABLE'
    LAMBDA_VARIABLE = 'LAMBDA_VARIABLE'
    AGGREGATION_FUNCTION = 'AGGREGATION_FUNCTION'


    def __init__(self):
        self.entities = []
        self.edges = []

    def get_type(self):
        return 'Normal'


class QGEdge:

    def __init__(self, s_node, t_node, pred):
        self.s_node = s_node
        self.t_node = t_node
        self.pred = pred

