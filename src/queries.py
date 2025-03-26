SUCCESSORS_QUERY = """
query getSuccessors($id: string) {
  successors(func: eq(id, $id)) {
    id
    label
    successors: to {
      id
      label
    }
  }
}
"""

COUNT_SUCCESSORS_QUERY = """
query countSuccessors($id: string) {
  successors(func: eq(id, $id)) {
    id
    label
    count: count(to)
  }
}
"""

PREDECESSORS_QUERY = """
query getPredecessors($id: string) {
  predecessors(func: eq(id, $id)) {
    id
    label
    predecessors: ~to {
      id
      label
    }
  }
}
"""

COUNT_PREDECESSORS_QUERY = """
query countPredecessors($id: string) {
  predecessors(func: eq(id, $id)) {
    id
    label
    count: count(~to)
  }
}
"""

NEIGHBORS_QUERY = """
query getNeighbors($id: string) {
  neighbors(func: eq(id, $id)) {
    id
    label
    successors: to {
      id
      label
    }
    predecessors: ~to {
      id
      label
    }
  }
}
"""

COUNT_NEIGHBORS_QUERY = """
query countNeighbors($id: string) {
  neighbors(func: eq(id, $id)) {
    id
    label
    successors_count AS count(to)
    predecessors_count AS count(~to)
    total_neighbors: math(successors_count + predecessors_count)
  }
}
"""

GRANDCHILDREN_QUERY = """
query getGrandchildren($id: string) {
  grandchildren(func: eq(id, $id)) @normalize{
    to {
      to {
        id:id
        label:label
      }
    }
  }
}
"""

GRANDPARENTS_QUERY = """
query getGrandparents($id: string) {
  grandparents(func: eq(id, $id)) @normalize{
    ~to {
      ~to {
        id:id
        label:label
      }
    }
  }
}
"""

TOTAL_NODES_QUERY = """
query countAllNodes {
  total(func: has(id)) {
    count(uid)
  }
}
"""

# TODO: fix query
NODES_NO_SUCCESSORS_QUERY = """
query countNodesWithoutSuccessors {
  nodes(func: has(id)) @filter(not has(to)) {
    count(uid)
  }
}
"""

# TODO: fix query
NODES_NO_PREDECESSORS_QUERY = """
query countNodesWithoutPredecessors {
  nodes(func: has(id)) @filter(not has(~to)) {
    count(uid)
  }
}
"""

# TODO: fix query
NODES_MOST_NEIGHBORS_QUERY = """
{
  var(func: has(id)) {
    successors_count as count(to)
    predecessors_count as count(~to)
    total_neighbors as math(successors_count + predecessors_count)
  }

  maxNeighborsValue as var(func: has(id)) {
    max_val as max(val(total_neighbors))
  }

  nodes_with_most_neighbors(func: has(id)) @filter(eq(val(total_neighbors), max_val)) {
    id
    total_neighbors: val(total_neighbors)
  }
}
"""

# TODO: fix query
NODES_SINGLE_NEIGHBOR_QUERY = """
{
  var(func: has(id)) {
    successors_count as count(to)
    predecessors_count as count(~to)
    total_neighbors as math(successors_count + predecessors_count)
  }

  count(func: has(id)) @filter(eq(val(total_neighbors), 1)) {
    count(uid)
  }
}
"""

RENAME_NODE_MUTATION = """

"""

SIMILAR_NODES_QUERY = """

"""

SHORTEST_PATH_QUERY = """

"""

DISTANT_SYNONYMS_QUERY = """

"""

DISTANT_ANTONYMS_QUERY = """

"""