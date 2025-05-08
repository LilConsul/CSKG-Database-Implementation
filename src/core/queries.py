IS_EXIST = """
query checkNodeExists($id: string) {
  node_exists(func: eq(id, $id)) {
    count: count(uid)
  }
}
"""

SUCCESSORS_QUERY = """
query getSuccessors($id: string) {
  successors(func: eq(id, $id)) @normalize {
    successors: to {
      id:id
      label:label
    }
  }
}
"""

COUNT_SUCCESSORS_QUERY = """
query countSuccessors($id: string) {
  successors(func: eq(id, $id)) {
    count: count(to)
  }
}
"""

PREDECESSORS_QUERY = """
query getPredecessors($id: string) {
  predecessors(func: eq(id, $id)) @normalize {
    predecessors: ~to {
      id:id
      label:label
    }
  }
}
"""

COUNT_PREDECESSORS_QUERY = """
query countPredecessors($id: string) {
  predecessors(func: eq(id, $id)) {
    count: count(~to)
  }
}
"""

NEIGHBORS_QUERY = """
query getNeighbors($id: string) {
  var(func: eq(id, $id)) {
    succ as to
    pred as ~to
  }
  
  neighbors(func: uid(succ, pred)) {
    id
    label
  }
}
"""

COUNT_NEIGHBORS_QUERY = """
query getNeighbors($id: string) {
    neighbors(func: eq(id, $id)) {
        count:unique_neighbors_count
    }
}
"""

GRANDCHILDREN_QUERY = """
query getGrandchildren($id: string) {
  var(func: eq(id, $id)) {
    to {
      to {
        unique_nodes as uid
      }
    }
  }
  
  grandchildren(func: uid(unique_nodes)) @normalize {
    id: id
    label: label
  }
}
"""

GRANDPARENTS_QUERY = """
query getGrandparents($id: string) {
  var(func: eq(id, $id)) {
    ~to {
      ~to {
        unique_parents as uid
      }
    }
  }
  
  grandparents(func: uid(unique_parents)) @normalize {
    id: id
    label: label
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

NODES_NO_SUCCESSORS_QUERY = """
query countNodesWithoutSuccessors {
  nodes(func: has(id)) @filter(not has(to)) {
    count(uid)
  }
}
"""

NODES_NO_PREDECESSORS_QUERY = """
query countNodesWithoutPredecessors {
  nodes(func: has(id)) @filter(not has(~to)) {
    count(uid)
  }
}
"""

NODES_MOST_NEIGHBORS_QUERY = """
query findNodesWithMostNeighbors {
  var(func: has(id), orderdesc: unique_neighbors_count, first: 1) {
    max_val as unique_neighbors_count
  }

  nodes_with_most_neighbors(func: eq(unique_neighbors_count, val(max_val))) {
    id
    label
    unique_neighbors_count
  }
}
"""

NODES_SINGLE_NEIGHBOR_QUERY = """
query countNodesWithSingleNeighbor {
  single_neighbor_count(func: eq(unique_neighbors_count, 1)) {
    count(uid)
  }
}
"""

SIMILAR_NODES_QUERY = """
query findSimilarNodesData($id: string) {
  node_info(func: eq(id, $id)) {
    id
    label
    to @facets(id) {
      id
      label
    ~to @facets(id) {
      id
      label
    	}
    }
    ~to @facets(id) {
      id
      label
      to @facets(id) {
     	 id
     	 label
    	}
    }
  }
}
"""

SHORTEST_PATH_QUERY = """
query shortestPath($id1: string, $id2: string) {
  A as var(func: eq(id, $id1))
  B as var(func: eq(id, $id2))

  shortestPath as shortest(from: uid(A), to: uid(B)) {
    to
    ~to
  }

  path(func: uid(shortestPath)) {
    id
    label
  }
}
"""

DISTANT_SYNONYMS_ANTONYM = """
query disantSynonymsAndAntonyms($id: string, $distance: int) {
  distant_nodes(func: eq(id, $id)) @recurse(depth: $distance, loop: false) {
    id
    label
    synonym: synonym 
    synonym~: ~synonym 
    antonym: antonym 
    antonym~: ~antonym 
  }
}
"""
