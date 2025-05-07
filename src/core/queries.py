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
query nodesWithMostNeighbors($offset: string) {
  var(func: has(id)) {
    successors_count as count(to)
    predecessors_count as count(~to)
    total_neighbors as math(successors_count + predecessors_count)
  }
      
  nodes_with_most_neighbors(func: has(id), orderdesc: val(total_neighbors), first: 10, offset: $offset) {
    total_neighbors: val(total_neighbors)
    id
    label
  }
}
"""

NODES_SINGLE_NEIGHBOR_QUERY = """
query nodesWithSingleNeighbor{
  var(func: has(id)) {
    successors_count as count(to)
    predecessors_count as count(~to)
    total_neighbors as math(successors_count + predecessors_count)
  }

  nodes_with_single_neighbor(func: has(id))
      @filter(eq(val(total_neighbors), 1))
      {
        amount:count(uid)
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
