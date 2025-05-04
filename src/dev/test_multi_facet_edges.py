import gzip
from collections import defaultdict

# Map from (subj, pred, obj) → set of facet‐strings
edge_facets = defaultdict(set)

with gzip.open("../../data/data.rdf.gz", "rt", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split("(", 1)
        triple = parts[0].strip()  # e.g. '<uid_A> <to> <uid_B>'
        facets = ""
        if len(parts) == 2:
            # remove closing ') .'
            facets = parts[1].rsplit(")", 1)[0]
        edge_facets[triple].add(facets)

# Count how many edges have >1 facet variants
multi_facet_edges = [t for t, fs in edge_facets.items() if len(fs) > 1]
print("Total unique edges:", len(edge_facets))
print("Edges with multiple facet‐variants:", len(multi_facet_edges))

# Show a few examples:
for t in multi_facet_edges[:10]:
    print("\nEdge:", t)
    for fac in edge_facets[t]:
        print("  Facet set:", fac)
