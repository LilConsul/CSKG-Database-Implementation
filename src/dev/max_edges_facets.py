import gzip
from collections import defaultdict

# Map from node → list of (relation_id_or_label, <;> count)
node_relations = defaultdict(list)

with gzip.open("../../data/data.rdf.gz", "rt", encoding="utf-8") as f:
    for line in f:
        if "(" not in line:
            continue  # Skip lines with no facets

        parts = line.strip().split("(", 1)
        triple = parts[0].strip()  # RDF triple like _:x <to> _:y
        facet_part = parts[1].rsplit(")", 1)[0]

        for key in ("id=", "label="):
            start = facet_part.find(key)
            if start != -1:
                start += len(key)
                quote_char = facet_part[start]
                if quote_char in ('"', "'"):
                    end = facet_part.find(quote_char, start + 1)
                    if end != -1:
                        value = facet_part[start + 1:end]
                        count = value.count("<;>")
                        if count > 0:
                            node_relations[triple].append((value, count))
                break  # Prefer id= over label= if both present

# Flatten results and sort by number of <;> separators
flat = []
for node, entries in node_relations.items():
    for value, count in entries:
        flat.append((node, value, count))

sorted_results = sorted(flat, key=lambda x: -x[2])

print("Top node + relation values by number of <;> separators:")
for node, value, count in sorted_results[:10]:
    print(f"Node: {node}\n  Relation ID/Label: {value} → {count} <;> separators\n")
