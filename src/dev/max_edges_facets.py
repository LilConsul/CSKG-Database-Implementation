import gzip
from collections import defaultdict

# Count how many <;> separators each relation id has
relation_separator_counts = defaultdict(int)

with gzip.open("../../data/data.rdf.gz", "rt", encoding="utf-8") as f:
    for line in f:
        if "(" not in line:
            continue  # Skip lines with no facets

        parts = line.strip().split("(", 1)
        facet_part = parts[1].rsplit(")", 1)[0]

        # Look for id="..." or label="..."
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
                            relation_separator_counts[value] = count
                break  # Prefer id= over label= if both present

# Sort and print the relation values with most <;>
sorted_relations = sorted(relation_separator_counts.items(), key=lambda x: -x[1])

print("Top relations by number of <;> separators:")
for value, count in sorted_relations[:10]:
    print(f"{value} → {count} <;> separators")
