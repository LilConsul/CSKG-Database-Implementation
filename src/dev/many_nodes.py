import gzip
from collections import defaultdict


def check_duplicate_id_label_annotations():
    """
    Check if any node in the RDF data has more than one annotation
    of the same type (<id> or <label>).
    """
    # Track predicate counts and values per node
    node_predicates = defaultdict(lambda: defaultdict(int))
    node_values = defaultdict(lambda: defaultdict(list))

    # Process the RDF data
    with gzip.open("../../data/data.rdf.gz", "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue  # Skip empty lines and comments

            # Extract just the triple part (before any facet)
            if "(" in line:
                triple_part = line.split("(", 1)[0].strip()
            else:
                triple_part = line

            # Parse triple: subject predicate object
            parts = triple_part.split(None, 2)
            if len(parts) < 3:
                continue  # Skip invalid lines

            subject, predicate, obj = parts

            # Clean up object - remove trailing period and whitespace
            if obj.endswith(" ."):
                obj = obj[:-2]
            elif obj.endswith("."):
                obj = obj[:-1]

            # Track only id and label predicates
            if predicate in ["<id>", "<label>"]:
                node_predicates[subject][predicate] += 1
                node_values[subject][predicate].append(obj)

    # Report nodes with duplicate annotations
    duplicate_count = 0
    duplicate_nodes = []

    for node, pred_counts in node_predicates.items():
        node_has_duplicates = False
        for pred, count in pred_counts.items():
            if count > 1:
                node_has_duplicates = True
                break

        if node_has_duplicates:
            duplicate_count += 1
            duplicate_nodes.append((node, pred_counts, node_values[node]))

    # Print results
    print(f"Found {duplicate_count} nodes with duplicate <id> or <label> annotations:")
    for node, pred_counts, values in duplicate_nodes:
        print(f"\nNode: {node}")
        for pred, count in pred_counts.items():
            if count > 1:
                print(
                    f"  {pred} appears {count} times with values: {', '.join(values[pred])}"
                )


if __name__ == "__main__":
    check_duplicate_id_label_annotations()