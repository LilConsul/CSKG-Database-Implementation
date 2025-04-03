import csv
import gzip
import re
from collections import defaultdict
from measure_time import measure_time
from functools import lru_cache

csv.field_size_limit(2**31 - 1)

ESCAPE_PATTERNS = [
    (re.compile(r"\\"), r"\\\\"),
    (re.compile(r'"'), r"\""),
    (re.compile(r"\n"), r"\n"),
    (re.compile(r"\r"), r"\r"),
    (re.compile(r"\t"), r"\t"),
]


def sanitize_id(id_string):
    return "".join(
        c if c.isascii() and (c.isalnum() or c in "_-") else "_" for c in id_string
    )


@lru_cache(maxsize=1024)
def escape_string(s):
    if not s:
        return ""
    result = s
    for pattern, replacement in ESCAPE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


def sanitize_label(node_id, label):
    """Sanitize the label by returning the most appropriate label."""
    if not label:
        return None
    if "|" not in label:
        return label

    for word in label.split("|"):
        if word in node_id:
            return word
    return None


def process_node(node_id, node_label, nodes, nodes_without_labels, batch, id_mapping):
    """Process a single node and generate RDF triples."""
    if not id_mapping[node_id]:
        id_mapping[node_id] = sanitize_id(node_id)

    sanitized_node_id = id_mapping[node_id]
    escaped_node_id = escape_string(node_id)
    node_label = sanitize_label(node_id, node_label)

    if node_id not in nodes:
        nodes.add(node_id)
        batch.append(f'_:{sanitized_node_id} <id> "{escaped_node_id}" .')
        if node_label:
            batch.append(f'_:{sanitized_node_id} <label> "{node_label}" .')
        else:
            nodes_without_labels.add(node_id)
    elif node_id in nodes_without_labels and node_label:
        batch.append(f'_:{sanitized_node_id} <label> "{node_label}" .')
        nodes_without_labels.remove(node_id)

    return sanitized_node_id


@measure_time
def convert_tsv_to_rdf_gzip(input_file, output_file, batch_size=250_000):
    print(f"Converting {input_file} to RDF format (gzipped)...")

    nodes = set()
    relationships = set()
    nodes_without_labels = set()
    id_mapping = defaultdict(lambda: None)

    def process_batch(batch, rdf_file):
        if batch:
            rdf_file.write("\n".join(batch))
            rdf_file.write("\n")
            batch.clear()

    with (
        gzip.open(input_file, "rt", encoding="utf-8") as tsv_file,
        gzip.open(output_file, "wt", encoding="utf-8", compresslevel=4) as rdf_file,
    ):
        reader = csv.reader(tsv_file, delimiter="\t")
        next(reader, None)  # Skip header

        batch = []
        count = 0

        for row in reader:
            if len(row) < 10:
                continue

            # Unpack row values
            row_id, node1_id, relation, node2_id = row[:4]
            node1_label = escape_string(row[4]) if len(row) > 4 else ""
            node2_label = escape_string(row[5]) if len(row) > 5 else ""
            relation_label = escape_string(row[6]) if len(row) > 6 else ""

            # Process both nodes
            sanitized_node1_id = process_node(
                node1_id, node1_label, nodes, nodes_without_labels, batch, id_mapping
            )
            sanitized_node2_id = process_node(
                node2_id, node2_label, nodes, nodes_without_labels, batch, id_mapping
            )

            # Process relationship
            rel_key = f"{node1_id}-{relation}-{node2_id}"
            if rel_key not in relationships:
                relationships.add(rel_key)
                batch.append(
                    f'_:{sanitized_node1_id} <to> _:{sanitized_node2_id} (id="{escape_string(relation)}", label="{relation_label}") .'
                )

            count += 1
            if count % batch_size == 0:
                process_batch(batch, rdf_file)
                print(
                    f"Processed {count:,} records. Nodes: {len(nodes):,}, Relationships: {len(relationships):,}"
                )

        process_batch(batch, rdf_file)


if __name__ == "__main__":
    convert_tsv_to_rdf_gzip(
        input_file="../data/cskg.tsv.gz", output_file="../data/data.rdf.gz"
    )
