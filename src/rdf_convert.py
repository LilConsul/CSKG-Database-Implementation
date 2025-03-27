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


@measure_time
def convert_tsv_to_rdf_gzip(input_file, output_file, batch_size=250_000):
    print(f"Converting {input_file} to RDF format (gzipped)...")

    nodes = set()
    relationships = set()
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

            if not id_mapping[node1_id]:
                id_mapping[node1_id] = sanitize_id(node1_id)
            if not id_mapping[node2_id]:
                id_mapping[node2_id] = sanitize_id(node2_id)

            sanitized_node1_id = id_mapping[node1_id]
            sanitized_node2_id = id_mapping[node2_id]

            escaped_node1_id = escape_string(node1_id)
            escaped_node2_id = escape_string(node2_id)
            escaped_relation = escape_string(relation)

            if node1_id not in nodes:
                nodes.add(node1_id)
                batch.extend(
                    [
                        f'_:{sanitized_node1_id} <id> "{escaped_node1_id}" .',
                        f'_:{sanitized_node1_id} <label> "{node1_label}" .',
                    ]
                )

            if node2_id not in nodes:
                nodes.add(node2_id)
                batch.extend(
                    [
                        f'_:{sanitized_node2_id} <id> "{escaped_node2_id}" .',
                        f'_:{sanitized_node2_id} <label> "{node2_label}" .',
                    ]
                )

            rel_key = f"{node1_id}-{relation}-{node2_id}"
            if rel_key not in relationships:
                relationships.add(rel_key)
                batch.append(
                    f'_:{sanitized_node1_id} <to> _:{sanitized_node2_id} (id="{escaped_relation}", label="{relation_label}") .'
                )

            count += 1
            if count % batch_size == 0:
                process_batch(batch, rdf_file)
                print(
                    f"Processed {count:,} records. Nodes: {len(nodes):,}, Relationships: {len(relationships):,}"
                )

        process_batch(batch, rdf_file)

    print(
        f"Conversion complete. Total nodes: {len(nodes):,}, Total relationships: {len(relationships):,}"
    )
    print(f"RDF file saved to {output_file}")


if __name__ == "__main__":
    convert_tsv_to_rdf_gzip(
        input_file="../data/cskg.tsv.gz", output_file="../data/data.rdf.gz"
    )
