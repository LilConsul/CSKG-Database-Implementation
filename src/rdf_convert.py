import csv
import re
import gzip
from measure_time import measure_time

csv.field_size_limit(2**31 - 1)


def sanitize_id(id_string):
    sanitized = re.sub(r'[/\'"]', "_", id_string)
    return re.sub(r"[^a-zA-Z0-9_-]", "_", sanitized)


def escape_string(s):
    if not s:
        return ""
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


@measure_time
def convert_tsv_to_rdf_gzip(input_file, output_file, batch_size=250_000):
    print(f"Converting {input_file} to RDF format (gzipped)...")

    nodes = set()
    relationships = set()
    id_mapping = {}

    with (
        gzip.open(input_file, "rt", encoding="utf-8") as tsv_file,
        gzip.open(output_file, "wt", encoding="utf-8") as rdf_file,
    ):
        reader = csv.reader(tsv_file, delimiter="\t")
        next(reader, None)  # Skip header

        batch = []
        count = 0

        for row in reader:
            if len(row) < 10:
                continue

            row_id, node1_id, relation, node2_id = row[:4]
            node1_label = escape_string(row[4]) if len(row) > 4 else ""
            node2_label = escape_string(row[5]) if len(row) > 5 else ""
            relation_label = escape_string(row[6]) if len(row) > 6 else ""

            sanitized_node1_id = id_mapping.setdefault(node1_id, sanitize_id(node1_id))
            sanitized_node2_id = id_mapping.setdefault(node2_id, sanitize_id(node2_id))

            escaped_node1_id = escape_string(node1_id)
            escaped_node2_id = escape_string(node2_id)
            escaped_relation = escape_string(relation)

            if node1_id not in nodes:
                nodes.add(node1_id)
                batch.append(f'_:{sanitized_node1_id} <id> "{escaped_node1_id}" .')
                batch.append(f'_:{sanitized_node1_id} <label> "{node1_label}" .')

            if node2_id not in nodes:
                nodes.add(node2_id)
                batch.append(f'_:{sanitized_node2_id} <id> "{escaped_node2_id}" .')
                batch.append(f'_:{sanitized_node2_id} <label> "{node2_label}" .')

            rel_key = f"{node1_id}-{relation}-{node2_id}"
            if rel_key not in relationships:
                relationships.add(rel_key)
                batch.append(
                    f'_:{sanitized_node1_id} <to> _:{sanitized_node2_id} (id="{escaped_relation}", label="{relation_label}") .'
                )

            count += 1
            if count % batch_size == 0:
                rdf_file.write("\n".join(batch) + "\n")
                batch.clear()
                print(
                    f"Processed {count:,} records. Nodes: {len(nodes):,}, Relationships: {len(relationships):,}"
                )

        if batch:
            rdf_file.write("\n".join(batch) + "\n")

    print(
        f"Conversion complete. Total nodes: {len(nodes):,}, Total relationships: {len(relationships):,}"
    )
    print(f"RDF file saved to {output_file}")


if __name__ == "__main__":
    convert_tsv_to_rdf_gzip(
        input_file="../data/cskg.tsv.gz", output_file="../data/data.rdf.gz"
    )
