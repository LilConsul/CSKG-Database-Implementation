import csv
from measure_time import measure_time

csv.field_size_limit(2 ** 31 - 1)


def escape_rdf_value(value):
    """Escape RDF values by properly handling quotes and special characters."""
    if not value:
        return ""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\t", " ").replace("|", " ").replace("\n", " ").strip()


@measure_time
def convert_tsv_to_rdf(tsv_file, rdf_file, batch_size=100000):
    node_map = {}
    relation_map = set()
    counter = 1

    with open(tsv_file, 'r', encoding='utf-8') as tsv, open(rdf_file, 'w', encoding='utf-8') as rdf:
        reader = csv.reader(tsv, delimiter='\t')
        next(reader, None)  # Skip header

        batch = []

        for i, row in enumerate(reader):
            if len(row) < 7:
                continue

            _, node1, relation, node2, node1_label, node2_label, relation_label = row[:7]

            node1_label = escape_rdf_value(node1_label)
            node2_label = escape_rdf_value(node2_label)
            relation_label = escape_rdf_value(relation_label)

            if node1 not in node_map:
                node_map[node1] = f"<0x{counter}>"
                batch.append(f'{node_map[node1]} <id> "{escape_rdf_value(node1)}" .\n')
                batch.append(f'{node_map[node1]} <label> "{node1_label}" .\n')
                counter += 1

            if node2 not in node_map:
                node_map[node2] = f"<0x{counter}>"
                batch.append(f'{node_map[node2]} <id> "{escape_rdf_value(node2)}" .\n')
                batch.append(f'{node_map[node2]} <label> "{node2_label}" .\n')
                counter += 1

            relation_key = (node1, relation, node2)
            if relation_key not in relation_map:
                relation_map.add(relation_key)
                batch.append(f'{node_map[node1]} <{escape_rdf_value(relation)}> {node_map[node2]} .\n')
                batch.append(f'{node_map[node1]} <relation_label> "{relation_label}" .\n')

            if i % batch_size == 0:
                rdf.writelines(batch)
                batch.clear()

                if len(node_map) > batch_size * 10:
                    node_map.clear()

        if batch:
            rdf.writelines(batch)


convert_tsv_to_rdf("../data/data.tsv", "../data/data.rdf")
