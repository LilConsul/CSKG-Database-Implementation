import json
import csv
from measure_time import measure_time

csv.field_size_limit(2**31 - 1)

def parse_rdf_line_to_json(line, nodes, relations):
    fields = line.strip().split("\t")
    if len(fields) < 7:
        return None

    # Extracting the relevant fields from the TSV
    relation_uid, node1_id, relation, node2_id, node1_label, node2_label, relation_label = fields[:7]

    # Create Node 1 if not already present
    if node1_id not in nodes:
        nodes[node1_id] = {
            "Node.id": node1_id,
            "Node.label": node1_label,
            "dgraph.type": "Node",
        }

    # Create Node 2 if not already present
    if node2_id not in nodes:
        nodes[node2_id] = {
            "Node.id": node2_id,
            "Node.label": node2_label,
            "dgraph.type": "Node",
        }

    # Create Relation and add to a separate relations dictionary
    relation_key = relation_uid
    if relation_key not in relations:
        relations[relation_key] = {
            "uid": f"_:{relation_key}",
            "Rel.id": relation,
            "Rel.label": relation_label,
            "predecessor": {"Node.id": node1_id},
            "successor": {"Node.id": node2_id},
            "dgraph.type": "Rel",
        }

    # Ensure that the reverse relationships (from/to) are correctly populated
    if "from" not in nodes[node1_id]:
        nodes[node1_id]["from"] = []
    nodes[node1_id]["from"].append({"uid": f"_:{relation_key}"})

    if "to" not in nodes[node2_id]:
        nodes[node2_id]["to"] = []
    nodes[node2_id]["to"].append({"uid": f"_:{relation_key}"})

@measure_time
def convert_tsv_to_json(tsv_file, rdf_file, batch_size=100_000):
    print("Converting TSV to JSON...")
    nodes = {}
    relations = {}

    with (
        open(tsv_file, "r", encoding="utf-8") as tsv,
        open(rdf_file, "w", encoding="utf-8") as json_out,
    ):
        reader = csv.reader(tsv, delimiter="\t")
        next(reader, None)  # Skip the header row
        batch = []

        for i, line in enumerate(reader):
            parse_rdf_line_to_json("\t".join(line), nodes, relations)

            # After processing each batch of data, write it to the output file
            if i % batch_size == 0 and i > 0:
                batch.extend(nodes.values())
                batch.extend(relations.values())
                json.dump(batch, json_out, ensure_ascii=False)
                json_out.write("\n")
                batch.clear()
                nodes.clear()
                relations.clear()

        # If any remaining data exists, write it to the output
        if nodes or relations:
            batch.extend(nodes.values())
            batch.extend(relations.values())
            json.dump(batch, json_out, ensure_ascii=False)

# Example usage:
convert_tsv_to_json("../data/data-100k.tsv", "../data/data-100k.json")
