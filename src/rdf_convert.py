import csv
import gzip
import re
from measure_time import measure_time
from functools import lru_cache
import unicodedata
import base64
import hashlib

csv.field_size_limit(2**31 - 1)

ESCAPE_PATTERNS = [
    (re.compile(r"\\"), r"\\\\"),
    (re.compile(r'"'), r"\""),
    (re.compile(r"\n"), r"\n"),
    (re.compile(r"\r"), r"\r"),
    (re.compile(r"\t"), r"\t"),
]


@lru_cache(maxsize=4096)
def escape_string(s):
    if not s:
        return ""
    result = s
    for pattern, replacement in ESCAPE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


@lru_cache(maxsize=4096)
def sanitize_id(id_string):
    if not id_string:
        return ""

    normalized = unicodedata.normalize("NFKD", id_string)

    if re.search(r"[^\x00-\x7F]|[^\w]", normalized):
        prefix = re.sub(r"[^\w]", "", re.sub(r"[^\x00-\x7F]", "", normalized))[:8]
        if not prefix:
            prefix = "id"

        hash_obj = hashlib.md5(id_string.encode("utf-8"))
        hash_digest = base64.b32encode(hash_obj.digest()).decode("ascii")[:12]

        return f"{prefix}_{hash_digest}"
    else:
        return normalized


def sanitize_label(node_id, label):
    """Sanitize the label by returning the most appropriate label based on node_id left-to-right position."""
    if not label:
        return None
    if "|" not in label:
        return label

    best_word = None
    best_position = float("inf")

    for word in label.split("|"):
        position = node_id.find(word)
        if position != -1 and position < best_position:
            best_word = word
            best_position = position

    return best_word


def get_optimal_batch_size():
    """Determine the optimal batch size for RDF conversion based on available system RAM.
    Returns:
        int: Recommended batch size between 100,000 and 1,000,000
    """
    try:
        import psutil

        available_ram_gb = int(psutil.virtual_memory().available / (1024 * 1024 * 1024))

        base_size = 250_000
        max_size = 1_500_000
        slope = 150_000

        batch_size = int(
            min(max_size, max(base_size, slope * available_ram_gb + base_size))
        )

        print(
            f"Available RAM: {available_ram_gb:.2f} GB, selected batch size: {batch_size:,}"
        )
        return batch_size

    except ImportError:
        print("psutil not installed. Using default batch size of 250,000.")
        return 250_000


def count_lines_in_file(input_file):
    print("Counting lines to initialize progress bar...")
    try:
        from tqdm import tqdm
    except ImportError:
        return None

    total_lines = 0
    with gzip.open(input_file, "rb") as f:
        buf_size = 1024 * 1024
        read_f = f.read
        buf = read_f(buf_size)

        while buf:
            total_lines += buf.count(b"\n")
            buf = read_f(buf_size)

    valid_lines = total_lines - 1

    return valid_lines


@measure_time
def convert_tsv_to_rdf_gzip(
    input_file, output_file, batch_size=250_000, *, total_lines=None
):
    if total_lines is None:
        use_tqdm = False
    else:
        from tqdm import tqdm

        print(f"Found {total_lines:,} lines to process, using tqdm for progress bar.")
        use_tqdm = True

    print(f"Converting {input_file} to RDF format...")

    nodes = set()
    relationships = set()
    nodes_without_labels = set()
    id_mapping = {}

    def process_batch(batch, rdf_file):
        if batch:
            rdf_file.write("\n".join(batch) + "\n")
            batch.clear()

    batch = []
    count = 0
    progress_interval = min(100_000, batch_size // 10)
    compression_level = 3

    with gzip.open(
        output_file, "wt", encoding="utf-8", compresslevel=compression_level
    ) as rdf_file:
        with gzip.open(input_file, "rt", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            header = next(reader)
            print(f"Processing TSV with columns: {header}")

            if use_tqdm:
                pbar = tqdm(total=total_lines, desc="Converting", unit="records")

            iterator = reader

            for row in iterator:
                if len(row) < 4:
                    if use_tqdm:
                        pbar.update(1)
                    print(f"Warning: Skipping row with insufficient columns: {row}")
                    continue

                node1_id = row[1] or ""
                relation = row[2] or ""
                node2_id = row[3] or ""

                if not (node1_id and relation and node2_id):
                    if use_tqdm:
                        pbar.update(1)
                    continue

                # Get sanitized IDs with fast lookups
                if node1_id not in id_mapping:
                    id_mapping[node1_id] = sanitize_id(node1_id)
                sanitized_node1_id = id_mapping[node1_id]

                if node2_id not in id_mapping:
                    id_mapping[node2_id] = sanitize_id(node2_id)
                sanitized_node2_id = id_mapping[node2_id]

                if node1_id not in nodes:
                    nodes.add(node1_id)
                    batch.append(
                        f'_:{sanitized_node1_id} <id> "{escape_string(node1_id)}" .'
                    )

                    node1_label = row[4] if len(row) > 4 else None
                    if node1_label:
                        processed_label = sanitize_label(
                            sanitized_node1_id, node1_label
                        )
                        if processed_label:
                            batch.append(
                                f'_:{sanitized_node1_id} <label> "{escape_string(processed_label)}" .'
                            )
                        else:
                            nodes_without_labels.add(node1_id)
                    else:
                        nodes_without_labels.add(node1_id)

                elif node1_id in nodes_without_labels and (row[4] or ""):
                    processed_label = sanitize_label(sanitized_node1_id, row[4])
                    if processed_label:
                        batch.append(
                            f'_:{sanitized_node1_id} <label> "{escape_string(processed_label)}" .'
                        )
                        nodes_without_labels.remove(node1_id)

                if node2_id not in nodes:
                    nodes.add(node2_id)
                    batch.append(
                        f'_:{sanitized_node2_id} <id> "{escape_string(node2_id)}" .'
                    )

                    node2_label = row[5] if len(row) > 5 else None
                    if node2_label:
                        processed_label = sanitize_label(
                            sanitized_node2_id, node2_label
                        )
                        if processed_label:
                            batch.append(
                                f'_:{sanitized_node2_id} <label> "{escape_string(processed_label)}" .'
                            )
                        else:
                            nodes_without_labels.add(node2_id)
                    else:
                        nodes_without_labels.add(node2_id)

                elif node2_id in nodes_without_labels and (row[5] or ""):
                    processed_label = sanitize_label(sanitized_node2_id, row[5])
                    if processed_label:
                        batch.append(
                            f'_:{sanitized_node2_id} <label> "{escape_string(processed_label)}" .'
                        )
                        nodes_without_labels.remove(node2_id)

                # Process relationships
                rel_key = f"{node1_id}-{relation}-{node2_id}"
                if rel_key not in relationships:
                    relationships.add(rel_key)
                    try:
                        relation_label = escape_string(row[6] or "")
                    except Exception as e:
                        relation_label = "label_not_found"
                    batch.append(
                        f'_:{sanitized_node1_id} <to> _:{sanitized_node2_id} (id="{escape_string(relation)}", label="{relation_label}") .'
                    )

                count += 1
                if use_tqdm:
                    pbar.update(1)
                    if count % 10000 == 0:
                        pbar.set_postfix(
                            {"Nodes": len(nodes), "Relationships": len(relationships)}
                        )
                elif count % progress_interval == 0:
                    print(f"Processed {count:,} records...")

                if count % batch_size == 0:
                    process_batch(batch, rdf_file)
                    if not use_tqdm:
                        print(
                            f"Processed {count:,} records. Nodes: {len(nodes):,}, Relationships: {len(relationships):,}"
                        )

            if use_tqdm:
                pbar.close()

        process_batch(batch, rdf_file)
        print(
            f"Finished processing {count:,} records. Generated {len(nodes):,} nodes and {len(relationships):,} relationships."
        )


if __name__ == "__main__":
    in_file = "../data/cskg.tsv.gz"
    out_file = "../data/data.rdf.gz"
    convert_tsv_to_rdf_gzip(
        input_file=in_file,
        output_file=out_file,
        batch_size=get_optimal_batch_size(),
        total_lines=count_lines_in_file(in_file),
    )
