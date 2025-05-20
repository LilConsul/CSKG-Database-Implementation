import base64
import csv
import gzip
import hashlib
import re
import unicodedata
from functools import lru_cache
from collections import defaultdict

from core.measure_time import measure_time

csv.field_size_limit(2**31 - 1)

ESCAPE_PATTERNS = [
    (re.compile(r"\\"), r"\\\\"),
    (re.compile(r'"'), r"\""),
    (re.compile(r"\n"), r"\n"),
    (re.compile(r"\r"), r"\r"),
    (re.compile(r"\t"), r"\t"),
]

# Pre-compile these patterns for better performance
ID_SPLIT_PATTERN = re.compile(r"[_\-:/]")
NON_ASCII_PATTERN = re.compile(r"[^\x00-\x7F]|[^\w]")
NON_ASCII_REMOVAL = re.compile(r"[^\x00-\x7F]")
NON_WORD_REMOVAL = re.compile(r"[^\w]")


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

    if NON_ASCII_PATTERN.search(normalized):
        prefix = NON_WORD_REMOVAL.sub("", NON_ASCII_REMOVAL.sub("", normalized))[:8]
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
    """Count lines in a gzipped file more efficiently using binary mode."""
    print("Counting lines to initialize progress bar...")
    try:
        from tqdm import tqdm
    except ImportError:
        return None

    total_lines = 0
    with gzip.open(input_file, "rb") as f:
        buf_size = 8 * 1024 * 1024  # Increased buffer size for better performance
        read_f = f.read
        buf = read_f(buf_size)

        while buf:
            total_lines += buf.count(b"\n")
            buf = read_f(buf_size)

    return total_lines - 1  # Subtract header line


def create_default_label(node_id):
    """Create a default label from the node ID when no label is available"""
    # Extract meaningful parts from the ID
    parts = ID_SPLIT_PATTERN.split(node_id)
    # Filter out empty parts and take the last non-empty part
    # If none are suitable, use the original ID
    for part in reversed(parts):
        if part and len(part) > 1:
            return part
    return node_id


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
    # Store only node pairs that haven't been written to the output file yet
    node_relationships = defaultdict(list)  # Key: (node1_id, node2_id), Value: list of (relation_id, relation_label) tuples
    id_mapping = {}
    nodes_without_labels = set()

    # Track both outgoing and incoming connections using defaultdict(set) for automatic initialization
    outgoing_neighbors = defaultdict(set)  # Key: node_id, Value: set of outgoing neighbor node_ids
    incoming_neighbors = defaultdict(set)  # Key: node_id, Value: set of incoming neighbor node_ids

    def process_batch(batch, rdf_file):
        if batch:
            rdf_file.write("\n".join(batch) + "\n")
            batch.clear()

    batch = []
    count = 0
    progress_interval = min(100_000, batch_size // 10)
    # Lower compression level for faster writing, still provides good compression
    compression_level = 2
    processed_relationships_count = 0

    # Pre-allocate batch list for better performance
    batch = [None] * (batch_size * 2)
    batch_index = 0

    # Function to add to batch with less overhead
    def add_to_batch(item):
        nonlocal batch_index
        if batch_index >= len(batch):
            # Double the batch size when needed
            batch.extend([None] * len(batch))
        batch[batch_index] = item
        batch_index += 1

    # Modified process_batch function to handle the pre-allocated list
    def process_batch(batch, batch_index, rdf_file):
        if batch_index > 0:
            rdf_file.write("\n".join(batch[:batch_index]) + "\n")
            return 0  # Reset the batch index
        return batch_index

    with gzip.open(
        output_file, "wt", encoding="utf-8", compresslevel=compression_level
    ) as rdf_file:
        with gzip.open(input_file, "rb") as f:
            # Process file in binary mode with custom TSV parsing for better performance
            reader = csv.reader(
                (line.decode('utf-8', errors='replace') for line in f),
                delimiter="\t",
                quoting=csv.QUOTE_NONE
            )
            header = next(reader)
            print(f"Processing TSV with columns: {header}")

            if use_tqdm:
                pbar = tqdm(total=total_lines, desc="Converting", unit="records")

            for row in reader:
                if len(row) < 4:
                    if use_tqdm:
                        pbar.update(1)
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
                    sanitized_id = sanitize_id(node1_id)
                    id_mapping[node1_id] = sanitized_id
                sanitized_node1_id = id_mapping[node1_id]

                if node2_id not in id_mapping:
                    sanitized_id = sanitize_id(node2_id)
                    id_mapping[node2_id] = sanitized_id
                sanitized_node2_id = id_mapping[node2_id]

                # Track connections - these are now automatically initialized thanks to defaultdict
                outgoing_neighbors[sanitized_node1_id].add(sanitized_node2_id)
                incoming_neighbors[sanitized_node2_id].add(sanitized_node1_id)

                if node1_id not in nodes:
                    nodes.add(node1_id)
                    add_to_batch(f'_:{sanitized_node1_id} <id> "{escape_string(node1_id)}" .')

                    node1_label = row[4] if len(row) > 4 else None
                    if node1_label:
                        processed_label = sanitize_label(sanitized_node1_id, node1_label)
                        if processed_label:
                            add_to_batch(f'_:{sanitized_node1_id} <label> "{escape_string(processed_label)}" .')
                        else:
                            nodes_without_labels.add(node1_id)
                    else:
                        nodes_without_labels.add(node1_id)

                elif node1_id in nodes_without_labels and len(row) > 4 and row[4]:
                    processed_label = sanitize_label(sanitized_node1_id, row[4])
                    if processed_label:
                        add_to_batch(f'_:{sanitized_node1_id} <label> "{escape_string(processed_label)}" .')
                        nodes_without_labels.remove(node1_id)

                if node2_id not in nodes:
                    nodes.add(node2_id)
                    add_to_batch(f'_:{sanitized_node2_id} <id> "{escape_string(node2_id)}" .')

                    node2_label = row[5] if len(row) > 5 else None
                    if node2_label:
                        processed_label = sanitize_label(sanitized_node2_id, node2_label)
                        if processed_label:
                            add_to_batch(f'_:{sanitized_node2_id} <label> "{escape_string(processed_label)}" .')
                        else:
                            nodes_without_labels.add(node2_id)
                    else:
                        nodes_without_labels.add(node2_id)

                elif node2_id in nodes_without_labels and len(row) > 5 and row[5]:
                    processed_label = sanitize_label(sanitized_node2_id, row[5])
                    if processed_label:
                        add_to_batch(f'_:{sanitized_node2_id} <label> "{escape_string(processed_label)}" .')
                        nodes_without_labels.remove(node2_id)

                relation_label = row[6] if len(row) > 6 and row[6] else ""

                node_pair = (sanitized_node1_id, sanitized_node2_id)
                node_relationships[node_pair].append((relation, relation_label))

                count += 1
                if use_tqdm:
                    pbar.update(1)
                    if count % 10000 == 0:
                        pbar.set_postfix({
                            "Nodes": len(nodes),
                            "Relationship Pairs": len(node_relationships),
                        })
                elif count % progress_interval == 0:
                    print(f"Processed {count:,} records...")

                # Process in batches to avoid excessive memory usage
                if batch_index >= batch_size or len(node_relationships) >= batch_size // 5:
                    processed_relationships_count += process_relationships(node_relationships, batch, batch_index, add_to_batch)
                    node_relationships.clear()
                    batch_index = process_batch(batch, batch_index, rdf_file)

                    # Periodic memory cleanup for large datasets
                    if count % (batch_size * 10) == 0:
                        import gc
                        gc.collect()

                    if not use_tqdm:
                        print(f"Processed {count:,} records. Nodes: {len(nodes):,}")

            if use_tqdm:
                pbar.close()

        # Process remaining relationships
        processed_relationships_count += process_relationships(node_relationships, batch, batch_index, add_to_batch)
        node_relationships.clear()

        # Process remaining nodes without labels
        for node_id in nodes_without_labels:
            sanitized_id = id_mapping[node_id]
            # Create a default label based on the node ID
            default_label = create_default_label(node_id)
            add_to_batch(f'_:{sanitized_id} <label> "{escape_string(default_label)}" .')

        # Process neighborhood data in chunks to avoid memory issues
        print("Processing neighborhood data...")
        all_node_ids = list(set(outgoing_neighbors.keys()) | set(incoming_neighbors.keys()))
        chunk_size = min(10000, max(1000, len(all_node_ids) // 100))

        for i in range(0, len(all_node_ids), chunk_size):
            node_chunk = all_node_ids[i:i+chunk_size]
            for node_id in node_chunk:
                out_neighbors = outgoing_neighbors.get(node_id, set())
                in_neighbors = incoming_neighbors.get(node_id, set())

                # Use a more efficient approach for computing unique neighbors
                total_unique_neighbors = len(out_neighbors)
                for n in in_neighbors:
                    if n not in out_neighbors:
                        total_unique_neighbors += 1

                add_to_batch(f'_:{node_id} <unique_neighbors_count> "{total_unique_neighbors}"^^<xs:int> .')

                # Free memory as we go
                if node_id in outgoing_neighbors:
                    del outgoing_neighbors[node_id]
                if node_id in incoming_neighbors:
                    del incoming_neighbors[node_id]

            # Write batch periodically while processing neighborhood data
            if batch_index > batch_size // 2:
                batch_index = process_batch(batch, batch_index, rdf_file)

        # Write any remaining data
        batch_index = process_batch(batch, batch_index, rdf_file)
        print(f"Finished processing {count:,} records. Generated {len(nodes):,} nodes. Processed {processed_relationships_count:,} relationships.")


def process_relationships(node_relationships, batch, batch_index, add_to_batch):
    """Process relationships and combine multiple edges between the same nodes."""
    total_relationships = 0

    for (node1_id, node2_id), relations in node_relationships.items():
        total_relationships += 1

        # Optimize for the common case of a single relation
        if len(relations) == 1:
            relation, relation_label = relations[0]
            escaped_relation = escape_string(relation)
            escaped_label = escape_string(relation_label)

            # Check for special relationship types
            if "/r/Synonym" in relation:
                add_to_batch(f"_:{node1_id} <synonym> _:{node2_id} .")
            if "/r/Antonym" in relation:
                add_to_batch(f"_:{node1_id} <antonym> _:{node2_id} .")

            add_to_batch(
                f'_:{node1_id} <to> _:{node2_id} (id="{escaped_relation}", label="{escaped_label}") .'
            )
        else:
            # For multiple relations, process and join
            seen_types = set()
            combined_ids = []
            combined_labels = []

            for relation, relation_label in relations:
                escaped_relation = escape_string(relation)
                escaped_label = escape_string(relation_label)
                combined_ids.append(escaped_relation)
                combined_labels.append(escaped_label)

                # Check for special relationship types, but avoid duplicates
                if "/r/Synonym" in relation and "synonym" not in seen_types:
                    add_to_batch(f"_:{node1_id} <synonym> _:{node2_id} .")
                    seen_types.add("synonym")

                if "/r/Antonym" in relation and "antonym" not in seen_types:
                    add_to_batch(f"_:{node1_id} <antonym> _:{node2_id} .")
                    seen_types.add("antonym")

            combined_id = "<;>".join(combined_ids)
            combined_label = "<;>".join(combined_labels)
            add_to_batch(
                f'_:{node1_id} <to> _:{node2_id} (id="{combined_id}", label="{combined_label}") .'
            )

    return total_relationships


if __name__ == "__main__":
    in_file = "../data/cskg.tsv.gz"
    out_file = "../data/data.rdf.gz"
    convert_tsv_to_rdf_gzip(
        input_file=in_file,
        output_file=out_file,
        batch_size=get_optimal_batch_size(),
        total_lines=count_lines_in_file(in_file),
    )
