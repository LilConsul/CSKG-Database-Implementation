import base64
import csv
import gzip
import hashlib
import re
import unicodedata
from functools import lru_cache
import os

from core.measure_time import measure_time

csv.field_size_limit(2**31 - 1)

# Pre-compile regex patterns only once
ESCAPE_PATTERNS = [
    (re.compile(r"\\"), r"\\\\"),
    (re.compile(r'"'), r"\""),
    (re.compile(r"\n"), r"\n"),
    (re.compile(r"\r"), r"\r"),
    (re.compile(r"\t"), r"\t"),
]

# Compile frequently used regex patterns once
NON_ASCII_OR_SPECIAL_CHARS = re.compile(r"[^\x00-\x7F]|[^\w]")
NON_WORD_CHARS = re.compile(r"[^\w]")
NON_ASCII_CHARS = re.compile(r"[^\x00-\x7F]")

# Constants for optimization
DEFAULT_BATCH_SIZE = 250_000
DEFAULT_COMPRESSION_LEVEL = 3


@lru_cache(maxsize=8192)
def escape_string(s):
    """Escape special characters in strings for RDF output with improved performance"""
    if not s:
        return ""

    # Fast path: if no special characters, return the string as-is
    if not any(char in s for char in '\\"\n\r\t'):
        return s

    # Otherwise, apply the substitutions
    result = s
    for pattern, replacement in ESCAPE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


@lru_cache(maxsize=8192)
def sanitize_id(id_string):
    """Sanitize node IDs to make them valid in RDF format with improved performance"""
    if not id_string:
        return ""

    # Use cached result if available (lru_cache decorator handles this)
    normalized = unicodedata.normalize("NFKD", id_string)

    # Fast path for simple IDs (most common case)
    if not NON_ASCII_OR_SPECIAL_CHARS.search(normalized):
        return normalized

    # For complex IDs, use a deterministic hash-based approach
    prefix = NON_WORD_CHARS.sub("", NON_ASCII_CHARS.sub("", normalized))[:8]
    if not prefix:
        prefix = "id"

    # Use a more efficient hashing approach
    hash_obj = hashlib.md5(id_string.encode("utf-8"))
    hash_digest = base64.b32encode(hash_obj.digest()).decode("ascii")[:12]

    return f"{prefix}_{hash_digest}"


@lru_cache(maxsize=4096)
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

    return best_word or label.split("|")[0]  # Default to first word if no match found


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


def create_default_label(node_id):
    """Create a default label from the node ID when no label is available"""
    # Extract meaningful parts from the ID
    parts = re.split(r"[_\-:/]", node_id)
    # Filter out empty parts and take the last non-empty part
    # If none are suitable, use the original ID
    for part in reversed(parts):
        if part and len(part) > 1:
            return part
    return node_id


@measure_time
def convert_tsv_to_rdf_gzip(
    input_file, output_file, batch_size=DEFAULT_BATCH_SIZE, *, total_lines=None
):
    if total_lines is None:
        use_tqdm = False
    else:
        from tqdm import tqdm

        print(f"Found {total_lines:,} lines to process, using tqdm for progress bar.")
        use_tqdm = True

    print(f"Converting {input_file} to RDF format...")

    nodes = set()
    node_relationships = {}  # Key: (node1_id, node2_id), Value: list of (relation_id, relation_label) tuples
    processed_node_pairs = set()
    nodes_without_labels = set()
    id_mapping = {}
    sanitized_to_original = {}  # Mapping from sanitized ID to original ID

    # Track both outgoing and incoming connections separately
    outgoing_neighbors = {}  # Key: node_id, Value: set of outgoing neighbor node_ids
    incoming_neighbors = {}  # Key: node_id, Value: set of incoming neighbor node_ids

    def process_batch(batch, rdf_file):
        if batch:
            rdf_file.write("\n".join(batch) + "\n")
            batch.clear()

    batch = []
    count = 0
    progress_interval = min(100_000, batch_size // 10)

    with gzip.open(
        output_file, "wt", encoding="utf-8", compresslevel=DEFAULT_COMPRESSION_LEVEL
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
                    sanitized_id = sanitize_id(node1_id)
                    id_mapping[node1_id] = sanitized_id
                    sanitized_to_original[sanitized_id] = node1_id
                sanitized_node1_id = id_mapping[node1_id]

                if node2_id not in id_mapping:
                    sanitized_id = sanitize_id(node2_id)
                    id_mapping[node2_id] = sanitized_id
                    sanitized_to_original[sanitized_id] = node2_id
                sanitized_node2_id = id_mapping[node2_id]

                # Track outgoing connections
                if sanitized_node1_id not in outgoing_neighbors:
                    outgoing_neighbors[sanitized_node1_id] = set()
                outgoing_neighbors[sanitized_node1_id].add(sanitized_node2_id)

                # Track incoming connections
                if sanitized_node2_id not in incoming_neighbors:
                    incoming_neighbors[sanitized_node2_id] = set()
                incoming_neighbors[sanitized_node2_id].add(sanitized_node1_id)

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

                try:
                    relation_label = row[6] if len(row) > 6 and row[6] else ""
                except Exception:
                    relation_label = ""

                node_pair = (sanitized_node1_id, sanitized_node2_id)
                if node_pair not in node_relationships:
                    node_relationships[node_pair] = []

                node_relationships[node_pair].append((relation, relation_label))

                count += 1
                if use_tqdm:
                    pbar.update(1)
                    if count % 10000 == 0:
                        pbar.set_postfix(
                            {
                                "Nodes": len(nodes),
                                "Relationship Pairs": len(node_relationships),
                            }
                        )
                elif count % progress_interval == 0:
                    print(f"Processed {count:,} records...")

                if count % batch_size == 0:
                    # Process relationships for this batch, but only those not already processed
                    new_relationships = {
                        k: v
                        for k, v in node_relationships.items()
                        if k not in processed_node_pairs
                    }
                    process_relationships(new_relationships, batch)
                    # Add processed pairs to our tracking set
                    processed_node_pairs.update(new_relationships.keys())
                    node_relationships.clear()

                    # Process the batch
                    process_batch(batch, rdf_file)
                    if not use_tqdm:
                        print(f"Processed {count:,} records. Nodes: {len(nodes):,}")

            if use_tqdm:
                pbar.close()

        # Process remaining relationships that haven't been processed yet
        new_relationships = {
            k: v for k, v in node_relationships.items() if k not in processed_node_pairs
        }
        process_relationships(new_relationships, batch)

        for node_id in nodes_without_labels:
            sanitized_id = id_mapping[node_id]
            # Create a default label based on the node ID
            default_label = create_default_label(node_id)
            batch.append(f'_:{sanitized_id} <label> "{escape_string(default_label)}" .')

        # Get all node IDs that appear in either outgoing or incoming connections
        # This part of your code correctly counts neighbors
        all_node_ids = set(outgoing_neighbors.keys()) | set(incoming_neighbors.keys())
        for node_id in all_node_ids:
            out_neighbors = outgoing_neighbors.get(node_id, set())
            in_neighbors = incoming_neighbors.get(node_id, set())
            total_unique_neighbors = len(
                out_neighbors | in_neighbors
            )  # Union of both sets
            batch.append(
                f'_:{node_id} <unique_neighbors_count> "{total_unique_neighbors}"^^<xs:int> .'
            )

        process_batch(batch, rdf_file)
        print(f"Finished processing {count:,} records. Generated {len(nodes):,} nodes.")


def process_relationships(node_relationships, batch):
    """Process relationships and combine multiple edges between the same nodes."""
    total_relationships = 0

    for (node1_id, node2_id), relations in node_relationships.items():
        total_relationships += 1
        combined_ids = []
        combined_labels = []

        for relation, relation_label in relations:
            combined_ids.append(escape_string(relation))
            combined_labels.append(escape_string(relation_label))

            # Check for synonym/antonym relationships for special handling of distant synonyms and antonyms
            # Since we are using facets to store the relationship and facets doesnt support indexing we may want to this additional field
            if "/r/Synonym" in relation:
                batch.append(f"_:{node1_id} <synonym> _:{node2_id} .")

            if "/r/Antonym" in relation:
                batch.append(f"_:{node1_id} <antonym> _:{node2_id} .")

        if len(relations) == 1:
            relation, relation_label = relations[0]
            batch.append(
                f'_:{node1_id} <to> _:{node2_id} (id="{escape_string(relation)}", label="{escape_string(relation_label)}") .'
            )
        else:
            combined_id = "<;>".join(combined_ids)
            combined_label = "<;>".join(combined_labels)
            batch.append(
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
