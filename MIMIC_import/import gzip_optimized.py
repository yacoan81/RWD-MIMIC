import gzip
import json
import os
import re
import pandas as pd
import glob
from collections import defaultdict, deque
from typing import Dict, List, Any, Iterator
import io
import sys

# No autocomplete setup - keep it simple
readline = None


def get_unique_filename(base_name: str, output_folder: str) -> str:
    """Return a filename with _01 before .csv in the output folder."""
    base, _ = os.path.splitext(base_name)  # e.g., MimicPatient.ndjson
    base, _ = os.path.splitext(base)       # -> MimicPatient
    n = 1
    candidate = os.path.join(output_folder, f"{base}_{n:02d}.csv")
    while os.path.exists(candidate):
        n += 1
        candidate = os.path.join(output_folder, f"{base}_{n:02d}.csv")
    return candidate


def flatten_dict_iterative(obj: Any, parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    """Iteratively flatten a dict/list into flat key-value pairs using a stack approach."""
    result = {}
    stack = deque([(obj, parent_key)])
    
    while stack:
        current_obj, current_key = stack.popleft()
        
        if isinstance(current_obj, dict):
            for key, value in current_obj.items():
                new_key = f"{current_key}{sep}{key}" if current_key else key
                if isinstance(value, (dict, list)):
                    stack.append((value, new_key))
                else:
                    result[new_key] = value
        elif isinstance(current_obj, list):
            for i, value in enumerate(current_obj, start=1):
                new_key = f"{current_key}{sep}{i}" if current_key else str(i)
                if isinstance(value, (dict, list)):
                    stack.append((value, new_key))
                else:
                    result[new_key] = value
    
    return result


# ---- Pre-compiled regex for better performance ----
_re_underscore_number = re.compile(r'_(\d+)')

# Cache for column transformations
_column_transform_cache = {}

def transform_col_name(name: str) -> str:
    """Transform column name with caching for repeated calls."""
    if name in _column_transform_cache:
        return _column_transform_cache[name]
    
    # Use string replace instead of split/join for better performance
    result = name.replace('_extension_', '_ext_').replace('_communication_', '_comm_')
    result = _re_underscore_number.sub(r'\1', result)
    
    _column_transform_cache[name] = result
    return result


def dedupe_column_names_optimized(names: List[str]) -> List[str]:
    """Efficiently deduplicate column names using defaultdict."""
    counts = defaultdict(int)
    result = []
    
    for name in names:
        if counts[name] == 0:
            result.append(name)
        else:
            result.append(f"{name}__{counts[name] + 1}")
        counts[name] += 1
    
    return result


def collect_all_columns(input_file: str, sample_size: int = 1000) -> List[str]:
    """Collect all unique columns from a sample of the file to establish schema upfront."""
    all_columns = set()
    processed_lines = 0
    
    try:
        for line in read_ndjson_lines(input_file):
            if processed_lines >= sample_size:
                break
                
            try:
                record = json.loads(line)
                flat_record = flatten_dict_iterative(record)
                all_columns.update(flat_record.keys())
                processed_lines += 1
            except (json.JSONDecodeError, Exception):
                continue
    except Exception:
        pass
    
    # Transform and deduplicate column names
    sorted_columns = sorted(all_columns)
    transformed = [transform_col_name(c) for c in sorted_columns]
    return dedupe_column_names_optimized(transformed)


def read_ndjson_lines(file_path: str, buffer_size: int = 65536) -> Iterator[str]:
    """Efficiently read NDJSON lines with proper buffering."""
    if file_path.endswith(".gz"):
        # gzip.open doesn't support buffering parameter
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    yield line
    else:
        # Regular files support buffering
        with open(file_path, 'r', encoding='utf-8', buffering=buffer_size) as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    yield line


def ndjson_to_csv_flat_optimized(input_file: str, output_folder: str,
                                chunk_size: int = 10000, buffer_size: int = 65536) -> None:
    """Optimized NDJSON to CSV conversion with single file output."""
    base_name = os.path.basename(input_file)
    output_file = get_unique_filename(base_name, output_folder)
    
    # Collect schema upfront to avoid multiple files
    print(f"🔍 Analyzing schema for {base_name}...")
    column_order = collect_all_columns(input_file, sample_size=1000)
    
    batch_records = []
    processed_lines = 0
    first_chunk = True
    
    try:
        for line in read_ndjson_lines(input_file, buffer_size):
            try:
                # Parse JSON
                record = json.loads(line)
                
                # Flatten the record
                flat_record = flatten_dict_iterative(record)
                batch_records.append(flat_record)
                
                processed_lines += 1
                
                # Process batch when it reaches chunk_size
                if len(batch_records) >= chunk_size:
                    # Create DataFrame with consistent column order
                    df = pd.DataFrame(batch_records)
                    
                    # Ensure all expected columns exist (fill with None if missing)
                    for col in column_order:
                        if col not in df.columns:
                            df[col] = None
                    
                    # Reorder columns to match schema
                    df = df.reindex(columns=column_order)
                    
                    # Write to CSV
                    df.to_csv(output_file, index=False, encoding='utf-8',
                              mode='w' if first_chunk else 'a', header=first_chunk)
                    first_chunk = False
                    batch_records = []  # Clear for next batch
                    print(f"✅ Processed {processed_lines:,} lines from {base_name}...")
                    
            except json.JSONDecodeError as e:
                print(f"⚠️  Skipping invalid JSON on line {processed_lines + 1}: {e}")
                continue
            except Exception as e:
                print(f"⚠️  Error processing line {processed_lines + 1}: {e}")
                continue
        
        # Write remaining records
        if batch_records:
            df = pd.DataFrame(batch_records)
            
            # Ensure all expected columns exist
            for col in column_order:
                if col not in df.columns:
                    df[col] = None
            
            # Reorder columns to match schema
            df = df.reindex(columns=column_order)
            
            df.to_csv(output_file, index=False, encoding='utf-8',
                      mode='w' if first_chunk else 'a', header=first_chunk)
                
    except Exception as e:
        print(f"❌ Error processing {input_file}: {e}")
        return
    
    print(f"✅ Finished {base_name} → {os.path.basename(output_file)} ({processed_lines:,} records)")


def ndjson_to_csv_flat_legacy(input_file: str, output_folder: str, chunk_size: int = 50000) -> None:
    """Original implementation for comparison."""
    base_name = os.path.basename(input_file)
    output_file = get_unique_filename(base_name, output_folder)

    open_func = gzip.open if input_file.endswith(".gz") else open
    first_chunk = True

    with open_func(input_file, 'rt', encoding='utf-8') as f_in:
        rows = []
        for line_num, line in enumerate(f_in, start=1):
            try:
                record = json.loads(line)
                flat_record = flatten_dict_iterative(record)  # Use optimized version
                rows.append(flat_record)

                if line_num % chunk_size == 0:
                    df = pd.DataFrame(rows)
                    transformed = [transform_col_name(c) for c in df.columns]
                    transformed = dedupe_column_names_optimized(transformed)
                    df.columns = transformed

                    df.to_csv(output_file, index=False, encoding='utf-8',
                              mode='w' if first_chunk else 'a', header=first_chunk)
                    first_chunk = False
                    rows = []  # free memory
                    print(f"✅ Wrote {line_num:,} lines so far from {base_name}...")
            except json.JSONDecodeError as e:
                print(f"⚠️  Skipping invalid JSON on line {line_num}: {e}")
                continue

        # write remaining rows
        if rows:
            df = pd.DataFrame(rows)
            transformed = [transform_col_name(c) for c in df.columns]
            transformed = dedupe_column_names_optimized(transformed)
            df.columns = transformed
            df.to_csv(output_file, index=False, encoding='utf-8',
                      mode='w' if first_chunk else 'a', header=first_chunk)

    print(f"✅ Finished {base_name} → {os.path.basename(output_file)}")


if __name__ == "__main__":
    input_folder = input("Enter input folder path: ").strip()
    output_folder = input("Enter output folder path: ").strip()
    
    use_optimized = input("Use optimized version? (y/n, default=y): ").strip().lower()
    use_optimized = use_optimized != 'n'

    if not os.path.isdir(input_folder):
        print("Input folder not found.")
        exit(1)
    if not os.path.isdir(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    files = glob.glob(os.path.join(input_folder, "*.ndjson")) + \
            glob.glob(os.path.join(input_folder, "*.ndjson.gz"))

    if not files:
        print("No .ndjson or .ndjson.gz files found in folder.")
    else:
        print(f"Found {len(files)} files to process")
        for f in files:
            if use_optimized:
                ndjson_to_csv_flat_optimized(f, output_folder)
            else:
                ndjson_to_csv_flat_legacy(f, output_folder)