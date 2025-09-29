import gzip
import json
import os
import re
import pandas as pd
import glob

# --- Autocomplete setup ---
try:
    import readline  # Linux/macOS
except ImportError:
    try:
        import pyreadline as readline  # Some Windows setups
    except ImportError:
        try:
            import pyreadline3 as readline  # Modern Windows
        except ImportError:
            readline = None


def get_unique_filename(base_name, output_folder):
    """Return a filename with _n before .csv in the output folder."""
    base, _ = os.path.splitext(base_name)  # e.g., MimicPatient.ndjson
    base, _ = os.path.splitext(base)       # -> MimicPatient
    n = 1
    candidate = os.path.join(output_folder, f"{base}_{n}.csv")
    while os.path.exists(candidate):
        n += 1
        candidate = os.path.join(output_folder, f"{base}_{n}.csv")
    return candidate


def flatten_dict(d, parent_key="", sep="_"):
    """Recursively flatten a dict/list into flat key-value pairs, numbering lists."""
    items = {}
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
    elif isinstance(d, list):
        for i, v in enumerate(d, start=1):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            if isinstance(v, (dict, list)):
                items.update(flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
    return items


# ---- Column-name transformation rules ----
_re_underscore_number = re.compile(r'_(\d+)')

def transform_col_name(name: str) -> str:
    tokens = name.split('_')
    mapped = []
    for t in tokens:
        if t == 'extension':
            mapped.append('ext')
        elif t == 'communication':
            mapped.append('comm')
        else:
            mapped.append(t)
    s = '_'.join(mapped)
    s = _re_underscore_number.sub(r'\1', s)
    return s


def dedupe_column_names(names):
    counts = {}
    out = []
    for n in names:
        c = counts.get(n, 0)
        if c == 0:
            out.append(n)
        else:
            out.append(f"{n}__{c+1}")
        counts[n] = c + 1
    return out


def ndjson_to_csv_flat(input_file, output_folder, chunk_size=50000):
    """Flatten NDJSON (possibly gzipped) to CSV with transformed headers."""
    base_name = os.path.basename(input_file)
    output_file = get_unique_filename(base_name, output_folder)

    open_func = gzip.open if input_file.endswith(".gz") else open
    first_chunk = True

    with open_func(input_file, 'rt', encoding='utf-8') as f_in:
        rows = []
        for line_num, line in enumerate(f_in, start=1):
            record = json.loads(line)
            flat_record = flatten_dict(record)
            rows.append(flat_record)

            if line_num % chunk_size == 0:
                df = pd.DataFrame(rows)
                transformed = [transform_col_name(c) for c in df.columns]
                transformed = dedupe_column_names(transformed)
                df.columns = transformed

                df.to_csv(output_file, index=False, encoding='utf-8',
                          mode='w' if first_chunk else 'a', header=first_chunk)
                first_chunk = False
                rows = []  # free memory
                print(f"✅ Wrote {line_num:,} lines so far from {base_name}...")

        # write remaining rows
        if rows:
            df = pd.DataFrame(rows)
            transformed = [transform_col_name(c) for c in df.columns]
            transformed = dedupe_column_names(transformed)
            df.columns = transformed
            df.to_csv(output_file, index=False, encoding='utf-8',
                      mode='w' if first_chunk else 'a', header=first_chunk)

    print(f"✅ Finished {base_name} → {os.path.basename(output_file)}")



if __name__ == "__main__":
    if readline:
        def complete(text, state):
            return (glob.glob(text + '*') + [None])[state]

        if hasattr(readline, "set_completer_delims"):
            readline.set_completer_delims(' \t\n;')

        if hasattr(readline, "parse_and_bind"):
            try:
                readline.parse_and_bind("tab: complete")
            except Exception:
                pass

        if hasattr(readline, "set_completer"):
            readline.set_completer(complete)

        
    input_folder = input("Enter input folder path: ").strip()
    output_folder = input("Enter output folder path: ").strip()

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
        for f in files:
            ndjson_to_csv_flat(f, output_folder)
