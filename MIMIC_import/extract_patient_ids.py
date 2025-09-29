import gzip
import json
import csv
import sys
import os

if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Usage: python extract_patient_ids.py <ICD10_CODE> [output_dir]")
    sys.exit(1)

icd_code = sys.argv[1]
output_dir = sys.argv[2] if len(sys.argv) == 3 else "."

patient_ids = set()

file_path = "MIMIC/MimicCondition.ndjson.gz"

try:
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if record.get('resourceType') == 'Condition':
                    code = record.get('code', {})
                    coding = code.get('coding', [])
                    for c in coding:
                        if c.get('code', '').startswith(icd_code):
                            subject = record.get('subject', {})
                            ref = subject.get('reference', '')
                            if ref.startswith('Patient/'):
                                patient_id = ref[len('Patient/'):]
                                patient_ids.add(patient_id)
                            break
            except json.JSONDecodeError:
                continue
except FileNotFoundError:
    print(f"File {file_path} not found.")
    sys.exit(1)

# Write to CSV
filename = os.path.join(output_dir, f"patient_ids_for_{icd_code}.csv")
with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['patient_id'])
    for pid in sorted(patient_ids):
        writer.writerow([pid])

print(f"Extracted {len(patient_ids)} unique patient IDs for ICD-10 code {icd_code} to {filename}")