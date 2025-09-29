import gzip
import json
import csv
import sys
import os

if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Usage: python filter_datasets.py <ICD10_CODE> [output_dir]")
    sys.exit(1)

icd_code = sys.argv[1]
output_dir = sys.argv[2] if len(sys.argv) == 3 else "."

# Paths
condition_input = "MIMIC/MimicCondition.ndjson.gz"
patient_input = "MIMIC/MimicPatient.ndjson.gz"
encounter_input = "MIMIC/MimicEncounter.ndjson.gz"
patient_ids_csv = os.path.join(output_dir, f"patient_ids_for_{icd_code}.csv")

# Output paths
condition_output = os.path.join(output_dir, f"MimicCondition_{icd_code}.ndjson")
patient_output = os.path.join(output_dir, f"MimicPatient_{icd_code}.ndjson")
encounter_output = os.path.join(output_dir, f"MimicEncounter_{icd_code}.ndjson")

# Load patient IDs
patient_ids = set()
try:
    with open(patient_ids_csv, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header
        for row in reader:
            if row:
                patient_ids.add(row[0])
except FileNotFoundError:
    print(f"Patient IDs file {patient_ids_csv} not found.")
    sys.exit(1)

print(f"Loaded {len(patient_ids)} patient IDs for ICD-10 code {icd_code}")

# Filter MimicCondition
print("Filtering MimicCondition...")
with open(condition_output, 'w', encoding='utf-8') as out_f:
    with gzip.open(condition_input, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if record.get('resourceType') == 'Condition':
                    subject = record.get('subject', {})
                    ref = subject.get('reference', '')
                    if ref.startswith('Patient/'):
                        patient_id = ref[len('Patient/'):]
                        if patient_id in patient_ids:
                            # Check if this condition matches the ICD code
                            code = record.get('code', {})
                            coding = code.get('coding', [])
                            has_code = False
                            for c in coding:
                                if c.get('code', '').startswith(icd_code):
                                    has_code = True
                                    break
                            if has_code:
                                out_f.write(line + '\n')
            except json.JSONDecodeError:
                continue

print(f"Filtered MimicCondition saved to {condition_output}")

# Filter MimicPatient
print("Filtering MimicPatient...")
with open(patient_output, 'w', encoding='utf-8') as out_f:
    with gzip.open(patient_input, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if record.get('resourceType') == 'Patient':
                    patient_id = record.get('id', '')
                    if patient_id in patient_ids:
                        out_f.write(line + '\n')
            except json.JSONDecodeError:
                continue

print(f"Filtered MimicPatient saved to {patient_output}")

# Filter MimicEncounter
print("Filtering MimicEncounter...")
with open(encounter_output, 'w', encoding='utf-8') as out_f:
    with gzip.open(encounter_input, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if record.get('resourceType') == 'Encounter':
                    subject = record.get('subject', {})
                    ref = subject.get('reference', '')
                    if ref.startswith('Patient/'):
                        patient_id = ref[len('Patient/'):]
                        if patient_id in patient_ids:
                            out_f.write(line + '\n')
            except json.JSONDecodeError:
                continue

print(f"Filtered MimicEncounter saved to {encounter_output}")