import gzip
import json
import csv
import sys
import os

if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Usage: python extract_comorbidities.py <ICD10_CODE> [output_dir]")
    sys.exit(1)

icd_code = sys.argv[1]
output_dir = sys.argv[2] if len(sys.argv) == 3 else "."

# Paths
condition_input = "MIMIC/MimicCondition.ndjson.gz"
patient_ids_csv = os.path.join(output_dir, f"patient_ids_for_{icd_code}.csv")
output_csv = os.path.join(output_dir, f"comorbidities_{icd_code}.csv")

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

# Collect comorbidities
comorbidities = []

print("Extracting comorbidities...")
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
                        # Check if this condition has the primary ICD code
                        code = record.get('code', {})
                        coding = code.get('coding', [])
                        has_primary = False
                        for c in coding:
                            if c.get('code', '').startswith(icd_code):
                                has_primary = True
                                break
                        if not has_primary:
                            # It's a comorbidity
                            condition_id = record.get('id', '')
                            # Take the first coding for simplicity
                            if coding:
                                cond_code = coding[0].get('code', '')
                                cond_display = coding[0].get('display', '')
                                cond_system = coding[0].get('system', '')
                            else:
                                cond_code = ''
                                cond_display = ''
                                cond_system = ''
                            comorbidities.append({
                                'patient_id': patient_id,
                                'condition_id': condition_id,
                                'condition_code': cond_code,
                                'condition_display': cond_display,
                                'condition_system': cond_system
                            })
        except json.JSONDecodeError:
            continue

# Write to CSV
with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['patient_id', 'condition_id', 'condition_code', 'condition_display', 'condition_system']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in comorbidities:
        writer.writerow(row)

print(f"Extracted {len(comorbidities)} comorbidity records to {output_csv}")