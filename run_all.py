import subprocess
import sys
import os

if len(sys.argv) != 2:
    print("Usage: python run_all.py <ICD10_CODE>")
    sys.exit(1)

icd_code = sys.argv[1]
output_dir = "Output"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

print(f"Starting extraction for ICD-10 code {icd_code}")

# Step 1: Extract patient IDs
print("Step 1: Extracting patient IDs...")
result = subprocess.run([sys.executable, "MIMIC_import/extract_patient_ids.py", icd_code, output_dir], capture_output=True, text=True)
if result.returncode != 0:
    print(f"Error in extract_patient_ids: {result.stderr}")
    sys.exit(1)
print(result.stdout.strip())

# Step 2: Filter datasets
print("Step 2: Filtering datasets...")
result = subprocess.run([sys.executable, "MIMIC_import/filter_datasets.py", icd_code, output_dir], capture_output=True, text=True)
if result.returncode != 0:
    print(f"Error in filter_datasets: {result.stderr}")
    sys.exit(1)
print(result.stdout.strip())

# Step 3: Extract comorbidities
print("Step 3: Extracting comorbidities...")
result = subprocess.run([sys.executable, "MIMIC_import/extract_comorbidities.py", icd_code, output_dir], capture_output=True, text=True)
if result.returncode != 0:
    print(f"Error in extract_comorbidities: {result.stderr}")
    sys.exit(1)
print(result.stdout.strip())

print(f"All steps completed successfully. Outputs are in the {output_dir} folder.")