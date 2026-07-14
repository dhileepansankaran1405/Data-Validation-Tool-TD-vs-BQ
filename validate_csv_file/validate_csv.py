import csv
import os
import sys

CSV_FILE = "migration_inventory.csv"
VALID_TYPES = {"row", "column"}

def perform_static_analysis():
    print(f"🕵️ Starting compliance analysis on catalog map: '{CSV_FILE}'...")
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ Critical Failure: Master document '{CSV_FILE}' cannot be resolved locally.")
        sys.exit(1)
        
    errors_detected = 0
    warnings_detected = 0
    
    with open(CSV_FILE, mode='r', encoding='utf-8') as file:
        try:
            sample = file.read(2048)
            file.seek(0)
            dialect = csv.Sniffer().sniff(sample)
        except Exception as e:
            print(f"❌ Structural Read Error: Formatting analysis failed: {e}")
            sys.exit(1)

        reader = csv.DictReader(file, dialect=dialect)
        
        expected_fields = {"source_table", "target_table", "validation_type", "primary_key", "filter_condition"}
        current_headers = set(reader.fieldnames or [])
        if not expected_fields.issubset(current_headers):
            missing = expected_fields - current_headers
            print(f"❌ Schema Validation Failure: Missing column definitions in CSV header row: {missing}")
            sys.exit(1)

        for line_num, row in enumerate(reader, start=2):
            source = (row.get('source_table') or '').strip()
            target = (row.get('target_table') or '').strip()
            v_type = (row.get('validation_type') or '').strip().lower()
            p_key = (row.get('primary_key') or '').strip()
            filter_cond = (row.get('filter_condition') or '').strip()

            if not source or not target:
                print(f"❌ Row {line_num}: Operational Error. Source or Target naming references are completely missing.")
                errors_detected += 1
                continue

            if v_type not in VALID_TYPES:
                print(f"❌ Row {line_num} [{source}]: Configuration Error. Engine type '{v_type}' is invalid. Supported options are: {VALID_TYPES}")
                errors_detected += 1

            if v_type == "row" and not p_key:
                print(f"❌ Row {line_num} [{source}]: Execution Blocked. Row-level comparison checks strictly require a defined primary key column mapping.")
                errors_detected += 1

            if v_type == "column" and p_key:
                print(f"⚠️ Row {line_num} [{source}]: Optimization Warning. Column evaluations ignore specific primary key settings.")
                warnings_detected += 1
                
            if ";" in filter_cond or "--" in filter_cond:
                print(f"❌ Row {line_num} [{source}]: Security Alert. Suspicious SQL formatting sequences isolated inside your filter criteria fields.")
                errors_detected += 1

    print("\n==============================================================================")
    print(f"📊 HEALTH REPORT SUMMARY: [Errors Isolated: {errors_detected}] | [Warnings Identified: {warnings_detected}]")
    print("==============================================================================")
    
    if errors_detected > 0:
        print("🛑 Migration build halted. Please correct the inventory configuration problems listed above.")
        sys.exit(1)
    else:
        print("🚀 Inventory checks passed safely! You are ready to run the automated compilation frameworks.")

if __name__ == "__main__":
    perform_static_analysis()
