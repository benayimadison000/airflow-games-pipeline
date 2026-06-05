"""
Validates all DAG files by importing them.
Catches syntax errors and bad imports before they reach production.
"""
import sys
import os
import importlib.util

dags_folder = os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags')
dags_folder = os.path.abspath(dags_folder)

sys.path.insert(0, dags_folder)

dag_files = [
    f for f in os.listdir(dags_folder)
    if f.endswith('.py') and not f.startswith('__')
]

if not dag_files:
    print("No DAG files found.")
    sys.exit(1)

errors = []

for filename in dag_files:
    filepath = os.path.join(dags_folder, filename)
    print(f"Checking: {filepath}")
    try:
        spec = importlib.util.spec_from_file_location('dag', filepath)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        print(f"  OK: {filename}")
    except Exception as e:
        print(f"  FAILED: {filename}")
        print(f"  Error: {e}")
        errors.append(filename)

if errors:
    print(f"\nValidation FAILED for {len(errors)} file(s): {errors}")
    sys.exit(1)

print(f"\nAll {len(dag_files)} DAG file(s) passed validation.")