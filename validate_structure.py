import pandas as pd
import json
import sys
import re

def validate_structure(file_path):
    summary = {
        "file_path": file_path,
        "status": "PASS",
        "row_count": 0,
        "col_count": 0,
        "null_pct": {},
        "special_char_columns": [],
        "error_message": ""
    }

    try:
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError("File contains no data rows")

        summary["row_count"] = len(df)
        summary["col_count"] = len(df.columns)
        summary["null_pct"] = df.isnull().mean().round(3).to_dict()

        # Detect special characters in columns
        bad_cols = [col for col in df.columns if re.search(r"[^a-zA-Z0-9_]", col)]
        summary["special_char_columns"] = bad_cols
        if bad_cols:
            summary["status"] = "WARN"

    except Exception as e:
        summary["status"] = "FAIL"
        summary["error_message"] = str(e)

    # Export key metrics as Azure DevOps pipeline variables
    print(json.dumps(summary))
    print(f"##vso[task.setvariable variable=structure_status;isOutput=true]{summary['status']}")
    print(f"##vso[task.setvariable variable=structure_error;isOutput=true]{summary['error_message']}")
    print(f"##vso[task.setvariable variable=row_count;isOutput=true]{summary['row_count']}")
    print(f"##vso[task.setvariable variable=col_count;isOutput=true]{summary['col_count']}")
    print(f"##vso[task.setvariable variable=null_pct;isOutput=true]{summary['null_pct']}")
    return summary

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    args = parser.parse_args()
    validate_structure(args.file) 
