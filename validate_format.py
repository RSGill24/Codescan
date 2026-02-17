import os
import csv
import json
import sys

def validate_format(file_path):
    result = {
        "file_path": file_path,
        "status": "PASS",
        "delimiter": None,
        "header_count": 0,
        "headers": [],
        "error_message": ""
    }

    try:
        # Check empty file
        if os.stat(file_path).st_size == 0:
            raise ValueError("File is empty")

        # Try multiple delimiters
        with open(file_path, "r", encoding="utf-8") as f:
            sample = f.readline()
            for delim in [",", "\t", "|", ";", "~"]:
                if delim in sample:
                    result["delimiter"] = delim
                    break

            if not result["delimiter"]:
                raise ValueError("File is not delimited by comma, tab, pipe, semicolon, or tilde")

            f.seek(0)
            reader = csv.reader(f, delimiter=result["delimiter"])
            headers = next(reader, [])
            if not headers:
                raise ValueError("No headers found in file")

            result["header_count"] = len(headers)
            result["headers"] = headers

    except Exception as e:
        result["status"] = "FAIL"
        result["error_message"] = str(e)

    print(json.dumps(result))
    print(f"##vso[task.setvariable variable=format_status;isOutput=true]{result['status']}")
    print(f"##vso[task.setvariable variable=format_error;isOutput=true]{result['error_message']}")
    return result

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    args = parser.parse_args()
    validate_format(args.file)

