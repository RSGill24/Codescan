import pandas as pd
import json
import sys

def integration_readiness(file_path, domain="Claims"):
    result = {
        "file_path": file_path,
        "domain": domain,
        "status": "PASS",
        "missing_keys": [],
        "error_message": ""
    }

    required_keys = {
        "Claims": ["claim_id", "member_id", "drug_amount"],
        "Provider": ["provider_id", "npi"],
        "Member": ["member_id", "dob"]
    }

    try:
        df = pd.read_csv(file_path)
        domain_keys = required_keys.get(domain, [])
        result["missing_keys"] = [k for k in domain_keys if k not in df.columns]

        if result["missing_keys"]:
            result["status"] = "WARN"

        # Numeric/date validation
        if "drug_amount" in df.columns and not pd.api.types.is_numeric_dtype(df["drug_amount"]):
            result["status"] = "FAIL"
            result["error_message"] = "drug_amount column is not numeric"

        if "dob" in df.columns:
            try:
                pd.to_datetime(df["dob"])
            except Exception:
                result["status"] = "WARN"
                result["error_message"] = "dob column has invalid date format"

    except Exception as e:
        result["status"] = "FAIL"
        result["error_message"] = str(e)

    print(json.dumps(result))
    print(f"##vso[task.setvariable variable=readiness_status;isOutput=true]{result['status']}")
    print(f"##vso[task.setvariable variable=readiness_error;isOutput=true]{result['error_message']}")
    print(f"##vso[task.setvariable variable=missing_keys;isOutput=true]{','.join(result['missing_keys']) if result['missing_keys'] else ''}")
    return result

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--domain", default="Claims")
    args = parser.parse_args()
    integration_readiness(args.file, args.domain)
