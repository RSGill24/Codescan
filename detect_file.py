import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime

def main():
    # ✅ Read environment variables passed from YAML
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    storage_account = os.getenv("STORAGE_ACCOUNT")
    container_name = os.getenv("CONTAINER_NAME")
    output_path = os.getenv("OUTPUT_PATH", "/tmp/downloaded_file.csv")

    # 🔍 Validate all required variables
    if not conn_str:
        print("❌ ERROR: AZURE_STORAGE_CONNECTION_STRING not found.")
        exit(1)
    if not container_name:
        print("❌ ERROR: CONTAINER_NAME not found.")
        exit(1)

    print(f"🔗 Connecting to Azure Blob Storage account: {storage_account or 'N/A'}")
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service.get_container_client(container_name)

    print(f"📂 Scanning container: {container_name}")
    blobs = list(container_client.list_blobs())

    if not blobs:
        print("⚠️ No files found in the container.")
        exit(1)

    # Sort blobs by last modified date (most recent first)
    blobs.sort(key=lambda x: x.last_modified, reverse=True)
    latest_blob = blobs[0]

    print(f"✅ Found latest blob: {latest_blob.name} (Last Modified: {latest_blob.last_modified})")

    # 🧩 Download the blob
    blob_client = container_client.get_blob_client(latest_blob)
    with open(output_path, "wb") as f:
        data = blob_client.download_blob()
        f.write(data.readall())

    print(f"##vso[task.setvariable variable=file_path;isOutput=true]{output_path}")

    # ✅ Export the downloaded file path for later pipeline stages
    print(f"##vso[task.setvariable variable=file_path]{output_path}")

    # Optional: log metadata
    print("🧾 Metadata:")
    print(f"  - Blob Name: {latest_blob.name}")
    print(f"  - Container: {container_name}")
    print(f"  - Downloaded To: {output_path}")
    print(f"  - Timestamp: {datetime.utcnow()}")

    print("✅ File detection and download completed successfully.")

if __name__ == "__main__":
    main()
