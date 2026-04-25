import requests

def bulk_delete(instance_url, access_token, object_name, csv_data):

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    job_url = f"{instance_url}/services/data/v59.0/jobs/ingest"

    # 1. Create Job
    job_data = {
        "object": object_name,
        "operation": "delete", #hardDelete
        "contentType": "CSV",
        "lineEnding": "CRLF",
        
    }

    job_res = requests.post(job_url, json=job_data, headers=headers)
    job = job_res.json()

    if isinstance(job, list) or "id" not in job:
        return {"state": "Failed", "error": job}

    job_id = job["id"]

    # 2. Upload CSV
    upload_url = f"{job_url}/{job_id}/batches"

    upload_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "text/csv"
    }

    upload_res = requests.put(
        upload_url,
        data=csv_data.encode("utf-8"),
        headers=upload_headers
    )

    if upload_res.status_code not in [200, 201, 204]:
        return {"state": "Failed", "error": upload_res.text}

    # 3. Close Job
    requests.patch(
        f"{job_url}/{job_id}",
        json={"state": "UploadComplete"},
        headers=headers
    )

    return job_id