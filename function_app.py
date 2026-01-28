import json
import logging
import os
import time
import uuid
import base64
from datetime import datetime, timezone

import azure.functions as func
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.function_name(name="enqueue_job")
@app.route(route="enqueue-job", methods=["POST"])
@app.queue_output(
    arg_name="msg",
    queue_name="will-jobs",
    connection="AzureWebJobsStorage",
)
def enqueue_job(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    """
    POST /api/enqueue-job
    Body: { "blobName": "some.pdf", "docId": "optional" }
    Enqueues: {docId, blobName, submittedUtc}
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid JSON body. Expected: {\"blobName\":\"...\", \"docId\":\"...\"}",
            status_code=400,
        )

    blob_name = body.get("blobName")
    if not blob_name or not isinstance(blob_name, str):
        return func.HttpResponse("Missing/invalid 'blobName' (string).", status_code=400)

    doc_id = body.get("docId")
    if not doc_id or not isinstance(doc_id, str):
        doc_id = str(uuid.uuid4())

    payload = {
        "docId": doc_id,
        "blobName": blob_name,
        "submittedUtc": datetime.now(timezone.utc).isoformat(),
    }

    msg.set(json.dumps(payload))

    return func.HttpResponse(
        json.dumps({"enqueued": True, "job": payload}),
        mimetype="application/json",
        status_code=202,
    )


def _guess_content_type(blob_name: str) -> str:
    name = (blob_name or "").lower()
    if name.endswith(".pdf"):
        return "application/pdf"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    if name.endswith(".png"):
        return "image/png"
    return "application/octet-stream"


def _get_cu_headers(content_type: str | None = None) -> dict:
    """
    Token auth for Content Understanding / Document Intelligence.
    Requires you to be signed in locally (e.g., `az login`) or have other creds available.
    """
    cred = DefaultAzureCredential()
    token = cred.get_token("https://cognitiveservices.azure.com/.default").token

    headers = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def cu_analyze_binary(endpoint: str, analyzer_id: str, content: bytes, api_version: str) -> str:
    """
    Starts analysis. Returns the Operation-Location URL to poll.

    IMPORTANT (GA 2025-11-01):
    analyzeBinary expects the request body to be a base64-encoded string (JSON string),
    not raw PDF bytes. :contentReference[oaicite:1]{index=1}
    """
    url = f"{endpoint.rstrip('/')}/contentunderstanding/analyzers/{analyzer_id}:analyzeBinary"

    # Base64 encode the binary content
    b64 = base64.b64encode(content).decode("ascii")

    # Send as a JSON string (quotes included via json.dumps)
    body = json.dumps(b64)

    headers = _get_cu_headers(content_type="application/json")
    headers["x-ms-client-request-id"] = str(uuid.uuid4())

    resp = requests.post(
        url,
        params={"api-version": api_version},
        headers=headers,
        data=body,
        timeout=120,
    )

    # Helpful diagnostics if it fails
    if resp.status_code >= 400:
        logging.error("CU analyzeBinary failed: %s %s", resp.status_code, resp.text)

    resp.raise_for_status()

    op_loc = resp.headers.get("Operation-Location")
    if not op_loc:
        raise RuntimeError("Missing Operation-Location header from analyzeBinary response.")
    return op_loc



def cu_poll_result(operation_location: str, timeout_s: int = 180, poll_interval_s: float = 1.0) -> dict:
    """
    Polls analysis until terminal state.
    """
    start = time.time()

    while True:
        headers = _get_cu_headers()
        resp = requests.get(operation_location, headers=headers, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        status = (payload.get("status") or "").lower()
        if status in ("succeeded", "failed", "canceled", "cancelled"):
            return payload

        if time.time() - start > timeout_s:
            raise TimeoutError(f"Timed out waiting for analysis result after {timeout_s}s.")

        time.sleep(poll_interval_s)


def extract_field_value(field_obj):
    """
    Generic field flattener for CU response fields.
    """
    t = (field_obj or {}).get("type")
    if t == "string":
        return field_obj.get("valueString")
    if t == "date":
        return field_obj.get("valueDate")
    if t == "time":
        return field_obj.get("valueTime")
    if t == "number":
        return field_obj.get("valueNumber")
    if t == "integer":
        return field_obj.get("valueInteger")
    if t == "boolean":
        return field_obj.get("valueBoolean")
    if t == "json":
        return field_obj.get("valueJson")
    if t == "object":
        obj = field_obj.get("valueObject") or {}
        return {k: extract_field_value(v) for k, v in obj.items()}
    if t == "array":
        arr = field_obj.get("valueArray") or []
        return [extract_field_value(v) for v in arr]
    return None


@app.function_name(name="process_will")
@app.queue_trigger(
    arg_name="msg",
    queue_name="will-jobs",
    connection="AzureWebJobsStorage",
)
def process_will(msg: func.QueueMessage) -> None:
    """
    Phase 3 + 4:
    - Downloads file bytes from raw-wills/<blobName>
    - Calls Content Understanding analyzer (REST, Entra ID token auth)
    - Writes full output JSON to extracted-json/<docId>.json
    - Writes curated JSON to curated-json/<docId>.json
    """
    raw = msg.get_body().decode("utf-8", errors="replace")
    logging.info("process_will received raw message: %s", raw)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logging.warning("Queue message was not valid JSON.")
        return

    doc_id = data.get("docId")
    blob_name = data.get("blobName")
    if not blob_name:
        logging.warning("Missing blobName in message; skipping.")
        return
    if not doc_id:
        doc_id = str(uuid.uuid4())

    # --- Storage config ---
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        logging.error("AzureWebJobsStorage is not set.")
        return

    raw_container = os.getenv("RAW_WILLS_CONTAINER", "raw-wills")
    extracted_container = os.getenv("EXTRACTED_CONTAINER", "extracted-json")
    curated_container = os.getenv("CURATED_CONTAINER", "curated-json")

    # --- Download from raw-wills ---
    bsc = BlobServiceClient.from_connection_string(conn_str)
    blob_client = bsc.get_blob_client(container=raw_container, blob=blob_name)

    logging.info("Downloading blob: container=%s blob=%s docId=%s", raw_container, blob_name, doc_id)

    try:
        downloader = blob_client.download_blob()
        content = downloader.readall()
    except Exception as e:
        logging.exception("Failed to download blob '%s' from '%s': %s", blob_name, raw_container, e)
        return

    logging.info("Downloaded %d bytes for docId=%s blobName=%s", len(content), doc_id, blob_name)

    # --- Phase 4: Call Content Understanding (REST) ---
    endpoint = os.getenv("DOCINTEL_ENDPOINT")
    analyzer_id = os.getenv("DOCINTEL_ANALYZER_ID", "Wills_Analysis")
    api_version = os.getenv("CU_API_VERSION", "2025-11-01")

    if not endpoint:
        logging.error("Missing DOCINTEL_ENDPOINT in settings.")
        return

    content_type = _guess_content_type(blob_name)

    logging.info("Submitting to Content Understanding (token auth): analyzerId=%s apiVersion=%s", analyzer_id, api_version)
    op_loc = cu_analyze_binary(endpoint, analyzer_id, content, api_version)
    logging.info("Operation-Location: %s", op_loc)

    result_payload = cu_poll_result(op_loc)
    status = result_payload.get("status")
    logging.info("Analysis completed with status=%s", status)

    # If failed, raise so the queue retries (you can change this behavior later)
    if str(status).lower() != "succeeded":
        raise RuntimeError(f"Analysis did not succeed. Status={status}. Payload={result_payload}")

    # --- Save FULL JSON to extracted-json ---
    extracted_blob_name = f"{doc_id}.json"
    curated_blob_name = f"{doc_id}.json"

    full_json = json.dumps(result_payload, ensure_ascii=False, indent=2)
    bsc.get_blob_client(extracted_container, extracted_blob_name).upload_blob(
        full_json,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logging.info("Wrote extracted result to %s/%s", extracted_container, extracted_blob_name)

    # --- Build curated JSON (generic field flattening) ---
    fields = {}
    try:
        contents = (result_payload.get("result") or {}).get("contents") or []
        first = contents[0] if contents else {}
        raw_fields = first.get("fields") or {}
        fields = {k: extract_field_value(v) for k, v in raw_fields.items()}
    except Exception:
        logging.exception("Curated mapping failed; continuing with empty fields.")

    curated = {
        "docId": doc_id,
        "blobName": blob_name,
        "analyzerId": analyzer_id,
        "status": status,
        "fields": fields,
    }

    curated_json = json.dumps(curated, ensure_ascii=False, indent=2)
    bsc.get_blob_client(curated_container, curated_blob_name).upload_blob(
        curated_json,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logging.info("Wrote curated result to %s/%s", curated_container, curated_blob_name)
