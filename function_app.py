import base64
import json
import logging
import os
import struct
import time
import uuid
from datetime import datetime, timezone

import azure.functions as func
import pyodbc
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings


# -----------------------------
# Azure Functions app
# -----------------------------
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# -----------------------------
# Helpers: Content Understanding (Foundry) REST
# -----------------------------
_CU_SCOPE = "https://cognitiveservices.azure.com/.default"


def _get_cu_headers(content_type: str | None = None) -> dict:
    cred = DefaultAzureCredential()
    token = cred.get_token(_CU_SCOPE).token
    headers = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def cu_analyze_binary(endpoint: str, analyzer_id: str, content: bytes, api_version: str) -> str:
    """
    Starts analysis. Returns the Operation-Location URL to poll.

    GA 2025-11-01: analyzeBinary expects the request body to be a base64-encoded JSON string.
    """
    url = f"{endpoint.rstrip('/')}/contentunderstanding/analyzers/{analyzer_id}:analyzeBinary"

    b64 = base64.b64encode(content).decode("ascii")
    body = json.dumps(b64)  # JSON string

    headers = _get_cu_headers(content_type="application/json")
    headers["x-ms-client-request-id"] = str(uuid.uuid4())

    resp = requests.post(
        url,
        params={"api-version": api_version},
        headers=headers,
        data=body,
        timeout=180,
    )

    if resp.status_code >= 400:
        logging.error("CU analyzeBinary failed: %s %s", resp.status_code, resp.text)

    resp.raise_for_status()

    op_loc = resp.headers.get("Operation-Location")
    if not op_loc:
        raise RuntimeError("Missing Operation-Location header from analyzeBinary response.")
    return op_loc


def cu_poll_result(operation_location: str, timeout_s: int = 240, poll_interval_s: float = 1.0) -> dict:
    start = time.time()

    while True:
        headers = _get_cu_headers()
        resp = requests.get(operation_location, headers=headers, timeout=60)

        if resp.status_code >= 400:
            logging.error("CU poll failed: %s %s", resp.status_code, resp.text)

        resp.raise_for_status()
        payload = resp.json()

        status = (payload.get("status") or "").lower()
        if status in ("succeeded", "failed", "canceled", "cancelled"):
            return payload

        if time.time() - start > timeout_s:
            raise TimeoutError(f"Timed out waiting for analysis result after {timeout_s}s.")

        time.sleep(poll_interval_s)


# -----------------------------
# Helpers: CU field extraction
# -----------------------------
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


def _cu_val_conf(raw_fields: dict, cu_key: str):
    f = (raw_fields or {}).get(cu_key)
    if not f:
        return None, None
    conf = f.get("confidence")
    val = extract_field_value(f)
    if isinstance(val, (dict, list)):
        val = json.dumps(val, ensure_ascii=False)
    return val, conf


# -----------------------------
# Helpers: Azure SQL (Managed Identity / Entra token)
# -----------------------------
_SQL_COPT_SS_ACCESS_TOKEN = 1256  # SQL_COPT_SS_ACCESS_TOKEN


def _get_sql_access_token() -> bytes:
    cred = DefaultAzureCredential()
    token = cred.get_token("https://database.windows.net/.default").token
    token_bytes = token.encode("utf-16-le")
    return struct.pack("<I", len(token_bytes)) + token_bytes


def _sql_connect() -> pyodbc.Connection:
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    if not server or not database:
        raise RuntimeError("Missing SQL_SERVER or SQL_DATABASE in settings.")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    token_struct = _get_sql_access_token()
    return pyodbc.connect(conn_str, attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct})


def upsert_processed_will_from_cu(
    doc_id: str,
    blob_name: str,
    analyzer_id: str,
    result_payload: dict,
    extracted_path: str,
    curated_path: str,
) -> None:
    """
    Writes into dbo.processed_wills using YOUR analyzer field keys.

    CU Field Keys (from your analyzer schema):
      - TestatorName -> ClientName
      - TestatorDateOfBirth -> DateOfBirth
      - SignatureDate -> WillDate
      - TestatorAddress -> Addresses
      - FuneralPreference + FuneralDonationRequest -> FuneralWishes
      - ResidueBeneficiaryPrimary (+ survivorship/method) -> ResiduaryBeneficiaries
      - TrusteePowers + TrusteeAdministrativeProvisions -> TrustProvisions
    """
    contents = (result_payload.get("result") or {}).get("contents") or []
    first = contents[0] if contents else {}
    raw_fields = first.get("fields") or {}

    # Required per your SQL schema
    client_name, client_conf = _cu_val_conf(raw_fields, "TestatorName")
    will_date, will_date_conf = _cu_val_conf(raw_fields, "SignatureDate")

    if not client_name:
        raise RuntimeError("Required CU field 'TestatorName' missing → cannot insert (ClientName is NOT NULL).")
    if not will_date:
        raise RuntimeError("Required CU field 'SignatureDate' missing → cannot insert (WillDate is NOT NULL).")

    dob, dob_conf = _cu_val_conf(raw_fields, "TestatorDateOfBirth")
    address, address_conf = _cu_val_conf(raw_fields, "TestatorAddress")

    funeral_pref, funeral_pref_conf = _cu_val_conf(raw_fields, "FuneralPreference")
    funeral_don, funeral_don_conf = _cu_val_conf(raw_fields, "FuneralDonationRequest")

    residue_primary, residue_primary_conf = _cu_val_conf(raw_fields, "ResidueBeneficiaryPrimary")
    residue_surv, residue_surv_conf = _cu_val_conf(raw_fields, "ResidueSurvivorshipPeriod")
    residue_method, residue_method_conf = _cu_val_conf(raw_fields, "ResidueDistributionMethod")

    trustee_powers, trustee_powers_conf = _cu_val_conf(raw_fields, "TrusteePowers")
    trustee_admin, trustee_admin_conf = _cu_val_conf(raw_fields, "TrusteeAdministrativeProvisions")

    # Build combined SQL fields
    funeral_wishes = "\n".join([x for x in [funeral_pref, funeral_don] if x]) or None
    funeral_conf = max([c for c in [funeral_pref_conf, funeral_don_conf] if c is not None], default=None)

    residuary_parts = []
    if residue_primary:
        residuary_parts.append(f"Primary: {residue_primary}")
    if residue_surv:
        residuary_parts.append(f"Survivorship: {residue_surv}")
    if residue_method:
        residuary_parts.append(f"Distribution: {residue_method}")
    residuary_benef = "\n".join(residuary_parts) or None
    residuary_conf = max(
        [c for c in [residue_primary_conf, residue_surv_conf, residue_method_conf] if c is not None],
        default=None,
    )

    trust_parts = []
    if trustee_powers:
        trust_parts.append(f"Trustee powers: {trustee_powers}")
    if trustee_admin:
        trust_parts.append(f"Administrative provisions: {trustee_admin}")
    trust_prov = "\n".join(trust_parts) or None
    trust_conf = max([c for c in [trustee_powers_conf, trustee_admin_conf] if c is not None], default=None)

    # Confidence flag
    threshold = float(os.getenv("LOW_CONF_THRESHOLD", "0.75"))
    confs = [client_conf, will_date_conf, dob_conf, address_conf, funeral_conf, residuary_conf, trust_conf]
    low_conf_flag = any((c is not None and c < threshold) for c in confs)

    source_path = f"raw-wills/{blob_name}"
    processed_utc = datetime.now(timezone.utc)

    lineage = {
        "analyzerId": analyzer_id,
        "sourcePath": source_path,
        "extractedJsonPath": extracted_path,
        "curatedJsonPath": curated_path,
        "apiVersion": os.getenv("CU_API_VERSION", "2025-11-01"),
    }
    lineage_refs = json.dumps(lineage, ensure_ascii=False)

    with _sql_connect() as conn:
        cur = conn.cursor()

        # MERGE = UPSERT
        cur.execute(
            """
            MERGE dbo.processed_wills AS tgt
            USING (SELECT ? AS DocId) AS src
              ON tgt.DocId = src.DocId
            WHEN MATCHED THEN
              UPDATE SET
                SourcePath = ?,
                ModelVersion = ?,
                ProcessedUtc = ?,
                ClientName = ?, ClientName_Confidence = ?,
                DateOfBirth = ?, DateOfBirth_Confidence = ?,
                WillDate = ?, WillDate_Confidence = ?,
                Addresses = ?, Addresses_Confidence = ?,
                FuneralWishes = ?, FuneralWishes_Confidence = ?,
                ResiduaryBeneficiaries = ?, ResiduaryBeneficiaries_Confidence = ?,
                TrustProvisions = ?, TrustProvisions_Confidence = ?,
                LowConfidenceFlag = ?,
                LineageRefs = ?
            WHEN NOT MATCHED THEN
              INSERT (
                DocId, SourcePath, ModelVersion, ProcessedUtc,
                ClientName, ClientName_Confidence,
                DateOfBirth, DateOfBirth_Confidence,
                WillDate, WillDate_Confidence,
                Addresses, Addresses_Confidence,
                FuneralWishes, FuneralWishes_Confidence,
                ResiduaryBeneficiaries, ResiduaryBeneficiaries_Confidence,
                TrustProvisions, TrustProvisions_Confidence,
                LowConfidenceFlag, LineageRefs
              )
              VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?);
            """,
            (
                doc_id,
                source_path,
                analyzer_id,
                processed_utc,
                client_name,
                client_conf,
                dob,
                dob_conf,
                will_date,
                will_date_conf,
                address,
                address_conf,
                funeral_wishes,
                funeral_conf,
                residuary_benef,
                residuary_conf,
                trust_prov,
                trust_conf,
                int(low_conf_flag),
                lineage_refs,
            ),
        )

        conn.commit()


# -----------------------------
# Function: enqueue_job
# -----------------------------
@app.function_name(name="enqueue_job")
@app.route(route="enqueue-job", methods=["POST"])
@app.queue_output(
    arg_name="msg",
    queue_name="will-jobs",
    connection="AzureWebJobsStorage",
)
def enqueue_job(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
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


# -----------------------------
# Function: process_will
# -----------------------------
@app.function_name(name="process_will")
@app.queue_trigger(
    arg_name="msg",
    queue_name="will-jobs",
    connection="AzureWebJobsStorage",
)
def process_will(msg: func.QueueMessage) -> None:
    raw = msg.get_body().decode("utf-8", errors="replace")
    logging.info("process_will received raw message: %s", raw)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logging.warning("Queue message was not valid JSON.")
        return

    doc_id = data.get("docId") or str(uuid.uuid4())
    blob_name = data.get("blobName")
    if not blob_name:
        logging.warning("Missing blobName in message; skipping.")
        return

    # --- Storage config ---
    conn_str = os.getenv("AzureWebJobsStorage")
    if not conn_str:
        logging.error("AzureWebJobsStorage is not set.")
        return

    raw_container = os.getenv("RAW_WILLS_CONTAINER", "raw-wills")
    extracted_container = os.getenv("EXTRACTED_CONTAINER", "extracted-json")
    curated_container = os.getenv("CURATED_CONTAINER", "curated-json")

    # --- Download file from raw-wills ---
    bsc = BlobServiceClient.from_connection_string(conn_str)
    blob_client = bsc.get_blob_client(container=raw_container, blob=blob_name)

    logging.info("Downloading blob: container=%s blob=%s docId=%s", raw_container, blob_name, doc_id)

    downloader = blob_client.download_blob()
    content = downloader.readall()
    logging.info("Downloaded %d bytes for docId=%s blobName=%s", len(content), doc_id, blob_name)

    # --- Call Content Understanding ---
    endpoint = os.getenv("DOCINTEL_ENDPOINT")
    analyzer_id = os.getenv("DOCINTEL_ANALYZER_ID", "Wills_Analysis")
    api_version = os.getenv("CU_API_VERSION", "2025-11-01")

    if not endpoint:
        logging.error("Missing DOCINTEL_ENDPOINT in settings.")
        return

    logging.info("Submitting to Content Understanding: analyzerId=%s apiVersion=%s", analyzer_id, api_version)
    op_loc = cu_analyze_binary(endpoint, analyzer_id, content, api_version)
    logging.info("Operation-Location: %s", op_loc)

    result_payload = cu_poll_result(op_loc)
    status = result_payload.get("status")
    logging.info("Analysis completed with status=%s", status)

    if str(status).lower() != "succeeded":
        raise RuntimeError(f"Analysis did not succeed. Status={status}. Payload={result_payload}")

    # --- Write FULL JSON to extracted-json ---
    extracted_blob_name = f"{doc_id}.json"
    curated_blob_name = f"{doc_id}.json"

    full_json = json.dumps(result_payload, ensure_ascii=False, indent=2)
    bsc.get_blob_client(extracted_container, extracted_blob_name).upload_blob(
        full_json,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logging.info("Wrote extracted result to %s/%s", extracted_container, extracted_blob_name)

    # --- Write curated JSON (flattened fields) to curated-json ---
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

    # --- Upsert into Azure SQL ---
    extracted_path = f"{extracted_container}/{extracted_blob_name}"
    curated_path = f"{curated_container}/{curated_blob_name}"

    upsert_processed_will_from_cu(
        doc_id=doc_id,
        blob_name=blob_name,
        analyzer_id=analyzer_id,
        result_payload=result_payload,
        extracted_path=extracted_path,
        curated_path=curated_path,
    )
    logging.info("Upserted dbo.processed_wills for docId=%s", doc_id)
