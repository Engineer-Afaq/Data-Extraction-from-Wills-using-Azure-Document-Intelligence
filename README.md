# Wills Processing Pipeline (Azure Functions + Content Understanding + Azure SQL)

This repo contains an **Azure Functions (Python)** pipeline that processes uploaded **PDF wills** using **Azure Content Understanding (Azure AI Foundry)** and writes both **raw + curated JSON outputs** to Blob Storage, then **upserts extracted fields** into **Azure SQL**.

## What it does

1. **PDF lands in Blob Storage** container: `raw-wills`
2. You call an HTTP endpoint to enqueue a job (`enqueue_job`)
3. A queue-triggered function (`process_will`) runs:
   - Downloads the PDF from Blob Storage
   - Sends it to **Content Understanding** custom analyzer (`Wills_Analysis`)
   - Polls the `Operation-Location` until completion
   - Writes:
     - Full CU payload JSON → `extracted-json/{docId}.json`
     - Curated/flattened JSON → `curated-json/{docId}.json`
   - Upserts key values + confidences to `dbo.processed_wills`

---

## Architecture

**Blob Storage (`raw-wills`)** → **HTTP enqueue** → **Queue (`will-jobs`)** → **Azure Function worker**  
→ **Content Understanding (Foundry) analyzer** → results persisted to **Blob Storage + Azure SQL**

---

## Azure resources required

- **Azure Storage Account**
  - Containers:
    - `raw-wills` (input PDFs)
    - `extracted-json` (full CU response JSON)
    - `curated-json` (flattened JSON)
  - Queue:
    - `will-jobs`
- **Azure AI Foundry / Content Understanding**
  - Custom analyzer: `Wills_Analysis` (or your analyzer id)
- **Azure SQL Database**
  - Table: `dbo.processed_wills`
- **Azure Function App** (Python)
  - Managed Identity (recommended) to access:
    - Azure AI (if using token auth)
    - Azure SQL (token auth)
    - (optionally) Storage (if you later switch from connection string)

---

## Key implementation detail (important)

The pipeline uses the **Content Understanding GA API version**:

- `2025-11-01`

It calls:

```
POST {DOCINTEL_ENDPOINT}/contentunderstanding/analyzers/{DOCINTEL_ANALYZER_ID}:analyzeBinary?api-version=2025-11-01
Content-Type: application/octet-stream
Body: raw PDF bytes
```

✅ This is the correct way to send a PDF for analysis (no base64-wrapping into JSON).

---

## Project entry points

- `function_app.py`
  - `enqueue_job` (HTTP trigger)
  - `process_will` (Queue trigger)
  - CU helpers: `cu_analyze_binary`, `cu_poll_result`
  - SQL upsert: `upsert_processed_will_from_cu`

---

## Environment variables

> You said you are keeping existing variable names — the code uses these.

### Storage
- `AzureWebJobsStorage` *(required)*  
  Connection string for the Storage account used by Functions runtime (queue + blob).
- `RAW_WILLS_CONTAINER` *(optional, default: `raw-wills`)*
- `EXTRACTED_CONTAINER` *(optional, default: `extracted-json`)*
- `CURATED_CONTAINER` *(optional, default: `curated-json`)*

### Content Understanding (Foundry)
- `DOCINTEL_ENDPOINT` *(required)*  
  Example: `https://<resource>.services.ai.azure.com/`
- `DOCINTEL_ANALYZER_ID` *(optional, default: `Wills_Analysis`)*
- `CU_API_VERSION` *(optional, default: `2025-11-01`)*

**Authentication options:**
- If `CU_KEY` is set → uses header `Ocp-Apim-Subscription-Key`
- Else → uses `DefaultAzureCredential` (Managed Identity / Entra)

Set one of:
- `CU_KEY` *(optional if using managed identity)*

### Azure SQL (token auth)
- `SQL_SERVER` *(required)*  
  Example: `myserver.database.windows.net`
- `SQL_DATABASE` *(required)*  
  Example: `mydb`

### Confidence threshold
- `LOW_CONF_THRESHOLD` *(optional, default: `0.75`)*

---

## Field mapping (analyzer → SQL columns)

From CU payload path:
`result.contents[0].fields`

Analyzer keys mapped:
- `TestatorName` → `ClientName`
- `TestatorDateOfBirth` → `DateOfBirth`
- `SignatureDate` → `WillDate`
- `TestatorAddress` → `Addresses`
- `FuneralPreference` + `FuneralDonationRequest` → `FuneralWishes`
- `ResidueBeneficiaryPrimary` (+ `ResidueSurvivorshipPeriod`, `ResidueDistributionMethod`) → `ResiduaryBeneficiaries`
- `TrusteePowers` + `TrusteeAdministrativeProvisions` → `TrustProvisions`

`LowConfidenceFlag` is set if any captured confidence is below `LOW_CONF_THRESHOLD`.

---

## Run locally

### 1) Create / update `local.settings.json`
Create a `local.settings.json` (do not commit it):

```json
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "DefaultEndpointsProtocol=...",

    "RAW_WILLS_CONTAINER": "raw-wills",
    "EXTRACTED_CONTAINER": "extracted-json",
    "CURATED_CONTAINER": "curated-json",

    "DOCINTEL_ENDPOINT": "https://<resource>.services.ai.azure.com/",
    "DOCINTEL_ANALYZER_ID": "Wills_Analysis",
    "CU_API_VERSION": "2025-11-01",

    "CU_KEY": "",

    "SQL_SERVER": "<server>.database.windows.net",
    "SQL_DATABASE": "<db>",

    "LOW_CONF_THRESHOLD": "0.75"
  }
}
```

> If you’re using Managed Identity locally, use Azure CLI login (`az login`) and leave `CU_KEY` empty.

### 2) Install dependencies
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3) Start Functions host
```bash
func start
```

---

## How to use

### Step A — Upload a PDF to Blob
Upload your PDF to container `raw-wills` with some filename, e.g. `sample.pdf`.

### Step B — Enqueue a job
Call the HTTP endpoint:

**POST** `http://localhost:7071/api/enqueue-job`

Body:
```json
{
  "blobName": "sample.pdf",
  "docId": "optional-guid-or-any-string"
}
```

If `docId` is omitted, the function generates one.

### Step C — Results
- Full CU output:
  - `extracted-json/{docId}.json`
- Curated output:
  - `curated-json/{docId}.json`
- SQL row:
  - `dbo.processed_wills` upserted by `DocId`

---

## Troubleshooting

### CU output contains Base64 of a PDF
If you see content starting with `JVBERi0x...` in markdown, that’s the PDF itself.
Fix is: send **raw bytes** to `:analyzeBinary` with `application/octet-stream` (this repo does that).

### Missing CU fields
Check:
- Analyzer schema matches keys in mapping
- Use logs: `CU raw_fields keys: ...`

### SQL connection fails
- Confirm `ODBC Driver 18 for SQL Server` is available in your environment
- Ensure Function App identity has SQL permissions (when deployed)
- Verify `SQL_SERVER` and `SQL_DATABASE`

---

## Open-source checklist (recommended next steps)

- Add `local.settings.json` to `.gitignore`
- Add `local.settings.json.example` without secrets
- Add `LICENSE` (MIT or Apache-2.0)
- Add CI (lint + minimal tests)
- Add sample PDF + sample output JSON (if allowed)

---

## Git commit

```bash
git status
git add function_app.py README.md
git commit -m "Fix CU analyzeBinary request; document pipeline"
git push
```

---

## License

Choose one:
- MIT
- Apache-2.0

