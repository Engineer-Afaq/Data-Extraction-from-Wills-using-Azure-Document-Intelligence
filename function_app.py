import json
import logging
import uuid
from datetime import datetime, timezone

import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


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
        logging.info("Parsed job: docId=%s blobName=%s", data.get("docId"), data.get("blobName"))
    except json.JSONDecodeError:
        logging.warning("Queue message was not valid JSON.")
