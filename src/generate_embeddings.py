import json
import logging
import os
from boto3 import client
import urllib.parse


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SQS_QUEUE_URL = os.environ.get("EMBEDDINGS_QUEUE_URL")
SOURCES_BUCKET_NAME = os.environ.get("SOURCES_BUCKET_NAME")

sqs = client("sqs")
s3 = client("s3")


def get_bucket_item(key: str):
    decoded_key = urllib.parse.unquote(key)
    response = s3.get_object(Bucket=SOURCES_BUCKET_NAME, Key=decoded_key)

    source_url = response["Metadata"].get("source", "")
    body = response["Body"].read().decode("utf-8")

    return source_url, body


def run(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
        for record in event["Records"]:
            try:
                body = json.loads(record["body"])
                get_bucket_item(body["key"])
            except Exception as e:
                batch_item_failures.append({"itemIdentifier": record["messageId"]})
                logger.error("An error occurred")
                logger.error(e)

        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
