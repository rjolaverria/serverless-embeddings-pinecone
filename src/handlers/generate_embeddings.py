import json
import logging
from .utils.BucketClient import BucketClient
from .utils.PineconeClient import PineconeClient
from .utils.OpenAIClient import OpenAIClient
from .utils.text import get_text_from_page, split_into_chunks

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def run(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}

        try:
            openAI = OpenAIClient()
            pinecone = PineconeClient()
            bucket = BucketClient()

            # process records in the batch
            for record in event["Records"]:
                body = json.loads(record["body"])

                # get the item from the bucket and split into chunks of text
                (source, text) = bucket.get_bucket_item(body["key"])
                text = get_text_from_page(text)
                chunks = split_into_chunks(text)
                embeddings = [openAI.get_embedding(chunk) for chunk in chunks]

                # fetch and store the embeddings
                pinecone.store_embeddings(chunks, embeddings, source)

        except Exception as e:
            batch_item_failures.append({"itemIdentifier": record["messageId"]})
            logger.error("An error occurred")
            logger.error(e)

        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
