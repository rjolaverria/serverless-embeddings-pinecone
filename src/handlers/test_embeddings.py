import logging
from .utils.EmbeddingsStore import EmbeddingsStore
from .utils.OpenAIClient import OpenAIClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    response = {}
    try:
        openAI = OpenAIClient()
        store = EmbeddingsStore()

        promptEmbedding = openAI.get_embedding(event)

        with store:
            results = store.get_by_cosine_similarity(
                embedding=str(promptEmbedding), threshold=0.75, limit=5
            )
        response["results"] = results
    except Exception as e:
        logger.error(f"Error occurred while running: {e}")
        raise e
    return response
