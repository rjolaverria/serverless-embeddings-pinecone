import psycopg2
import os
import logging


class EmbeddingsStore:
    """A class to store and retrieve embeddings from a PostgreSQL database that has the `pgvector` extension installed"""

    def __init__(self):
        self.db_host = os.environ.get("DB_HOST") or "localhost"
        self.db_port = os.environ.get("DB_PORT") or 5432
        self.db_name = os.environ.get("DB_NAME") or "embeddings"
        self.db_user = os.environ.get("DB_USER") or ""
        self.db_password = os.environ.get("DB_PASSWORD") or ""
        self.vector_size = os.environ.get("VECTOR_SIZE") or 1536
        self.conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        # create the table if it doesn't exist
        # not ideal to run on every class instantiation,
        # but it's fine for this example's testing purposes
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS embeddings (
                    id SERIAL PRIMARY KEY, 
                    text TEXT, source_url TEXT NOT NULL, 
                    embedding VECTOR({self.vector_size}) NOT NULL, 
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def store_embedding(self, text: str, embedding: str, source_url: str = ""):
        """Store the embedding in the database if there is no embedding with at least 99% similarity"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 - (embedding <=> %s) as similarity FROM embeddings WHERE 1 - (embedding <=> %s) > 0.98",
                    (embedding, embedding),
                )
                result = cur.fetchone()
                if result is None:
                    cur.execute(
                        f"INSERT INTO embeddings (text, source_url, embedding) VALUES (%s, %s, %s)",
                        (text, source_url, embedding),
                    )
                    self.conn.commit()
                else:
                    self.logger.warning(
                        f'Skipping - Similar embedding found for this text from source: "{source_url}" with similarity: {result[0] * 100:.2f}%.'
                    )
        except Exception as e:
            self.logger.error(f"Error storing embedding: {e}")
            raise e

    def get_by_cosine_similarity(
        self, embedding: str, threshold: float = 0.7, limit: int = 5
    ) -> list[tuple[str, str, float]]:
        """Gets the embeddings from the database by cosine similarity"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT text, source_url, 1 - (embedding <=> %s) as similarity
                    FROM embeddings
                    WHERE 1 - (embedding <=> %s) > %s
                    ORDER BY similarity DESC
                    LIMIT %s
                    """,
                    (embedding, embedding, threshold, limit),
                )
                results = cur.fetchall()  # Fetch the results within the `with` block
            return results
        except Exception as e:
            self.logger.error(f"Error retrieving embeddings: {e}")
            raise e

    def close(self):
        """Close the database connection"""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
