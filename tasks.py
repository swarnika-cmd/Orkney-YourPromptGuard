import os
import logging
import math
import psycopg2
from typing import List, Tuple
from celery import Celery
from celery.signals import worker_ready

# Configure Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("shieldwall_worker")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/shieldwall")

# Initialize Celery Application
celery_app = Celery("shieldwall", broker=REDIS_URL, backend=REDIS_URL)

# Configure Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

# Model Pricing: Cost per token (in USD)
MODEL_PRICING = {
    # OpenAI models
    "gpt-4o": {"input": 5.00 / 1_000_000, "output": 15.00 / 1_000_000},
    "gpt-4o-mini": {"input": 0.150 / 1_000_000, "output": 0.600 / 1_000_000},
    "gpt-3.5-turbo": {"input": 0.50 / 1_000_000, "output": 1.50 / 1_000_000},
    "gpt-4-turbo": {"input": 10.00 / 1_000_000, "output": 30.00 / 1_000_000},
    "gpt-4": {"input": 30.00 / 1_000_000, "output": 60.00 / 1_000_000},
    # Groq models
    "llama3-8b-8192": {"input": 0.05 / 1_000_000, "output": 0.08 / 1_000_000},
    "llama3-70b-8192": {"input": 0.59 / 1_000_000, "output": 0.79 / 1_000_000},
    "llama-3.1-8b-instant": {"input": 0.05 / 1_000_000, "output": 0.08 / 1_000_000},
    "llama-3.1-70b-versatile": {"input": 0.59 / 1_000_000, "output": 0.79 / 1_000_000},
    "mixtral-8x7b-32768": {"input": 0.24 / 1_000_000, "output": 0.24 / 1_000_000},
    "gemma2-9b-it": {"input": 0.20 / 1_000_000, "output": 0.20 / 1_000_000},
}
DEFAULT_PRICING = {"input": 10.00 / 1_000_000, "output": 30.00 / 1_000_000}

# Global CrossEncoder Instance
cross_encoder_model = None

def get_cross_encoder():
    """Lazily loads or returns the global CrossEncoder model."""
    global cross_encoder_model
    if cross_encoder_model is None:
        try:
            from sentence_transformers import CrossEncoder
            logger.info("Loading Cross-Encoder model (cross-encoder/ms-marco-MiniLM-L-6-v2)...")
            cross_encoder_model = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
            logger.info("Cross-Encoder model loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load Cross-Encoder model: {e}")
    return cross_encoder_model

def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculates the absolute cost of the request based on model pricing."""
    pricing = MODEL_PRICING.get(model, DEFAULT_PRICING)
    return (input_tokens * pricing["input"]) + (output_tokens * pricing["output"])

def sigmoid(x: float) -> float:
    """Standard sigmoid activation mapping raw logit scores to [0.0, 1.0]."""
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def evaluate_rag_metrics(user_query: str, rag_context: List[str], response_text: str) -> Tuple[float, float]:
    """Uses Cross-Encoder to compute Context Relevance and Faithfulness scores."""
    model = get_cross_encoder()
    if not model or not rag_context:
        return 0.0, 0.0

    # 1. Context Relevance: score (user_query, context_chunk) pairs
    try:
        relevance_pairs = [(user_query, chunk) for chunk in rag_context]
        relevance_logits = model.predict(relevance_pairs)
        # Handle single float output if rag_context contains only 1 element
        if isinstance(relevance_logits, (int, float)):
            relevance_logits = [relevance_logits]
        relevance_scores = [sigmoid(float(logit)) for logit in relevance_logits]
        context_relevance = sum(relevance_scores) / len(relevance_scores)
    except Exception as e:
        logger.error(f"Failed to score context relevance: {e}")
        context_relevance = 0.0

    # 2. Faithfulness: score (context_chunk, response_text) pairs
    try:
        faithfulness_pairs = [(chunk, response_text) for chunk in rag_context]
        faithfulness_logits = model.predict(faithfulness_pairs)
        if isinstance(faithfulness_logits, (int, float)):
            faithfulness_logits = [faithfulness_logits]
        faithfulness_scores = [sigmoid(float(logit)) for logit in faithfulness_logits]
        # Max score indicates if at least one chunk grounds the response
        faithfulness = max(faithfulness_scores)
    except Exception as e:
        logger.error(f"Failed to score faithfulness: {e}")
        faithfulness = 0.0

    return round(context_relevance, 3), round(faithfulness, 3)

def init_db():
    """Initializes the database by executing our time-series logs migration schema."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    if not os.path.exists(schema_path):
        logger.warning(f"schema.sql not found at {schema_path}. Skipping automatic migration.")
        return
        
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            conn.commit()
            logger.info("PostgreSQL database migrations applied successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to apply SQL migrations: {e}")
        raise e
    finally:
        conn.close()

# Auto-apply database migration schema and preload ML models on worker ready
@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info("Celery worker process ready. Syncing Postgres database schema...")
    try:
        init_db()
    except Exception as e:
        logger.critical(f"Database migration aborted during startup: {e}")
        
    # Preload the Cross-Encoder model
    try:
        get_cross_encoder()
    except Exception as e:
        logger.warning(f"Could not preload Cross-Encoder model at startup: {e}")

@celery_app.task(name="tasks.evaluate_request_quality")
def evaluate_request_quality(request_id: str, user_query: str, rag_context: List[str], response_text: str) -> str:
    """Background task evaluating context relevance and faithfulness, persisting results to Postgres."""
    if not user_query or not rag_context or not response_text:
        logger.info(f"Skipping evaluation for request '{request_id}': missing user_query, rag_context, or response_text.")
        return request_id

    logger.info(f"Starting async quality evaluation for request '{request_id}'...")
    context_relevance, faithfulness = evaluate_rag_metrics(user_query, rag_context, response_text)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE request_logs
                SET context_relevance = %s, faithfulness = %s
                WHERE request_id = %s;
                """,
                (context_relevance, faithfulness, request_id)
            )
            conn.commit()
            logger.info(f"Updated request '{request_id}' quality scores: relevance={context_relevance:.3f}, faithfulness={faithfulness:.3f}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save evaluation scores for request '{request_id}': {e}")
        raise e
    finally:
        conn.close()

    return request_id

@celery_app.task(name="tasks.log_request")
def log_request(payload: dict) -> str:
    """Consumes request details, calculates metrics, and persists to PostgreSQL."""
    request_id = payload["request_id"]
    tenant_id = payload["tenant_id"]
    timestamp = payload["timestamp"]  # epoch float
    model = payload["model"]
    upstream_provider = payload["upstream_provider"]
    latency_ms = payload["latency_ms"]
    http_status = payload["http_status"]
    input_tokens = payload["input_tokens"]
    output_tokens = payload["output_tokens"]
    violations_triggered = payload.get("violations_triggered", "")

    # Calculate absolute financial cost
    cost = calculate_cost(model, input_tokens, output_tokens)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO request_logs (
                    request_id, tenant_id, timestamp, model, upstream_provider,
                    latency_ms, http_status, input_tokens, output_tokens, cost, violations_triggered
                ) VALUES (%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id) DO NOTHING;
                """,
                (
                    request_id,
                    tenant_id,
                    timestamp,
                    model,
                    upstream_provider,
                    latency_ms,
                    http_status,
                    input_tokens,
                    output_tokens,
                    cost,
                    violations_triggered
                )
            )
            conn.commit()
            logger.info(f"Successfully logged metric event {request_id} for tenant '{tenant_id}' (cost: ${cost:.6f})")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to commit metrics for request {request_id}: {e}")
        raise e
    finally:
        conn.close()

    # Trigger evaluation asynchronously if RAG metadata is available
    user_query = payload.get("user_query")
    rag_context = payload.get("rag_context")
    response_text = payload.get("response_text")

    if user_query and rag_context and response_text:
        evaluate_request_quality.delay(request_id, user_query, rag_context, response_text)

    return request_id
