import os
import logging
import psycopg2
from celery import Celery
from celery.signals import worker_ready

# Configure Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("shieldwall_worker")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/shieldwall")

# Initialize Celery Application
celery_app = Celery("shieldwall", broker=REDIS_URL, backend=REDIS_URL)

# Configure Celery (enable standard serialization)
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

def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculates the absolute cost of the request based on model pricing."""
    pricing = MODEL_PRICING.get(model, DEFAULT_PRICING)
    return (input_tokens * pricing["input"]) + (output_tokens * pricing["output"])

def init_db():
    """Initializes the database by executing our time-series logs migration schema."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    if not os.path.exists(schema_path):
        logger.warning(f"schema.sql not found at {schema_path}. Skipping automatic migration.")
        return
        
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    # Parse parameters from DATABASE_URL or let psycopg2 resolve it directly
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

# Auto-apply database migration schema when worker is ready
@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info("Celery worker process ready. Syncing Postgres database schema...")
    try:
        init_db()
    except Exception as e:
        logger.critical(f"Database migration aborted during startup: {e}")

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

    return request_id
