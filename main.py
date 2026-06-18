import os
import time
import json
import asyncio
import logging
import re
from contextlib import asynccontextmanager
from typing import List, Dict, Any

from fastapi import FastAPI, Request, Response, HTTPException, status, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
import httpx
import redis.asyncio as aioredis
import tiktoken

# Configure Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("shieldwall")

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

# Determine default upstream based on available API Keys
DEFAULT_UPSTREAM = "https://api.openai.com/v1/chat/completions"
if os.getenv("GROQ_API_KEY"):
    DEFAULT_UPSTREAM = "https://api.groq.com/openai/v1/chat/completions"

UPSTREAM_URL = os.getenv("UPSTREAM_URL", DEFAULT_UPSTREAM)
FAIL_SAFE_MODE = os.getenv("FAIL_SAFE_MODE", "fail_open") # fail_open or fail_closed
BUDGET_LIMIT = 5.00 # $5.00 budget per 10 mins

redis_client = None
http_client = None

# Pre-compiled high-performance Regex rules for PII/Secrets detection
PII_RULES = {
    "EMAIL": re.compile(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'),
    "SSN": re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
    "CREDIT_CARD": re.compile(r'\b(?:\d[ -]*?){13,16}\b'),
    "OPENAI_KEY": re.compile(r'\bsk-(?:proj-)?[a-zA-Z0-9-_]{32,}\b'),
    "AWS_KEY": re.compile(r'\bAKIA[A-Z0-9]{16}\b'),
    "ENV_VAR": re.compile(r'\b[A-Z_]+[A-Z0-9_]*\s*=\s*[\'"]?[A-Za-z0-9/+=]{40}[\'"]?\b'),
    "AWS_SECRET_KEY": re.compile(r'\bAWS_SECRET_ACCESS_KEY\s*[:=]\s*[\'"]?[A-Za-z0-9/+=]{40}[\'"]?\b', re.IGNORECASE)
}

# Redis Lua Script to atomically check budget and clean up expired entries
CHECK_BUDGET_LUA = """
local zset_key = KEYS[1]
local sum_key = KEYS[2]
local now = tonumber(ARGV[1])

-- Clean up expired items
local expired = redis.call('ZRANGEBYSCORE', zset_key, '-inf', now)
local subtracted = 0
for _, item in ipairs(expired) do
    local cost = string.match(item, ":([^:]+)$")
    if cost then
        subtracted = subtracted + tonumber(cost)
    end
end
if #expired > 0 then
    redis.call('ZREMRANGEBYSCORE', zset_key, '-inf', now)
    local new_sum = redis.call('DECRBYFLOAT', sum_key, subtracted)
    if tonumber(new_sum) < 0 then
        redis.call('SET', sum_key, "0")
    end
end

local current_sum = tonumber(redis.call('GET', sum_key) or "0")
return current_sum
"""

# Redis Lua Script to atomically record cost and clean up expired entries
ADD_COST_LUA = """
local zset_key = KEYS[1]
local sum_key = KEYS[2]
local now = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local request_id = ARGV[3]
local window = tonumber(ARGV[4])

-- Clean up expired items first to make sure sum is accurate
local expired = redis.call('ZRANGEBYSCORE', zset_key, '-inf', now)
local subtracted = 0
for _, item in ipairs(expired) do
    local cost_val = string.match(item, ":([^:]+)$")
    if cost_val then
        subtracted = subtracted + tonumber(cost_val)
    end
end
if #expired > 0 then
    redis.call('ZREMRANGEBYSCORE', zset_key, '-inf', now)
    redis.call('DECRBYFLOAT', sum_key, subtracted)
end

-- Add new cost
redis.call('INCRBYFLOAT', sum_key, cost)
redis.call('ZADD', zset_key, now + window, request_id .. ":" .. cost)

-- Set expiration for safety
redis.call('EXPIRE', zset_key, window * 2)
redis.call('EXPIRE', sum_key, window * 2)

local current_sum = tonumber(redis.call('GET', sum_key) or "0")
if current_sum < 0 then
    current_sum = 0
    redis.call('SET', sum_key, "0")
end
return current_sum
"""

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/shieldwall")
db_pool = None
alert_task = None

ALERT_LOOP_INTERVAL = 60.0
ALERT_COOLDOWN_PERIOD = 600.0

def query_db(query: str, params: tuple = None, is_update: bool = False):
    """Executes a query against Postgres database using the connection pool or a direct connection."""
    import psycopg2
    conn = None
    try:
        if db_pool:
            conn = db_pool.getconn()
        else:
            conn = psycopg2.connect(DATABASE_URL)
        
        with conn.cursor() as cur:
            cur.execute(query, params)
            if is_update:
                conn.commit()
                return None
            else:
                if cur.description is None:
                    return []
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise e
    finally:
        if conn:
            if db_pool:
                db_pool.putconn(conn)
            else:
                conn.close()

async def regression_alert_loop():
    logger.info("Faithfulness regression alerting loop started.")
    last_alert_time = 0
    threshold = float(os.getenv("ALERT_FAITHFULNESS_THRESHOLD", "0.82"))
    
    while True:
        try:
            await asyncio.sleep(ALERT_LOOP_INTERVAL)
            
            # Query the average faithfulness in the last 10 minutes
            query = """
                SELECT 
                    COUNT(*) as count,
                    AVG(faithfulness) as avg_faith
                FROM request_logs
                WHERE timestamp >= NOW() - INTERVAL '10 minutes'
                  AND faithfulness IS NOT NULL;
            """
            rows = await asyncio.to_thread(query_db, query)
            if rows and rows[0]["count"] > 0:
                count = rows[0]["count"]
                avg_faith = float(rows[0]["avg_faith"] or 0.0)
                
                if avg_faith < threshold:
                    now = time.time()
                    if now - last_alert_time > ALERT_COOLDOWN_PERIOD:
                        last_alert_time = now
                        # Emit alert to logs
                        slack_payload = {
                            "text": f"🚨 *ShieldWall Alert: Faithfulness Regression Detected!* 🚨\n"
                                    f"The average faithfulness score over the last 10 minutes is *{avg_faith:.3f}* "
                                    f"(Threshold: *{threshold:.2f}*, Sample count: {count}).\n"
                                    f"This may indicate severe prompt drift or system degradation."
                        }
                        logger.warning(
                            f"[ALERT] [Slack Webhook Sim] Faithfulness regression detected! "
                            f"Average faithfulness in the last 10m is {avg_faith:.3f} (Threshold: {threshold:.2f}). "
                            f"Payload: {json.dumps(slack_payload)}"
                        )
        except asyncio.CancelledError:
            logger.info("Faithfulness regression alerting loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in regression alert loop: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, http_client, db_pool, alert_task
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    logger.info(f"Connecting to Redis at {redis_url}...")
    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    
    try:
        await redis_client.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        if FAIL_SAFE_MODE == "fail_closed":
            raise e
            
    # Persistent HTTP client with connection pool for ultra-low proxy overhead
    http_client = httpx.AsyncClient(timeout=60.0)
    logger.info("HTTP persistent client initialized.")

    # Initialize DB connection pool
    try:
        from psycopg2.pool import SimpleConnectionPool
        logger.info(f"Initializing DB connection pool for {DATABASE_URL}...")
        db_pool = SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
        logger.info("DB connection pool initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize DB connection pool: {e}")

    # Start the alerting background loop
    alert_task = asyncio.create_task(regression_alert_loop())
    
    yield
    
    # Cancel alert loop on shutdown
    if alert_task:
        alert_task.cancel()
        try:
            await alert_task
        except asyncio.CancelledError:
            pass
            
    # Clean up connections on shutdown
    if db_pool:
        db_pool.closeall()
        logger.info("DB connection pool closed.")
    if redis_client:
        await redis_client.close()
        logger.info("Redis client connection closed.")
    if http_client:
        await http_client.aclose()
        logger.info("HTTP persistent client connection closed.")

app = FastAPI(lifespan=lifespan)

# Custom Exception Handler to enforce X-ShieldWall-Reason header
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    headers = exc.headers or {}
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=headers
    )

def estimate_input_tokens(messages: List[Dict[str, Any]], model: str) -> int:
    """Estimates the input token count for the request payload using tiktoken."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")
        
    num_tokens = 0
    for message in messages:
        num_tokens += 4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
        for key, value in message.items():
            if isinstance(value, str):
                num_tokens += len(encoding.encode(value))
    num_tokens += 2  # every reply is primed with <im_start>assistant
    return num_tokens

def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculates the total cost of the request based on the model's token prices."""
    pricing = MODEL_PRICING.get(model, DEFAULT_PRICING)
    return (input_tokens * pricing["input"]) + (output_tokens * pricing["output"])

async def check_project_budget(project_id: str) -> float:
    """Queries Redis to get the rolling 10-minute expenditure for the project."""
    if not redis_client:
        if FAIL_SAFE_MODE == "fail_closed":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Security Gate active: Cache unavailable in fail_closed mode."
            )
        return 0.0  # Fail open
        
    zset_key = f"project:budget:10m:zset:{project_id}"
    sum_key = f"project:budget:10m:sum:{project_id}"
    now = int(time.time())
    try:
        current_sum = await redis_client.eval(CHECK_BUDGET_LUA, 2, zset_key, sum_key, now)
        return float(current_sum)
    except Exception as e:
        logger.error(f"Redis budget check error: {e}")
        if FAIL_SAFE_MODE == "fail_closed":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Security Gate active: Cache operation failed in fail_closed mode."
            )
        return 0.0  # Fail open

async def record_project_cost(project_id: str, request_id: str, cost: float) -> float:
    """Atomically adds the request's cost to the rolling 10-minute window in Redis."""
    if not redis_client:
        return 0.0
        
    zset_key = f"project:budget:10m:zset:{project_id}"
    sum_key = f"project:budget:10m:sum:{project_id}"
    now = int(time.time())
    window = 600  # 10 minutes (600 seconds)
    try:
        new_sum = await redis_client.eval(ADD_COST_LUA, 2, zset_key, sum_key, now, cost, request_id, window)
        logger.info(f"Recorded cost ${cost:.6f} for project '{project_id}'. New rolling 10m spend: ${new_sum:.6f}")
        return float(new_sum)
    except Exception as e:
        logger.error(f"Redis budget record error: {e}")
        return 0.0

# --- PII MASKING ENGINE FUNCTIONS ---
def mask_pii(text: str, project_id: str, request_id: str, mappings: Dict[str, str]) -> str:
    """Scans and masks PII in the input text, returning the masked text.
    Updates the 'mappings' dictionary with token -> original_value mapping."""
    if not text:
        return text
        
    masked_text = text
    counts = {}
    
    # Run each compiled regex rule
    for rule_name, rule_regex in PII_RULES.items():
        matches = list(set(rule_regex.findall(masked_text)))
        # Sort by length descending to prevent substring collisions
        matches.sort(key=len, reverse=True)
        
        for match in matches:
            # Check if this match was already tokenized in this request to maintain consistency
            existing_token = None
            for token, raw_val in mappings.items():
                if raw_val == match:
                    existing_token = token
                    break
                    
            if existing_token:
                token = existing_token
            else:
                counter = counts.get(rule_name, 0) + 1
                counts[rule_name] = counter
                token = f"[REDACTED_{rule_name}_{counter}]"
                mappings[token] = match
                
            masked_text = masked_text.replace(match, token)
            
    return masked_text

def unmask_text(text: str, mappings: Dict[str, str]) -> str:
    """Replaces all placeholder tokens with their raw original text values."""
    if not text or not mappings:
        return text
    unmasked = text
    for token, original in mappings.items():
        unmasked = unmasked.replace(token, original)
    return unmasked

async def save_pii_mappings(request_id: str, mappings: Dict[str, str]):
    """Stores the generated PII mappings in Redis as a hash with a 5-minute TTL."""
    if not mappings or not redis_client:
        return
    hash_key = f"trace:{request_id}:mappings"
    try:
        await redis_client.hset(hash_key, mapping=mappings)
        await redis_client.expire(hash_key, 300) # 5 minutes TTL
        logger.info(f"Saved {len(mappings)} PII mappings in Redis for trace '{request_id}' with 5m TTL.")
    except Exception as e:
        logger.error(f"Failed to save PII mappings: {e}")
        if FAIL_SAFE_MODE == "fail_closed":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Security Gate active: Failed to secure PII mapping state."
            )

async def load_pii_mappings(request_id: str) -> Dict[str, str]:
    """Loads PII token mappings for the given trace ID from Redis."""
    if not redis_client:
        return {}
    hash_key = f"trace:{request_id}:mappings"
    try:
        mappings = await redis_client.hgetall(hash_key)
        return mappings or {}
    except Exception as e:
        logger.error(f"Failed to load PII mappings: {e}")
        if FAIL_SAFE_MODE == "fail_closed":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Security Gate active: Failed to retrieve PII mapping state."
            )
        return {}

# --- ASYNC LOGING ENGINE & CELERY TASK INTEGRATION ---
def enqueue_request_log(payload: dict):
    """Fires request log payload to the Celery Redis Queue in a non-blocking background task."""
    try:
        from tasks import log_request
        log_request.delay(payload)
    except Exception as e:
        logger.error(f"Failed to push logging task to Celery: {e}")

async def log_request_background(
    project_id: str,
    request_id: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    latency_ms: int,
    http_status: int,
    violations_triggered: str,
    user_query: str = None,
    rag_context: List[str] = None,
    response_text: str = None
):
    """Background task executing cost accounting in Redis and database logging in Celery."""
    # 1. Compute Cost
    cost = calculate_cost(model, input_tokens, output_tokens)
    
    # 2. Record cost in Redis for sliding budget gates
    await record_project_cost(project_id, request_id, cost)
    
    # 3. Push to Celery queue for database ingestion and async evaluation
    enqueue_request_log({
        "request_id": request_id,
        "tenant_id": project_id,
        "timestamp": time.time(),
        "model": model,
        "upstream_provider": "OpenAI",
        "latency_ms": latency_ms,
        "http_status": http_status,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "violations_triggered": violations_triggered,
        "user_query": user_query,
        "rag_context": rag_context,
        "response_text": response_text
    })

# --- PROXY HANDLERS ---
async def handle_non_streaming_proxy(
    body: dict,
    headers: dict,
    project_id: str,
    request_id: str,
    model: str,
    input_tokens: int,
    background_tasks: BackgroundTasks,
    violations_triggered: str = "",
    user_query: str = None,
    rag_context: List[str] = None
) -> Response:
    start_time = time.perf_counter()
    try:
        response = await http_client.post(UPSTREAM_URL, json=body, headers=headers)
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        logger.info(f"Upstream non-streaming request completed in {latency_ms}ms")
    except Exception as e:
        logger.error(f"Upstream request failed: {e}")
        # Log failure asynchronously
        background_tasks.add_task(
            log_request_background,
            project_id=project_id,
            request_id=request_id,
            model=model,
            input_tokens=input_tokens,
            output_tokens=0,
            latency_ms=0,
            http_status=status.HTTP_502_BAD_GATEWAY,
            violations_triggered="UPSTREAM_ERROR",
            user_query=user_query,
            rag_context=rag_context
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to connect to upstream provider"
        )

    if response.status_code != 200:
        background_tasks.add_task(
            log_request_background,
            project_id=project_id,
            request_id=request_id,
            model=model,
            input_tokens=input_tokens,
            output_tokens=0,
            latency_ms=latency_ms,
            http_status=response.status_code,
            violations_triggered="UPSTREAM_ERROR",
            user_query=user_query,
            rag_context=rag_context
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )

    try:
        response_json = response.json()
    except Exception:
        background_tasks.add_task(
            log_request_background,
            project_id=project_id,
            request_id=request_id,
            model=model,
            input_tokens=input_tokens,
            output_tokens=0,
            latency_ms=latency_ms,
            http_status=response.status_code,
            violations_triggered="INVALID_RESPONSE",
            user_query=user_query,
            rag_context=rag_context
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )

    # Inbound post-filter: De-anonymize PII in the completion choices
    mappings = await load_pii_mappings(request_id)
    has_pii = len(mappings) > 0
    if mappings and "choices" in response_json:
        for choice in response_json["choices"]:
            message = choice.get("message", {})
            if "content" in message and message["content"]:
                message["content"] = unmask_text(message["content"], mappings)

    # Extract response text for async evaluation scoring
    response_text = ""
    if "choices" in response_json and response_json["choices"]:
        msg = response_json["choices"][0].get("message", {})
        response_text = msg.get("content", "")

    usage = response_json.get("usage", {})
    prompt_tokens = usage.get("prompt_tokens", input_tokens)
    completion_tokens = usage.get("completion_tokens", 0)

    # Trigger async logging and budget accounting via FastAPI BackgroundTasks
    violation_str = "PII_MASKED" if (violations_triggered or has_pii) else ""
    background_tasks.add_task(
        log_request_background,
        project_id=project_id,
        request_id=request_id,
        model=model,
        input_tokens=prompt_tokens,
        output_tokens=completion_tokens,
        latency_ms=latency_ms,
        http_status=response.status_code,
        violations_triggered=violation_str,
        user_query=user_query,
        rag_context=rag_context,
        response_text=response_text
    )

    # Forward upstream response headers cleanly, adjusting Content-Length
    resp_headers = dict(response.headers)
    resp_headers.pop("content-length", None)
    return JSONResponse(
        content=response_json,
        status_code=response.status_code,
        headers=resp_headers
    )

async def handle_streaming_proxy(
    body: dict,
    headers: dict,
    project_id: str,
    request_id: str,
    model: str,
    input_tokens: int,
    background_tasks: BackgroundTasks,
    violations_triggered: str = "",
    user_query: str = None,
    rag_context: List[str] = None
) -> StreamingResponse:
    start_time = time.perf_counter()
    async def generator():
        accumulated_text = []
        final_usage = None
        http_status_code = 200
        
        # Load PII mappings for boundary-safe de-anonymization
        mappings = await load_pii_mappings(request_id)
        active_tokens = list(mappings.keys())
        has_pii = len(mappings) > 0
        
        content_buffer = ""

        req = http_client.build_request("POST", UPSTREAM_URL, json=body, headers=headers)
        try:
            response = await http_client.send(req, stream=True)
            http_status_code = response.status_code
        except Exception as e:
            logger.error(f"Stream request connection failed: {e}")
            yield f"data: {json.dumps({'error': 'Failed to connect to upstream'})}\n\n"
            # Log error directly since generator is decoupled from request threads
            enqueue_request_log({
                "request_id": request_id,
                "tenant_id": project_id,
                "timestamp": time.time(),
                "model": model,
                "upstream_provider": "OpenAI",
                "latency_ms": 0,
                "http_status": status.HTTP_502_BAD_GATEWAY,
                "input_tokens": input_tokens,
                "output_tokens": 0,
                "violations_triggered": "UPSTREAM_ERROR",
                "user_query": user_query,
                "rag_context": rag_context
            })
            return

        if response.status_code != 200:
            body_content = await response.aread()
            yield body_content
            await response.aclose()
            latency_ms = int((time.perf_counter() - start_time) * 1000)
            # Log error response from provider
            enqueue_request_log({
                "request_id": request_id,
                "tenant_id": project_id,
                "timestamp": time.time(),
                "model": model,
                "upstream_provider": "OpenAI",
                "latency_ms": latency_ms,
                "http_status": response.status_code,
                "input_tokens": input_tokens,
                "output_tokens": 0,
                "violations_triggered": "UPSTREAM_ERROR",
                "user_query": user_query,
                "rag_context": rag_context
            })
            return

        try:
            async for line in response.aiter_lines():
                if not line:
                    continue

                if line.startswith("data:"):
                    data_str = line[5:].strip()
                    if data_str == "[DONE]":
                        # Flush any leftover chars in the content buffer
                        if content_buffer:
                            unmasked_flush = unmask_text(content_buffer, mappings)
                            flush_chunk = {
                                "choices": [{
                                    "index": 0,
                                    "delta": {"content": unmasked_flush},
                                    "finish_reason": None
                                }]
                            }
                            yield f"data: {json.dumps(flush_chunk)}\n\n"
                            accumulated_text.append(unmasked_flush)
                            content_buffer = ""
                        yield line + "\n"
                        continue
                        
                    try:
                        chunk_data = json.loads(data_str)
                        if "usage" in chunk_data and chunk_data["usage"]:
                            final_usage = chunk_data["usage"]
                            
                        has_content = False
                        chunk_content = ""
                        
                        if "choices" in chunk_data and chunk_data["choices"]:
                            choice = chunk_data["choices"][0]
                            delta = choice.get("delta", {})
                            if "content" in delta and delta["content"]:
                                chunk_content = delta["content"]
                                has_content = True
                                
                        if has_content and active_tokens:
                            # 1. Append content to suffix buffer
                            content_buffer += chunk_content
                            
                            # 2. Swap fully matched tokens in buffer
                            content_buffer = unmask_text(content_buffer, mappings)
                            
                            # 3. Find if the buffer ends with a prefix of any active token (split token check)
                            longest_prefix_len = 0
                            for token in active_tokens:
                                for i in range(1, len(token) + 1):
                                    prefix = token[:i]
                                    if content_buffer.endswith(prefix):
                                        if len(prefix) > longest_prefix_len:
                                            longest_prefix_len = len(prefix)
                                            
                            # 4. Yield everything before the split prefix match
                            if longest_prefix_len > 0:
                                yield_text = content_buffer[:-longest_prefix_len]
                                content_buffer = content_buffer[-longest_prefix_len:]
                            else:
                                yield_text = content_buffer
                                content_buffer = ""
                                
                            if yield_text:
                                choice["delta"]["content"] = yield_text
                                yield f"data: {json.dumps(chunk_data)}\n\n"
                                accumulated_text.append(yield_text)
                        else:
                            # If no mappings or no content, forward chunk immediately
                            yield line + "\n"
                            if has_content:
                                accumulated_text.append(chunk_content)
                                
                    except json.JSONDecodeError:
                        yield line + "\n"
                else:
                    yield line + "\n"
        finally:
            await response.aclose()
            
            # Post-stream cost and log logic (executes as part of generator teardown)
            latency_ms = int((time.perf_counter() - start_time) * 1000)
            
            prompt_tokens = input_tokens
            completion_tokens = 0

            if final_usage:
                prompt_tokens = final_usage.get("prompt_tokens", prompt_tokens)
                completion_tokens = final_usage.get("completion_tokens", 0)
            else:
                full_text = "".join(accumulated_text)
                try:
                    encoding = tiktoken.encoding_for_model(model)
                except KeyError:
                    encoding = tiktoken.get_encoding("cl100k_base")
                completion_tokens = len(encoding.encode(full_text))

            cost = calculate_cost(model, prompt_tokens, completion_tokens)
            
            # 1. Update Redis budget
            await record_project_cost(project_id, request_id, cost)
            
            # 2. Enqueue metrics payload to celery queue, carrying response text
            violation_str = "PII_MASKED" if (violations_triggered or has_pii) else ""
            enqueue_request_log({
                "request_id": request_id,
                "tenant_id": project_id,
                "timestamp": time.time(),
                "model": model,
                "upstream_provider": "OpenAI",
                "latency_ms": latency_ms,
                "http_status": http_status_code,
                "input_tokens": prompt_tokens,
                "output_tokens": completion_tokens,
                "violations_triggered": violation_str,
                "user_query": user_query,
                "rag_context": rag_context,
                "response_text": "".join(accumulated_text)
            })

    return StreamingResponse(
        generator(),
        media_type="text/event-stream"
    )

@app.post("/v1/chat/completions")
async def chat_completions(request: Request, background_tasks: BackgroundTasks):
    # 1. Enforce custom project billing header
    project_id = request.headers.get("X-Project-ID")
    if not project_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing required X-Project-ID header for rate/budget limiting"
        )

    # 2. Synchronous budget constraint check
    current_spend = await check_project_budget(project_id)
    if current_spend >= BUDGET_LIMIT:
        logger.warning(f"Blocked request for project '{project_id}': Rolling spend is ${current_spend:.4f} >= limit ${BUDGET_LIMIT:.4f}")
        
        # Log budget exceeded event asynchronously in Celery (called directly since it takes ~0.5ms)
        enqueue_request_log({
            "request_id": f"req_blocked_{int(time.time() * 1000)}",
            "tenant_id": project_id,
            "timestamp": time.time(),
            "model": "unknown",
            "upstream_provider": "OpenAI",
            "latency_ms": 0,
            "http_status": status.HTTP_429_TOO_MANY_REQUESTS,
            "input_tokens": 0,
            "output_tokens": 0,
            "violations_triggered": "BUDGET_EXCEEDED"
        })
        
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "Project rolling 10-minute budget limit exceeded."},
            headers={"X-ShieldWall-Reason": "Budget Exceeded"}
        )

    # 3. Parse JSON Body
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid request body. Content must be JSON."
        )

    # Intercept RAG evaluation metadata from body (pop to avoid forwarding to provider)
    user_query = body.pop("user_query", None)
    rag_context = body.pop("rag_context", None)

    model = body.get("model", "gpt-4o-mini")
    messages = body.get("messages", [])
    is_stream = body.get("stream", False)

    # Generate a unique Request ID (trace namespace)
    request_id = f"req_{int(time.time() * 1000)}_{os.urandom(4).hex()}"

    # Outbound filter: Mask PII in incoming messages
    mappings = {}
    masked_messages = []
    for msg in messages:
        masked_msg = dict(msg)
        content = masked_msg.get("content")
        if isinstance(content, str):
            masked_msg["content"] = mask_pii(content, project_id, request_id, mappings)
        elif isinstance(content, list):
            new_content = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    item_copy = dict(item)
                    item_copy["text"] = mask_pii(item_copy.get("text", ""), project_id, request_id, mappings)
                    new_content.append(item_copy)
                else:
                    new_content.append(item)
            masked_msg["content"] = new_content
        masked_messages.append(masked_msg)
        
    body["messages"] = masked_messages

    # Store the trace PII mapping in Redis hash with 5m TTL
    if mappings:
        await save_pii_mappings(request_id, mappings)

    # Re-estimate input tokens based on masked messages to forward correct stats
    input_tokens = estimate_input_tokens(body["messages"], model)

    # Ensure upstream returns usage in streaming mode
    if is_stream:
        if "stream_options" not in body:
            body["stream_options"] = {}
        body["stream_options"]["include_usage"] = True

    # 4. Forwarding Authorization Credentials
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        groq_key = os.getenv("GROQ_API_KEY", "")
        if groq_key:
            auth_header = f"Bearer {groq_key}"
        else:
            api_key = os.getenv("OPENAI_API_KEY", "")
            if api_key:
                auth_header = f"Bearer {api_key}"

    headers = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header

    if is_stream:
        return await handle_streaming_proxy(
            body=body,
            headers=headers,
            project_id=project_id,
            request_id=request_id,
            model=model,
            input_tokens=input_tokens,
            background_tasks=background_tasks,
            violations_triggered="PII_MASKED" if mappings else "",
            user_query=user_query,
            rag_context=rag_context
        )
    else:
        return await handle_non_streaming_proxy(
            body=body,
            headers=headers,
            project_id=project_id,
            request_id=request_id,
            model=model,
            input_tokens=input_tokens,
            background_tasks=background_tasks,
            violations_triggered="PII_MASKED" if mappings else "",
            user_query=user_query,
            rag_context=rag_context
        )

@app.get("/api/analytics")
async def get_analytics(window: str = "24h"):
    if window not in ["1h", "24h", "30d"]:
        raise HTTPException(status_code=400, detail="Invalid window parameter. Choose from 1h, 24h, 30d.")
        
    if window == "1h":
        interval = "1 hour"
    elif window == "30d":
        interval = "30 days"
    else:
        interval = "24 hours"
        
    query = """
        SELECT 
            COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0.0) as p95_latency,
            COALESCE(SUM(cost), 0.0) as total_cost,
            COALESCE(AVG(faithfulness), 0.0) as avg_faithfulness
        FROM request_logs
        WHERE timestamp >= NOW() - CAST(%s AS INTERVAL);
    """
    
    try:
        rows = await asyncio.to_thread(query_db, query, (interval,))
        if not rows:
            return {"p95_latency": 0.0, "total_cost": 0.0, "avg_faithfulness": 0.0}
        row = rows[0]
        return {
            "p95_latency": round(float(row.get("p95_latency") or 0.0), 2),
            "total_cost": round(float(row.get("total_cost") or 0.0), 6),
            "avg_faithfulness": round(float(row.get("avg_faithfulness") or 0.0), 3)
        }
    except Exception as e:
        logger.error(f"Error fetching analytics: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")

@app.get("/api/analytics/tenants")
async def get_tenant_spend(window: str = "24h"):
    if window not in ["1h", "24h", "30d"]:
        raise HTTPException(status_code=400, detail="Invalid window parameter. Choose from 1h, 24h, 30d.")
        
    if window == "1h":
        interval = "1 hour"
    elif window == "30d":
        interval = "30 days"
    else:
        interval = "24 hours"
        
    query = """
        SELECT 
            tenant_id,
            COALESCE(SUM(cost), 0.0) as total_cost
        FROM request_logs
        WHERE timestamp >= NOW() - CAST(%s AS INTERVAL)
        GROUP BY tenant_id
        ORDER BY total_cost DESC;
    """
    
    try:
        rows = await asyncio.to_thread(query_db, query, (interval,))
        results = []
        for row in rows:
            results.append({
                "tenant_id": row["tenant_id"],
                "total_cost": round(float(row["total_cost"]), 6)
            })
        return results
    except Exception as e:
        logger.error(f"Error fetching tenant spend: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")

@app.get("/health")
async def health_check():
    redis_status = "unconnected"
    if redis_client:
        try:
            await redis_client.ping()
            redis_status = "healthy"
        except Exception:
            redis_status = "unhealthy"
            
    return {
        "status": "healthy",
        "redis": redis_status,
        "fail_safe_mode": FAIL_SAFE_MODE
    }
