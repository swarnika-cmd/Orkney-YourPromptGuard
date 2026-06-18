import os
import sys
import json
import asyncio
import time
from typing import List, Dict, Any

# Configure environment for local testing
os.environ["REDIS_URL"] = "redis://localhost:6380/0"
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@db:5432/shieldwall"
os.environ["FAIL_SAFE_MODE"] = "fail_closed"

import httpx
from fastapi import status

# Import the FastAPI app and modules
import main
import tasks
from main import app

# --- IN-MEMORY MOCK REDIS FALLBACK ---
class MockRedis:
    def __init__(self):
        self.store = {}  # key -> value (strings, hashes, or list of tuples for zset)

    async def ping(self):
        return True

    async def delete(self, *keys):
        for key in keys:
            self.store.pop(key, None)

    async def close(self):
        pass

    async def hset(self, key, mapping):
        self.store[key] = {k: str(v) for k, v in mapping.items()}
        return len(mapping)

    async def hgetall(self, key):
        return self.store.get(key, {})

    async def expire(self, key, ttl):
        pass

    async def eval(self, script, numkeys, *args):
        keys = args[:numkeys]
        argv = args[numkeys:]

        is_add_cost = "ADD_COST_LUA" in script or "INCRBYFLOAT" in script or len(argv) >= 3
        
        if not is_add_cost:
            # CHECK_BUDGET_LUA
            zset_key = keys[0]
            sum_key = keys[1]
            now = float(argv[0])

            zset = self.store.setdefault(zset_key, [])
            expired_sum = 0.0
            new_zset = []
            
            for score, member in zset:
                if score <= now:
                    parts = member.split(":")
                    cost = float(parts[-1])
                    expired_sum += cost
                else:
                    new_zset.append((score, member))
            self.store[zset_key] = new_zset

            if expired_sum > 0:
                current_sum = float(self.store.get(sum_key, "0"))
                new_sum = max(0.0, current_sum - expired_sum)
                self.store[sum_key] = str(new_sum)

            return float(self.store.get(sum_key, "0"))
        else:
            # ADD_COST_LUA
            zset_key = keys[0]
            sum_key = keys[1]
            now = float(argv[0])
            cost = float(argv[1])
            request_id = argv[2]
            window = float(argv[3])

            zset = self.store.setdefault(zset_key, [])
            expired_sum = 0.0
            new_zset = []
            
            for score, member in zset:
                if score <= now:
                    parts = member.split(":")
                    cost_val = float(parts[-1])
                    expired_sum += cost_val
                else:
                    new_zset.append((score, member))
            
            current_sum = float(self.store.get(sum_key, "0"))
            if expired_sum > 0:
                current_sum = max(0.0, current_sum - expired_sum)
                
            current_sum += cost
            self.store[sum_key] = str(current_sum)

            new_zset.append((now + window, f"{request_id}:{cost}"))
            self.store[zset_key] = new_zset

            return current_sum

# --- IN-MEMORY MOCK POSTGRES FALLBACK ---
postgres_db_store = []

class MockPostgresCursor:
    def __init__(self, db_store):
        self.db_store = db_store
        
    def execute(self, sql, params=None):
        # Record the execute statement and params
        self.db_store.append((sql, params))
        
    def close(self):
        pass
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class MockPostgresConnection:
    def __init__(self, db_store):
        self.db_store = db_store
        
    def cursor(self):
        return MockPostgresCursor(self.db_store)
        
    def commit(self):
        pass
        
    def rollback(self):
        pass
        
    def close(self):
        pass

# Mocked HTTP responses for upstream calls
class MockResponse:
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code
        self.headers = {"content-type": "application/json"}
        
    def json(self):
        return self._json_data
        
    @property
    def content(self):
        return json.dumps(self._json_data).encode("utf-8")

class MockStreamResponse:
    def __init__(self, lines: List[bytes], status_code=200):
        self.lines = lines
        self.status_code = status_code
        self.headers = {"content-type": "text/event-stream"}
        
    async def aiter_lines(self):
        for line in self.lines:
            yield line.decode("utf-8")
            await asyncio.sleep(0.001)  # Fast streaming
            
    async def aclose(self):
        pass
        
    async def aread(self):
        return b"".join(self.lines)

# Outbound payloads for validation
outbound_payload_with_pii = {
    "model": "gpt-4",
    "messages": [
        {
            "role": "user",
            "content": "Contact me at user@example.com or call 123-45-6789. Also my server has AWS_SECRET_ACCESS_KEY=abcd1234abcd1234abcd1234abcd1234abcd1234. Keep it safe!"
        }
    ]
}

# Upstream mock output for outbound validation
mocked_received_payload = {}

# Non-streaming OpenAI mock response containing token placeholders
mock_pii_token_reply = {
    "id": "chatcmpl-piimock",
    "object": "chat.completion",
    "created": int(time.time()),
    "model": "gpt-4",
    "choices": [
        {
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Understood. Replaced your email with [REDACTED_EMAIL_1], SSN with [REDACTED_SSN_1], and credential with [REDACTED_ENV_VAR_1]."
            },
            "finish_reason": "stop"
        }
    ],
    "usage": {
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_tokens": 150
    }
}

# Stream responses containing PII tokens
mock_stream_tokens = [
    b'data: {"choices":[{"delta":{"role":"assistant"}}]}\n',
    b'data: {"choices":[{"delta":{"content":"Sure, unmasked value is "}}]}\n',
    b'data: {"choices":[{"delta":{"content":"[REDACTED_EMAIL_1]"}}]}\n',
    b'data: {"choices":[{"delta":{"content":" and your token was [REDACTED_SSN_1]"}}]}\n',
    b'data: [DONE]\n'
]

# Stream responses where PII tokens are split across chunks (boundary testing)
mock_stream_split_tokens = [
    b'data: {"choices":[{"delta":{"role":"assistant"}}]}\n',
    b'data: {"choices":[{"delta":{"content":"Contacting "}}]}\n',
    b'data: {"choices":[{"delta":{"content":"[RED"}}]}\n',
    b'data: {"choices":[{"delta":{"content":"ACTED_EM"}}]}\n',
    b'data: {"choices":[{"delta":{"content":"AIL_1] now."}}]}\n',
    b'data: [DONE]\n'
]

class MockCrossEncoder:
    def predict(self, pairs: List[List[str]]) -> List[float]:
        logits = []
        for prompt, response in pairs:
            # Distinguish between relevance and faithfulness by query content
            if "query" in prompt:
                # Relevance: (user_query, chunk)
                if "chunk1" in response:
                    logits.append(1.0)
                elif "chunk2" in response:
                    logits.append(2.0)
                else:
                    logits.append(0.0)
            else:
                # Faithfulness: (chunk, response)
                if "chunk1" in prompt:
                    logits.append(-1.0)
                elif "chunk2" in prompt:
                    logits.append(1.5)
                else:
                    logits.append(0.0)
        return logits

async def clear_redis_keys(redis_client, project_id: str):
    await redis_client.delete(f"project:budget:10m:zset:{project_id}")
    await redis_client.delete(f"project:budget:10m:sum:{project_id}")


async def run_tests():
    global mocked_received_payload, postgres_db_store
    print("=== STARTING SHIELDWALL PHASE 3 VERIFICATION ===")
    
    # Configure Celery in eager mode to run tasks synchronously in tests
    tasks.celery_app.conf.task_always_eager = True
    tasks.celery_app.conf.task_ignore_result = True
    
    # Patch psycopg2 in tasks to use our MockPostgres
    def mock_connect(*args, **kwargs):
        return MockPostgresConnection(postgres_db_store)
    tasks.psycopg2.connect = mock_connect

    # 1. Establish Redis mode
    import redis.asyncio as aioredis
    use_mock_redis = False
    
    try:
        r = aioredis.from_url(os.environ["REDIS_URL"])
        await r.ping()
        await r.close()
        print("[REDIS] Successfully connected to local Redis.")
    except Exception:
        use_mock_redis = True
        print("[REDIS] Local Redis server not detected. Patching main with in-memory MockRedis.")
        
    mock_redis_client = MockRedis() if use_mock_redis else None
    
    if use_mock_redis:
        def mock_from_url(*args, **kwargs):
            return mock_redis_client
        main.aioredis.from_url = mock_from_url
        active_redis_client = mock_redis_client
    else:
        active_redis_client = aioredis.from_url(os.environ["REDIS_URL"])

    project_a = "project_analytics_test"
    await clear_redis_keys(active_redis_client, project_a)
    
    # Initialize FastAPI application lifespan
    async with main.lifespan(app):
        
        # Mock upstream POST interceptor to inspect forwarded JSON
        async def mock_post(url, json, headers):
            global mocked_received_payload
            mocked_received_payload = json
            return MockResponse(mock_pii_token_reply)
            
        def mock_build_request(method, url, json, headers):
            return {"method": method, "url": url, "json": json, "headers": headers}
            
        current_stream_chunks = mock_stream_tokens
        
        async def mock_send(req, stream=False):
            global mocked_received_payload
            mocked_received_payload = req["json"]
            return MockStreamResponse(current_stream_chunks)
            
        # Patch the HTTP client
        main.http_client.post = mock_post
        main.http_client.build_request = mock_build_request
        main.http_client.send = mock_send

        # Test client running ASGI
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
            
            # --- TEST 1: Database Migration Schema Initialisation ---
            print("\n--- Test 1: Database Migration Schema Initialisation ---")
            postgres_db_store.clear()
            tasks.init_db()
            assert len(postgres_db_store) > 0
            migration_sql = postgres_db_store[0][0]
            assert "CREATE TABLE IF NOT EXISTS request_logs" in migration_sql
            assert "idx_tenant_timestamp" in migration_sql
            assert "idx_timestamp_latency" in migration_sql
            print("[OK] Success: schema migrations applied schema structure and time-series composite indexes.")

            # --- TEST 2: Non-streaming Metrics Logging ---
            print("\n--- Test 2: Non-streaming Metrics Logging ---")
            postgres_db_store.clear()
            headers = {"X-Project-ID": project_a}
            
            resp = await client.post("/v1/chat/completions", json=outbound_payload_with_pii, headers=headers)
            assert resp.status_code == 200
            
            # Wait briefly to let background task execute eager Celery task
            await asyncio.sleep(0.05)
            
            # Find the INSERT query in postgres_db_store
            insert_queries = [q for q in postgres_db_store if "INSERT INTO request_logs" in q[0]]
            assert len(insert_queries) == 1, "Should record exactly one insert command"
            
            sql, params = insert_queries[0]
            # Params order: request_id, tenant_id, timestamp, model, upstream_provider, latency_ms, http_status, input_tokens, output_tokens, cost, violations_triggered
            print(f"Recorded parameters: {params}")
            
            assert params[1] == project_a
            assert params[3] == "gpt-4"
            assert params[6] == 200
            assert params[7] == 100 # prompt_tokens from mock usage
            assert params[8] == 50  # completion_tokens from mock usage
            assert abs(float(params[9]) - 0.0060) < 0.0001 # 100 * 30/1M + 50 * 60/1M = 0.003 + 0.003 = 0.0060
            assert "PII_MASKED" in params[10] # Masking was triggered
            print("[OK] Success: Non-streaming request recorded correctly in database.")

            # --- TEST 3: Streaming Metrics Logging ---
            print("\n--- Test 3: Streaming Metrics Logging ---")
            postgres_db_store.clear()
            
            stream_payload = {
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Email me user@example.com"}],
                "stream": True
            }
            
            resp = await client.post("/v1/chat/completions", json=stream_payload, headers=headers)
            assert resp.status_code == 200
            
            # Read the stream content to completion
            async for _ in resp.aiter_text():
                pass
                
            await asyncio.sleep(0.05)
            
            insert_queries = [q for q in postgres_db_store if "INSERT INTO request_logs" in q[0]]
            assert len(insert_queries) == 1
            sql, params = insert_queries[0]
            print(f"Recorded stream parameters: {params}")
            
            assert params[1] == project_a
            assert params[3] == "gpt-4"
            assert params[6] == 200
            assert "PII_MASKED" in params[10]
            print("[OK] Success: Streaming request recorded correctly in database.")

            # --- TEST 4: Budget Limit Exceeded Block Logging ---
            print("\n--- Test 4: Budget Limit Exceeded Block Logging ---")
            postgres_db_store.clear()
            
            # Artificially set project_a rolling budget sum to exceed 5.00 in Redis
            zset_key = f"project:budget:10m:zset:{project_a}"
            sum_key = f"project:budget:10m:sum:{project_a}"
            if use_mock_redis:
                mock_redis_client.store[sum_key] = "6.50"
            else:
                import redis.asyncio as aioredis
                temp_r = aioredis.from_url(os.environ["REDIS_URL"])
                await temp_r.set(sum_key, "6.50")
                await temp_r.close()
                
            # Fire request for project_a which should be instantly blocked
            resp = await client.post("/v1/chat/completions", json=stream_payload, headers=headers)
            assert resp.status_code == status.HTTP_429_TOO_MANY_REQUESTS
            
            await asyncio.sleep(0.05)
            
            insert_queries = [q for q in postgres_db_store if "INSERT INTO request_logs" in q[0]]
            assert len(insert_queries) == 1
            sql, params = insert_queries[0]
            print(f"Recorded budget gate block parameters: {params}")
            
            assert params[1] == project_a
            assert params[6] == 429
            assert params[10] == "BUDGET_EXCEEDED"
            print("[OK] Success: Blocked request correctly recorded as BUDGET_EXCEEDED in Postgres database.")

            # --- TEST 5: Automated Async Evaluation Engine (Phase 4) ---
            print("\n--- Test 5: Automated Async Evaluation Engine (Phase 4) ---")
            postgres_db_store.clear()
            await clear_redis_keys(active_redis_client, project_a)

            # Assign our MockCrossEncoder to tasks.cross_encoder_model
            tasks.cross_encoder_model = MockCrossEncoder()

            rag_payload = {
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Tell me about the retrieval chunks."}],
                "user_query": "test query",
                "rag_context": ["chunk1", "chunk2"],
                "stream": False
            }

            resp = await client.post("/v1/chat/completions", json=rag_payload, headers=headers)
            assert resp.status_code == 200

            # Wait briefly to let background tasks/Celery run
            await asyncio.sleep(0.05)

            # Verify that user_query and rag_context were NOT forwarded to the upstream LLM
            assert "user_query" not in mocked_received_payload
            assert "rag_context" not in mocked_received_payload
            print("[OK] Success: user_query and rag_context popped and not forwarded upstream.")

            # Find the INSERT query in postgres_db_store
            insert_queries = [q for q in postgres_db_store if "INSERT INTO request_logs" in q[0]]
            assert len(insert_queries) == 1, "Should record exactly one insert command"
            print("[OK] Success: Request log inserted.")

            # Find the UPDATE query in postgres_db_store representing the quality evaluation
            update_queries = [q for q in postgres_db_store if "UPDATE request_logs" in q[0]]
            assert len(update_queries) == 1, f"Should record exactly one update command, got {len(update_queries)}"
            
            sql, params = update_queries[0]
            print(f"Recorded update parameters: {params}")
            # Params format: (context_relevance, faithfulness, request_id)
            assert params[0] == 0.806
            assert params[1] == 0.818
            # The request_id should match the one inserted in the INSERT query
            insert_sql, insert_params = insert_queries[0]
            assert params[2] == insert_params[0] # request_id is the first param in INSERT
            print("[OK] Success: Relevance (0.806) and Faithfulness (0.818) correctly calculated and updated in Postgres.")

    if not use_mock_redis:
        await active_redis_client.close()

    print("\n=== ALL VERIFICATION TESTS PASSED SUCCESSFULLY! ===")

if __name__ == "__main__":
    try:
        asyncio.run(run_tests())
    except Exception as e:
        print(f"\n❌ Unexpected test failure: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
