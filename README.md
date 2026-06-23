# 🛡️ ShieldWall Gateway: Enterprise LLM Security & Telemetry Control Room

![ShieldWall Gateway Dashboard](dashboard.png)
ShieldWall Gateway is a high-throughput, self-hosted, operational LLM proxy, data-redaction engine, and real-time observability control room. It provides robust perimeter defense for enterprise LLM integrations—protecting infrastructure budgets via atomic token-based rate limits, redacting sensitive PII/secrets before they reach public APIs, and validating generation quality in real-time.

All operations—from security filtering to full-stack dashboard visualization—are served from a unified, single-port gateway layout with an asynchronous background evaluation queue.

---

## 🏗️ System Architecture Topology

The diagram below details the end-to-end telemetry and request processing path of ShieldWall. 

```mermaid
graph TD
    classDef external fill:#2c2c2c,stroke:#444,stroke-width:1px,color:#fff;
    classDef gateway fill:#003366,stroke:#0055ff,stroke-width:2px,color:#fff;
    classDef storage fill:#2a4d2a,stroke:#3b7a3b,stroke-width:2px,color:#fff;
    classDef async_queue fill:#6b4423,stroke:#a66a38,stroke-width:2px,color:#fff;

    Client[Client App / LLM Consumer]:::external
    Admin[Admin Cockpit Browser]:::external
    Upstream[Upstream LLM Provider <br> OpenAI / Groq]:::external

    subgraph ShieldWall Infrastructure
        Gateway[FastAPI Proxy Server <br> :8000]:::gateway
        Redis[(Redis Cache & Broker <br> :6380)]:::storage
        Postgres[(PostgreSQL DB <br> :5435)]:::storage
        Celery[Celery Worker Container]:::async_queue
        CrossEncoder[Cross-Encoder ML Model <br> ms-marco-MiniLM-L-6-v2]:::async_queue
    end

    %% Sync Request Path
    Client -->|1. Chat Completion Request| Gateway
    Gateway <-->|2. Lua Atomic Budget Check| Redis
    Gateway -->|3. Regex Outbound PII Masking| Gateway
    Gateway -->|4. Store Trace Mappings <br> 5m TTL| Redis
    Gateway -->|5. Forward Clean Payload| Upstream
    Upstream -->|6. Encrypted Stream/Response Chunks| Gateway
    Gateway -->|7. Boundary-Matching De-Anonymization| Gateway
    Gateway -->|8. Clean Decrypted Response| Client

    %% Async Telemetry & Eval Path
    Gateway -->|9. Dispatch Background Logging Task| Celery
    Celery <-->|10. Read/Write Trace Metadata| Redis
    Celery -->|11. Compute Token Cost & Migrations| Postgres
    Celery -->|12. Evaluate Context Relevance & Faithfulness| CrossEncoder
    CrossEncoder -->|13. Persist Float Evaluation Metrics| Postgres

    %% Admin Dashboard View
    Admin -->|Dashboard Static Assets| Gateway
    Admin -->|API Queries: Analytics / Logs / Summary| Gateway
    Gateway -->|Query Rolling Aggregates| Postgres
    Gateway -->|Unmask Diagnostics Logs| Redis
```

---

## 🔄 Synchronous Request Pipeline (PII Masking & Streaming)

The request pipeline performs deep filtering on the input prompt and performs streaming boundary unmasking on outbound chunks, keeping overhead under **15ms**.

```mermaid
sequenceDiagram
    autonumber
    actor Client as LLM Client
    participant Proxy as FastAPI Gateway
    participant Cache as Redis Cache
    participant LLM as Upstream LLM (OpenAI/Groq)

    Client->>Proxy: POST /v1/chat/completions (with PII & custom RAG metadata)
    Proxy->>Cache: Evaluate check_budget_lua (10m window)
    alt Budget Exceeded (Lifetime or Window Limit)
        Cache-->>Proxy: Return current spend > limit
        Proxy-->>Client: HTTP 429: Budget limit exceeded (X-ShieldWall-Reason)
    else Budget Approved
        Cache-->>Proxy: Return current spend within limit
        Proxy->>Proxy: Run Regex PII masking rules (Email, SSN, Secrets)
        Proxy->>Cache: Save original PII mappings (trace:{id}:mappings) [5m TTL]
        Proxy->>Proxy: Strip custom RAG metadata (user_query, rag_context)
        Proxy->>LLM: Forward sanitized chat completions request
        alt Streaming Response
            loop For each chunk
                LLM->>Proxy: Yield stream chunk (contains masked tokens)
                Proxy->>Proxy: Match boundary buffer and replace tokens with original values
                Proxy-->>Client: Yield de-anonymized stream chunk
            end
        else Non-Streaming Response
            LLM->>Proxy: Return full JSON response payload
            Proxy->>Proxy: Replace masked tokens with original values
            Proxy-->>Client: Return de-anonymized JSON response
        end
    end
```

---

## 📊 Telemetry & Async Quality Evaluation Engine

Telemetry ingestion, pricing calculation, and NLP quality evaluation are offloaded from the hot request path using **Celery** to ensure zero performance degradation.

```mermaid
flowchart LR
    classDef step fill:#1e1e24,stroke:#3b3b44,stroke-width:1px,color:#fff;
    classDef action fill:#0a3641,stroke:#0f5f72,stroke-width:1.5px,color:#fff;
    classDef storage fill:#213b21,stroke:#336933,stroke-width:1.5px,color:#fff;

    A[FastAPI Response Complete]:::step -->|Enqueue| B(FastAPI BackgroundTasks Dispatcher):::action
    B -->|Fast Fire-and-Forget Enqueue| C{Redis Queue Broker}:::action
    C -->|Pulls Task| D(Celery Worker):::action
    
    subgraph Celery Tasks Pipeline
        D --> E[Calculate Token Count & Pricing]:::step
        E --> F[Persist Audit Trail Record in Postgres]:::storage
        F --> G[Check RAG Evaluation Fields]:::step
        G -->|If query, context & response present| H(Sentence-Transformers Cross-Encoder):::action
        H --> I[Evaluate Context Relevance: Query vs RAG Chunks]:::step
        H --> J[Evaluate Faithfulness: RAG Chunks vs Response Text]:::step
        I & J --> K[Update Record with relevance & faithfulness floats]:::storage
    end
```

---

## 🎨 Visual Design & Dashboard Cockpit

The Admin Cockpit is built with premium **glassmorphism** aesthetics to provide a command-center user experience:

*   **Dark Mode Palette**: Seamlessly blending rich obsidian backgrounds, glowing indicators, and neon accents (electric blue, warning pink, nominal emerald).
*   **KPI Status Ribbon**: High-visibility metric cards indicating Total Corporate Spend, System Overhead (added proxy latency), and Total Blocked Injections.
*   **Financial Consumption Charting**: Custom responsive bar charts mapping and ranking financial token consumption grouped by tenant sub-identities (`GROUP BY tenant_id`).
*   **Live Vulnerability Table**: Auto-refreshing security events display with diagnostic buttons.
*   **Diagnostics Modal**: Interactive overlays allowing administrators to unmask masked string values (retrieved securely from Redis TTL storage) for diagnostic audits.

---

## 🗄️ Database Time-Series Schema

The PostgreSQL schema (`schema.sql`) records analytical data and is optimized for rolling metrics lookbacks and latency distribution scans:

```sql
CREATE TABLE IF NOT EXISTS request_logs (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) UNIQUE NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    model VARCHAR(64) NOT NULL,
    upstream_provider VARCHAR(64) NOT NULL,
    latency_ms INTEGER NOT NULL,
    http_status INTEGER NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cost NUMERIC(10, 6) NOT NULL,
    violations_triggered VARCHAR(256),
    context_relevance NUMERIC(4, 3),
    faithfulness NUMERIC(4, 3)
);

-- Optimize for dashboard aggregations filtering by tenant over time
CREATE INDEX IF NOT EXISTS idx_tenant_timestamp ON request_logs(tenant_id, timestamp DESC);

-- Optimize for dashboard performance KPIs (e.g. latency distributions over time)
CREATE INDEX IF NOT EXISTS idx_timestamp_latency ON request_logs(timestamp DESC, latency_ms);
```

---

## ⚙️ Running Locally with Docker Compose

### Prerequisites
*   Docker & Docker Compose installed.
*   An OpenAI API Key or Groq API Key added to your `.env` file at the project root.

### Steps to Run
1.  **Configure Environment Variables**:
    Create a `.env` file at the root of the workspace:
    ```bash
    OPENAI_API_KEY=sk-proj-your-key-here
    GROQ_API_KEY=gsk_your-key-here
    FAIL_SAFE_MODE=fail_open # fail_open or fail_closed
    ```
2.  **Launch the Containers**:
    Run the following command to build and launch the ShieldWall network:
    ```bash
    docker compose up --build
    ```
    *Note: If port `5432` is already allocated by an active PostgreSQL server on your host machine, the database container will automatically bind to port `5435` on localhost, preventing configuration conflicts.*

3.  **Access the Applications**:
    *   **Admin Dashboard Cockpit**: [http://localhost:8000/](http://localhost:8000/)
    *   **Interactive Swagger API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
    *   **LLM Gateway Endpoint**: `http://localhost:8000/v1/chat/completions` (Compatible with any standard OpenAI SDK wrapper)
    *   **API Telemetry Endpoints**:
        *   Summary Metrics: `GET http://localhost:8000/api/analytics/summary`
        *   Window Analytics: `GET http://localhost:8000/api/analytics?window=24h`
        *   Tenant Rankings: `GET http://localhost:8000/api/analytics/tenants?window=24h`
        *   Security Logs: `GET http://localhost:8000/api/security/logs`
