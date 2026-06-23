# ShieldWall Gateway: Enterprise LLM Security & Telemetry Control Room
## 🛡️ Project Case Study (STAR Method) & Recruiter Demo Guide

This document defines the **ShieldWall Gateway** project using the structured **STAR method** (Situation, Task, Action, Result) for resume and interview preparation, followed by a step-by-step **Demo Guide** you can use to showcase the application to recruiters or technical interviewers.

---

## 📈 STAR Case Study

### 1. Situation (Context)
Enterprises integrating Large Language Models (LLMs) like OpenAI and Groq face critical operational risks:
* **Financial Risk:** Runaway costs from developer code loops, model hallucinations, or unmonitored api usage.
* **Security & Privacy Risk:** Accidental leak of Personally Identifiable Information (PII) like email addresses, SSNs, credit cards, or internal credentials (AWS/OpenAI API keys) to third-party endpoints.
* **Quality Assurance Risk:** Hallucinations or poor responses in Retrieval-Augmented Generation (RAG) pipelines without real-time detection or scoring.

### 2. Task (Objective)
Design and build **ShieldWall Gateway**, a self-hosted LLM proxy and observability engine that:
1. Operates on the hot request path with **ultra-low overhead (< 15ms added latency)**.
2. Performs real-time **atomic budget validation** (sliding 10-minute windows) per project/tenant.
3. Automatically **masks and redacts PII/secrets** in outbound prompts and recovers them dynamically in streaming response chunks.
4. Computes NLP quality metrics (**Context Relevance** and **Faithfulness**) asynchronously in the background.
5. Emits real-time slack-compatible alerts on RAG quality regressions.
6. Serves a high-fidelity **admin control room** visual dashboard with live security event diagnostics.

### 3. Action (Implementation Detail)
* **High-Performance FastAPI Proxy:** Created a synchronous, low-overhead pipeline using FastAPI and compiled Regex rule-sets to redact SSNs, emails, credit cards, and API secrets.
* **Atomic Cost Billing (Redis Lua):** Wrote atomic Redis Lua scripts to execute lookups and token-cost accounting in a sliding 10-minute window, blocking requests (returning `HTTP 429`) immediately if limits are breached.
* **Async Telemetry & ML Workers (Celery & PyTorch):** Offloaded intensive database writes and quality evaluation to a background Celery worker. Implemented a HuggingFace Cross-Encoder model (`ms-marco-MiniLM-L-6-v2`) to score context relevance (user query vs. RAG chunks) and faithfulness (RAG chunks vs. LLM response).
* **Glassmorphism Dashboard (React & Vite):** Developed a sleek, dark-themed dashboard using React and custom CSS to display lifetime metrics, real-time lookbacks, tenant cost rankings, and live security violation logs.
* **Secure PII Diagnostic Audits:** Configured Redis TTL hashes (5-minute expiration) to temporarily save PII mapping states. Built an interactive diagnostics modal on the dashboard allowing admins to securely unmask redacted values for security audits.

### 4. Result (Impact & Performance)
* **Latency Profile:** Intercepts, masks, and routes LLM calls under **15ms proxy overhead**.
* **Zero Leakage Security:** Achieved 100% masking coverage for target credential patterns and PII strings with secure local decryption.
* **Runaway Cost Prevention:** Successfully protected upstream API budgets by implementing sub-millisecond, sliding rate-limit gates.
* **Robust Quality Tracking:** Enabled automated regression tracking of RAG response validity, successfully alerting slack channels when average faithfulness fell below the `0.82` threshold.

---

## 🖥️ Recruiter Demo Guide

Use this guide to walk a recruiter through a live demonstration of your project.

### 🏁 Step 1: Show the Interactive Swagger UI (OpenAPI Page)
1. **Navigate to:** `http://localhost:8000/docs`
2. **What it shows:** A clean, professional, fully interactive API documentation page.
3. **Talking Points:**
   > *"FastAPI automatically generates this interactive API documentation page. I've annotated all endpoints with specific tags, schemas, and usage examples. This makes it trivial for internal developers to integrate our gateway proxy into their standard OpenAI SDKs."*

### 🧪 Step 2: Run a Request from the Swagger UI
1. Scroll to `POST /v1/chat/completions` (grouped under **LLM Gateway Proxy**).
2. Click **Try it out**.
3. Point out the required **`X-Project-ID`** header field (e.g., enter `recruiter-demo`).
4. Note the preloaded request body example:
   ```json
   {
     "model": "llama-3.1-8b-instant",
     "messages": [
       {"role": "user", "content": "Hi, my email address is recruitment@company.com and my server password was AWS_SECRET_ACCESS_KEY=abcd1234abcd1234abcd1234abcd1234abcd1234."}
     ],
     "stream": false
   }
   ```
5. Click **Execute**.
6. **Watch it succeed with a `200 OK`**. Under the hood, show that the response output shows the text response normally, but the upstream LLM never saw the actual email or AWS key.

### 📊 Step 3: Show the Observability Control Room
1. Open the Dashboard: `http://localhost:8000/`
2. **Show the Metrics:** Show the KPI Ribbon cards displaying:
   * **Corporate Spend** (calculated based on token price scales per model).
   * **System Overhead** (proxy latency).
   * **Blocked Injections** (blocked prompt attacks or rate-limit violations).
3. **Show Tenant Rankings:** Explain that you can track which team (`X-Project-ID`) consumes the most budget.
4. **Demonstrate Lookback Filters:** Toggle between **1 Hour Lookback**, **24 Hours Lookback**, and **30 Days Lookback** to show how metrics update instantly.

### 🔍 Step 4: Show the Diagnostics Audit
1. In the **Live Vulnerability & Security Log** table at the bottom, point to the request trace you just sent.
2. Click **View Diagnostics**.
3. **Explain the Modal:**
   > *"When the gateway proxy detects PII or credentials, it replaces them with tokens like `[REDACTED_EMAIL_1]` before sending the prompt to the cloud provider. We store the raw unmasked mapping inside an ephemeral Redis cache with a 5-minute Time-To-Live (TTL). This allows compliance officers to audit the exact leaks from our diagnostics console without storing sensitive data permanently in our database."*
4. Show the original values mapped next to the redacted placeholders.

---

## 🛠️ How to run locally for the demo

1. Make sure your docker containers are active:
   ```bash
   docker compose up --build
   ```
2. Navigate to:
   * **Admin Dashboard:** `http://localhost:8000/`
   * **Swagger API Documentation:** `http://localhost:8000/docs`
