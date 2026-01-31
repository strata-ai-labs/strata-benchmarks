# Future: Enterprise Support Agent Dataset

**Status**: Planned, not yet implemented.

This document describes a comprehensive benchmark dataset simulating an enterprise technical support agent for Salesforce/ERP integration troubleshooting. It is designed to exercise all six StrataDB primitives under realistic workload patterns.

## Developer use cases

A developer building this agent needs StrataDB to handle five distinct workload patterns:

### 1. Knowledge base (JSON + Vector)

The canonical enterprise KB — Salesforce docs, known issues, resolution playbooks. In production this is 30-40K documents stored as JSON, auto-embedded into vectors. For the benchmark dataset we start with ~500 documents (enough to be meaningful for search quality, small enough to load fast).

### 2. Short-term memory (KV + State)

The agent's working context for the current ticket. What has it tried, what's the current hypothesis, what's the customer's environment. This churns fast — written and overwritten constantly during a single diagnostic session. CAS on state cells for things like "current diagnostic phase" that multiple async tool calls might race on.

### 3. Long-term memory (KV + Events)

Cross-ticket learning. "Last time we saw this error pattern, the root cause was X." Event log of every resolved ticket's key facts, searchable by type. KV for learned heuristics.

### 4. RAG retrieval (Vector search + JSON fetch)

The core search loop: customer describes a problem, embed the description, vector search the KB, fetch the top-N JSON documents, feed to the agent. This is the hot path.

### 5. Branched diagnostic simulation (Branches)

The agent forks the current state to explore a hypothesis without contaminating the main ticket context. Each branch gets its own KV (findings), events (steps taken), and state (confidence). Dead-end branches get deleted. Successful ones feed back into the resolution.

## Dataset structure

```
data/
├── knowledge_base.json          # ~500 Salesforce KB articles
├── long_term_memory.json        # ~100 resolved ticket summaries
├── tickets/
│   └── ticket_10847.json        # Active ticket with diagnostics
└── embeddings.json              # Precomputed 384-dim embeddings
```

## Scale knobs

| Parameter | Benchmark start | Production target |
|-----------|----------------|-------------------|
| KB articles | 500 | 30-40K |
| Embedding dimensions | 384 | 384 |
| Resolved ticket history | 100 | 10K+ |
| Active diagnostic branches | 3-5 | 3-5 |
| Short-term memory writes per ticket | 50 | 50-200 |
| Vector search per diagnostic cycle | 5-10 | 5-10 |

## Content approach

KB articles and ticket data should feel real — real error messages, real config parameter names, real component names (Apex triggers, Process Builder, Platform Events, REST API, OAuth 2.0). Vector embeddings will be random but correctly shaped (384-dim, normalized) until the intelligence inference layer ships with MiniLM.
