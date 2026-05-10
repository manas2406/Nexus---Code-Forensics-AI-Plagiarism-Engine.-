# Nexus — Code Forensics & AI Plagiarism Engine

Nexus is an event-driven code-plagiarism detection platform that ingests student submissions, fingerprints source files using Tree-sitter AST hashing and MinHash (Jaccard) similarity, and escalates suspicious pairs to an LLM-powered forensic analyser that identifies obfuscation techniques and renders a verdict. The system is built as a polyglot monorepo with a Next.js frontend, an Express API gateway, Python worker microservices, and infrastructure powered by Kafka, MinIO, and Redis.

## Quick Start

```bash
# 1. Copy environment variables
cp .env.example .env

# 2. Start all infrastructure services
docker compose up -d

# 3. Install Node.js dependencies
pnpm install
```
