# FlowMaster – Distributed Loan Approval Workflow

FlowMaster is a distributed, event-driven loan approval system built using Node.js microservices, Apache Kafka, PostgreSQL, Redis, and the Saga design pattern.

The system coordinates identity verification, credit checks, and loan approval through asynchronous Kafka events. It also supports compensating transactions, workflow persistence, and recovery of unfinished workflows after an Orchestrator restart.

## Features

- Microservices architecture using Node.js and Express.
- Event-driven communication using Apache Kafka.
- YAML-based loan workflow configuration.
- Saga orchestration pattern.
- Identity verification and credit check services.
- Automatic compensation when a credit check fails.
- PostgreSQL persistence for workflows and event history.
- Recovery of unfinished workflows after Orchestrator restart.
- Prometheus metrics endpoint.
- Docker Compose infrastructure configuration.
- Redis and BullMQ dependencies for future background processing.

## Technology Stack

| Technology | Purpose |
|---|---|
| Node.js | Backend runtime |
| Express.js | REST APIs |
| Apache Kafka | Event-driven communication |
| PostgreSQL | Workflow and event persistence |
| Redis | In-memory data store |
| Docker | Containerization |
| Docker Compose | Infrastructure management |
| Prometheus | Application metrics |
| Grafana | Monitoring visualization |
| js-yaml | Workflow configuration |
| BullMQ | Background job processing dependency |

## Architecture

The application consists of three microservices.

### 1. Orchestrator Service

- Accepts loan workflow requests.
- Coordinates the Saga workflow.
- Publishes and consumes Kafka events.
- Maintains workflow status.
- Initiates compensation when credit checks fail.
- Recovers unfinished workflows after restart.
- Exposes REST APIs and Prometheus metrics.

**Port:** `3000`

### 2. Verification Service

- Consumes identity verification requests.
- Performs demo identity validation.
- Saves verification results in PostgreSQL.
- Publishes verification success or failure events.
- Processes compensation requests.

**Port:** `3001`

### 3. Credit Service

- Consumes credit check requests.
- Evaluates the supplied credit score.
- Approves scores of 650 or higher.
- Publishes credit success or failure events.

**Port:** `3002`

## Saga Workflow

The loan approval workflow consists of three main steps:

1. Verify Identity
2. Check Credit
3. Approve Loan

### Successful Workflow

```text
Client
  |
  v
Orchestrator
  |
  v
Identity Verification
  |
  | Identity Verified
  v
Credit Check
  |
  | Credit Approved
  v
Loan Approved
```

### Failure and Compensation Workflow

```text
Client
  |
  v
Orchestrator
  |
  v
Identity Verification
  |
  | Verification Successful
  v
Credit Check
  |
  | Credit Failed
  v
Compensation Requested
  |
  v
Verification Reversed
  |
  v
Loan Rejected
```

The compensation operation changes the identity verification status from `VERIFIED` to `COMPENSATED`.

## Kafka Events

The services communicate asynchronously using Kafka topics.

| Event | Description |
|---|---|
| `identity.verification.requested` | Starts identity verification |
| `identity.verification.completed` | Identity verification succeeded |
| `identity.verification.failed` | Identity verification failed |
| `credit.check.requested` | Starts credit check |
| `credit.check.completed` | Credit check succeeded |
| `credit.check.failed` | Credit check failed |
| `loan.approved` | Loan approval notification |
| `loan.rejected` | Loan rejection notification |
| `verification.compensation.requested` | Requests compensation |
| `verification.compensation.completed` | Compensation result |

## Project Structure

```text
FlowMaster_Pranali/
|
|-- orchestrator/
|   |-- server.js
|
|-- verification-service/
|   |-- server.js
|
|-- credit-service/
|   |-- server.js
|
|-- shared/
|   |-- kafka.js
|   |-- database.js
|   |-- redis.js
|   |-- events.js
|   |-- schema.sql
|   |-- init-db.js
|   |-- create-topics.js
|
|-- config/
|   |-- loan-workflow.yaml
|
|-- monitoring/
|   |-- prometheus/
|       |-- prometheus.yml
|
|-- docker-compose.yml
|-- package.json
|-- package-lock.json
|-- README.md
```

## Prerequisites

Install the following:

- Node.js
- npm
- Docker Desktop
- Git

## Installation and Setup

### 1. Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd FlowMaster_Pranali
```

### 2. Install dependencies

```bash
npm install
```

### 3. Configure environment variables

Create a `.env` file in the project root.

```env
PORT=3000

KAFKA_BROKER=localhost:9092

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=flowmaster
POSTGRES_PASSWORD=flowmaster123
POSTGRES_DB=flowmaster_db

REDIS_HOST=localhost
REDIS_PORT=6379
```

These are development credentials. Use secure credentials for production.

### 4. Start infrastructure

```bash
docker compose up -d
```

This starts Kafka, Zookeeper, PostgreSQL, Redis, Prometheus, and Grafana as configured in Docker Compose.

### 5. Initialize PostgreSQL

```bash
node shared/init-db.js
```

### 6. Create Kafka topics

```bash
node shared/create-topics.js
```

### 7. Start the microservices

Open three separate terminals.

**Terminal 1 – Orchestrator**

```bash
node orchestrator/server.js
```

**Terminal 2 – Verification Service**

```bash
node verification-service/server.js
```

**Terminal 3 – Credit Service**

```bash
node credit-service/server.js
```

## API Endpoints

### Submit a Loan Workflow

**Method:** `POST`

**Endpoint:** `/workflows`

Request:

```json
{
  "identityNumber": "ID12345",
  "creditScore": 750
}
```

Successful response:

```json
{
  "message": "Workflow submitted successfully",
  "workflowId": "generated-workflow-id",
  "status": "IDENTITY_VERIFICATION_PENDING"
}
```

### Get Workflow Status

**Method:** `GET`

**Endpoint:** `/workflows/:id`

Example:

```bash
curl http://localhost:3000/workflows/YOUR_WORKFLOW_ID
```

### Orchestrator Health Check

**Method:** `GET`

**Endpoint:** `/health`

Example:

```bash
curl http://localhost:3000/health
```

### Prometheus Metrics

**Method:** `GET`

**Endpoint:** `/metrics`

Example:

```bash
curl http://localhost:3000/metrics
```

Custom metrics include:

- `flowmaster_workflows_submitted_total`
- `flowmaster_workflows_completed_total`
- `flowmaster_active_workflows`

## Testing

### Successful Loan Approval

Submit a workflow with a credit score greater than or equal to 650.

```bash
curl -X POST http://localhost:3000/workflows \
-H "Content-Type: application/json" \
-d '{"identityNumber":"TEST123","creditScore":750}'
```

**Expected final status:**

`APPROVED`

### Credit Check Failure and Compensation

Submit a workflow with a credit score below 650.

```bash
curl -X POST http://localhost:3000/workflows \
-H "Content-Type: application/json" \
-d '{"identityNumber":"TEST456","creditScore":550}'
```

**Expected final status:**

`REJECTED`

**Expected verification status in PostgreSQL:**

`COMPENSATED`

### Input Validation

Submit a request with a missing identity number or invalid credit score.

**Expected HTTP status:**

`400 Bad Request`

### Recovery Testing

1. Ensure Kafka and PostgreSQL are running.
2. Stop the Verification Service.
3. Submit a new workflow.
4. Verify that the workflow remains pending.
5. Restart the Orchestrator.
6. Confirm that the Orchestrator detects the unfinished workflow and republishes the pending event based on its saved status.
7. Restart the Verification Service.
8. Check that the workflow eventually completes.

## Database Tables

### workflows

Stores workflow IDs, status, current step, input data, and timestamps.

### workflow_events

Stores workflow event history for tracking and recovery support.

### identity_verifications

Stores identity verification records and compensation status.

## Monitoring

The Orchestrator exposes Prometheus-compatible metrics through:

`http://localhost:3000/metrics`

Prometheus and Grafana are configured in Docker Compose.

- Prometheus uses the configuration in `monitoring/prometheus/prometheus.yml`.
- Grafana is included as a monitoring service.
- Monitoring service availability and Grafana dashboard configuration should be verified before use.

## Important Notes

- Identity verification and credit evaluation are demo implementations, not production financial or identity checks.
- The credit approval threshold is 650.
- Redis and BullMQ dependencies are included, but background job processing is not yet integrated.
- Workflow recovery currently republishes events based on persisted workflow status; it is not a complete event-log replay implementation.
- A transactional outbox and a dedicated Dead Letter Queue are not implemented.
- Production deployment would require stronger security, authentication, validation, retry policies, and transaction guarantees.

## Author

**Prenali**

Backend Developer Take-Home Assignment