
require("dotenv").config();

const express = require("express");
const { randomUUID } = require("crypto");
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");
const promClient = require("prom-client");

const { kafka, producer } = require("../shared/kafka");
const events = require("../shared/events");
const pool = require("../shared/database");

const app = express();
app.use(express.json({ limit: "10kb" }));

const PORT = 3000;

// Prometheus metrics
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const workflowSubmissions = new promClient.Counter({
    name: "flowmaster_workflows_submitted_total",
    help: "Total number of workflows submitted"
});

const workflowCompletions = new promClient.Counter({
    name: "flowmaster_workflows_completed_total",
    help: "Total number of workflows completed",
    labelNames: ["status"]
});

const activeWorkflows = new promClient.Gauge({
    name: "flowmaster_active_workflows",
    help: "Number of workflows currently in progress"
});

register.registerMetric(workflowSubmissions);
register.registerMetric(workflowCompletions);
register.registerMetric(activeWorkflows);

// Load YAML workflow configuration
const configPath = path.join(__dirname, "../config/loan-workflow.yaml");
const workflowConfig = yaml.load(fs.readFileSync(configPath, "utf8"));

function validateWorkflowConfig() {
    const requiredSteps = [
        "verify_identity",
        "check_credit",
        "approve_loan"
    ];

    const configuredSteps = workflowConfig.steps.map(step => step.name);

    if (JSON.stringify(configuredSteps) !== JSON.stringify(requiredSteps)) {
        throw new Error("Invalid loan workflow configuration");
    }

    console.log("Workflow configuration validated successfully!");
}

async function publishEvent(topic, data) {
    await producer.send({
        topic,
        messages: [{
            key: data.workflowId,
            value: JSON.stringify(data)
        }]
    });
}

async function saveEvent(workflowId, eventType, data) {
    await pool.query(
        `INSERT INTO workflow_events
         (workflow_id, event_type, event_data)
         VALUES ($1, $2, $3)`,
        [workflowId, eventType, JSON.stringify(data)]
    );
}

async function updateWorkflow(workflowId, status, step) {
    const result = await pool.query(
        `UPDATE workflows
         SET status = $1,
             current_step = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $3
         RETURNING id`,
        [status, step, workflowId]
    );

    if (result.rowCount > 0 &&
        ["APPROVED", "REJECTED"].includes(status)) {
        workflowCompletions.inc({ status });
    }
}

async function refreshActiveWorkflowMetric() {
    const result = await pool.query(
        `SELECT COUNT(*)::int AS count
         FROM workflows
         WHERE status NOT IN ('APPROVED', 'REJECTED')`
    );

    activeWorkflows.set(result.rows[0].count);
}

// Recover unfinished workflows after restart
async function recoverWorkflows() {
    const result = await pool.query(
        `SELECT id, status, input_data
         FROM workflows
         WHERE status NOT IN ('APPROVED', 'REJECTED')`
    );

    console.log(`Recovery check: found ${result.rows.length} unfinished workflows`);

    for (const workflow of result.rows) {
        const input = workflow.input_data || {};

        if (workflow.status === "IDENTITY_VERIFICATION_PENDING") {
            await publishEvent(events.IDENTITY_VERIFICATION_REQUESTED, {
                workflowId: workflow.id,
                identityNumber: input.identityNumber,
                creditScore: input.creditScore
            });
        } else if (workflow.status === "CREDIT_CHECK_PENDING") {
            await publishEvent(events.CREDIT_CHECK_REQUESTED, {
                workflowId: workflow.id,
                creditScore: input.creditScore
            });
        } else if (workflow.status === "COMPENSATION_PENDING") {
            await publishEvent(events.COMPENSATION_REQUESTED, {
                workflowId: workflow.id,
                reason: "Credit check failed"
            });
        }
    }

    await refreshActiveWorkflowMetric();
}

async function startOrchestrator() {
    validateWorkflowConfig();

    await producer.connect();

    const consumer = kafka.consumer({
        groupId: "orchestrator-group"
    });

    await consumer.connect();

    const topics = [
        events.IDENTITY_VERIFICATION_COMPLETED,
        events.IDENTITY_VERIFICATION_FAILED,
        events.CREDIT_CHECK_COMPLETED,
        events.CREDIT_CHECK_FAILED,
        events.COMPENSATION_COMPLETED
    ];

    for (const topic of topics) {
        await consumer.subscribe({
            topic,
            fromBeginning: false
        });
    }

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                const workflowId = data.workflowId;

                const result = await pool.query(
                    "SELECT * FROM workflows WHERE id = $1",
                    [workflowId]
                );

                if (result.rows.length === 0) {
                    console.log("Unknown workflow:", workflowId);
                    return;
                }

                const workflow = result.rows[0];

                if (topic === events.IDENTITY_VERIFICATION_COMPLETED) {
                    if (workflow.status !== "IDENTITY_VERIFICATION_PENDING") return;

                    await saveEvent(workflowId, topic, data);

                    await updateWorkflow(
                        workflowId,
                        "CREDIT_CHECK_PENDING",
                        "check_credit"
                    );

                    await publishEvent(events.CREDIT_CHECK_REQUESTED, {
                        workflowId,
                        creditScore: workflow.input_data.creditScore
                    });

                } else if (topic === events.IDENTITY_VERIFICATION_FAILED) {
                    if (workflow.status !== "IDENTITY_VERIFICATION_PENDING") return;

                    await saveEvent(workflowId, topic, data);
                    await updateWorkflow(workflowId, "REJECTED", "verify_identity");

                } else if (topic === events.CREDIT_CHECK_COMPLETED) {
                    if (workflow.status !== "CREDIT_CHECK_PENDING") return;

                    await saveEvent(workflowId, topic, data);
                    await updateWorkflow(workflowId, "APPROVED", "approve_loan");

                    await publishEvent(events.LOAN_APPROVED, {
                        workflowId,
                        message: "Loan approved successfully"
                    });

                } else if (topic === events.CREDIT_CHECK_FAILED) {
                    if (workflow.status !== "CREDIT_CHECK_PENDING") return;

                    await saveEvent(workflowId, topic, data);

                    await updateWorkflow(
                        workflowId,
                        "COMPENSATION_PENDING",
                        "compensate_verification"
                    );

                    await publishEvent(events.COMPENSATION_REQUESTED, {
                        workflowId,
                        reason: "Credit check failed"
                    });

                } else if (topic === events.COMPENSATION_COMPLETED) {
                    if (workflow.status !== "COMPENSATION_PENDING") return;

                    await saveEvent(workflowId, topic, data);

                    if (data.compensated === true) {
                        await updateWorkflow(
                            workflowId,
                            "REJECTED",
                            "compensate_verification"
                        );

                        await publishEvent(events.LOAN_REJECTED, {
                            workflowId,
                            message: "Loan rejected after compensation"
                        });
                    } else {
                        await updateWorkflow(
                            workflowId,
                            "COMPENSATION_FAILED",
                            "compensate_verification"
                        );
                    }
                }

                await refreshActiveWorkflowMetric();

            } catch (error) {
                console.error("Orchestrator event error:", error);
                throw error;
            }
        }
    });

    console.log("Orchestrator Kafka consumer started!");

    await recoverWorkflows();
}

// Submit a workflow
app.post("/workflows", async (req, res) => {
    try {
        const { identityNumber, creditScore } = req.body;

        const score = Number(creditScore);

        if (
            typeof identityNumber !== "string" ||
            !identityNumber.trim() ||
            creditScore === undefined ||
            creditScore === null ||
            creditScore === "" ||
            !Number.isInteger(score) ||
            score < 300 ||
            score > 900
        ) {
            return res.status(400).json({
                error: "identityNumber and a valid creditScore between 300 and 900 are required"
            });
        }

        const workflowId = randomUUID();

        const input = {
            workflowId,
            identityNumber: identityNumber.trim(),
            creditScore: score
        };

        await pool.query(
            `INSERT INTO workflows
             (id, status, current_step, input_data)
             VALUES ($1, $2, $3, $4)`,
            [
                workflowId,
                "IDENTITY_VERIFICATION_PENDING",
                "verify_identity",
                JSON.stringify({
                    identityNumber: input.identityNumber,
                    creditScore: input.creditScore
                })
            ]
        );

        await saveEvent(
            workflowId,
            events.IDENTITY_VERIFICATION_REQUESTED,
            input
        );

        await publishEvent(
            events.IDENTITY_VERIFICATION_REQUESTED,
            input
        );

        workflowSubmissions.inc();
        await refreshActiveWorkflowMetric();

        res.status(202).json({
            message: "Workflow submitted successfully",
            workflowId,
            status: "IDENTITY_VERIFICATION_PENDING"
        });

    } catch (error) {
        console.error("Workflow submission failed:", error);

        res.status(500).json({
            error: "Workflow submission failed"
        });
    }
});

// NEW FEATURE: Workflow statistics API
app.get("/workflows/stats", async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT
                COUNT(*)::int AS total_workflows,
                COUNT(*) FILTER (
                    WHERE status = 'APPROVED'
                )::int AS approved,
                COUNT(*) FILTER (
                    WHERE status = 'REJECTED'
                )::int AS rejected,
                COUNT(*) FILTER (
                    WHERE status NOT IN (
                        'APPROVED',
                        'REJECTED',
                        'COMPENSATION_FAILED'
                    )
                )::int AS pending,
                COUNT(*) FILTER (
                    WHERE status = 'COMPENSATION_FAILED'
                )::int AS compensation_failed
            FROM workflows
        `);

        res.status(200).json({
            message: "Workflow statistics fetched successfully",
            statistics: result.rows[0]
        });

    } catch (error) {
        console.error("Statistics API error:", error);

        res.status(500).json({
            error: "Unable to fetch workflow statistics"
        });
    }
});

// Get individual workflow status
app.get("/workflows/:id", async (req, res) => {
    try {
        const result = await pool.query(
            "SELECT * FROM workflows WHERE id = $1",
            [req.params.id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({
                error: "Workflow not found"
            });
        }

        res.json(result.rows[0]);

    } catch (error) {
        res.status(500).json({
            error: error.message
        });
    }
});

// Prometheus metrics
app.get("/metrics", async (req, res) => {
    try {
        await refreshActiveWorkflowMetric();

        res.set("Content-Type", register.contentType);
        res.end(await register.metrics());

    } catch (error) {
        console.error("Metrics error:", error);
        res.status(500).send("Unable to collect metrics");
    }
});

// Health check
app.get("/health", async (req, res) => {
    try {
        await pool.query("SELECT 1");

        res.status(200).json({
            service: "FlowMaster Orchestrator",
            status: "running",
            database: "connected"
        });

    } catch (error) {
        res.status(503).json({
            service: "FlowMaster Orchestrator",
            status: "unhealthy",
            database: "disconnected"
        });
    }
});

app.listen(PORT, () => {
    console.log(`Orchestrator running on port ${PORT}`);

    startOrchestrator().catch(error => {
        console.error("Orchestrator startup failed:", error);
    });
});
