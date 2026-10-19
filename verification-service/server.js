
require("dotenv").config();

const express = require("express");
const { kafka, producer } = require("../shared/kafka");
const events = require("../shared/events");
const pool = require("../shared/database");

const app = express();
app.use(express.json());

const PORT = 3001;

async function startVerificationConsumer() {
    await producer.connect();

    const consumer = kafka.consumer({
        groupId: "verification-service-group"
    });

    await consumer.connect();

    await consumer.subscribe({
        topics: [
            events.IDENTITY_VERIFICATION_REQUESTED,
            events.COMPENSATION_REQUESTED
        ],
        fromBeginning: false
    });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                const data = JSON.parse(message.value.toString());

                if (topic === events.IDENTITY_VERIFICATION_REQUESTED) {
                    console.log("Identity verification request:", data);

                    const isVerified = Boolean(data.identityNumber);

                    if (isVerified) {
                        await pool.query(
                            `INSERT INTO identity_verifications
                             (workflow_id, identity_number, status)
                             VALUES ($1, $2, $3)
                             ON CONFLICT (workflow_id)
                             DO UPDATE SET
                                 identity_number = EXCLUDED.identity_number,
                                 status = EXCLUDED.status,
                                 updated_at = CURRENT_TIMESTAMP`,
                            [data.workflowId, data.identityNumber, "VERIFIED"]
                        );
                    }

                    await producer.send({
                        topic: isVerified
                            ? events.IDENTITY_VERIFICATION_COMPLETED
                            : events.IDENTITY_VERIFICATION_FAILED,
                        messages: [{
                            key: data.workflowId,
                            value: JSON.stringify({
                                workflowId: data.workflowId,
                                verified: isVerified,
                                creditScore: data.creditScore,
                                message: isVerified
                                    ? "Identity verified successfully"
                                    : "Identity verification failed"
                            })
                        }]
                    });

                    console.log("Verification result published:", isVerified);
                }

                if (topic === events.COMPENSATION_REQUESTED) {
                    console.log("Compensation request received:", data);

                    const result = await pool.query(
                        `UPDATE identity_verifications
                         SET status = 'COMPENSATED',
                             updated_at = CURRENT_TIMESTAMP
                         WHERE workflow_id = $1
                         AND status = 'VERIFIED'
                         RETURNING workflow_id`,
                        [data.workflowId]
                    );

                    let compensated = result.rowCount > 0;

                    // Treat an already compensated record as success.
                    if (!compensated) {
                        const existing = await pool.query(
                            `SELECT status
                             FROM identity_verifications
                             WHERE workflow_id = $1`,
                            [data.workflowId]
                        );

                        compensated =
                            existing.rows.length > 0 &&
                            existing.rows[0].status === "COMPENSATED";
                    }

                    await producer.send({
                        topic: events.COMPENSATION_COMPLETED,
                        messages: [{
                            key: data.workflowId,
                            value: JSON.stringify({
                                workflowId: data.workflowId,
                                compensated,
                                message: compensated
                                    ? "Identity verification is compensated"
                                    : "No verified record was available to reverse"
                            })
                        }]
                    });

                    console.log(
                        "Verification compensation result:",
                        compensated
                    );
                }

            } catch (error) {
                console.error("Verification processing failed:", error);
            }
        }
    });

    console.log("Verification Kafka consumer started!");
}

app.get("/health", (req, res) => {
    res.json({
        service: "FlowMaster Verification Service",
        status: "running"
    });
});

app.listen(PORT, () => {
    console.log(`Verification Service running on port ${PORT}`);
    startVerificationConsumer().catch(console.error);
});
