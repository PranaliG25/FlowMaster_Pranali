
require("dotenv").config();

const express = require("express");
const { kafka, producer } = require("../shared/kafka");
const events = require("../shared/events");

const app = express();

app.use(express.json());

const PORT = 3002;

async function startCreditConsumer() {
    await producer.connect();

    const consumer = kafka.consumer({
        groupId: "credit-service-group"
    });

    await consumer.connect();

    await consumer.subscribe({
        topic: events.CREDIT_CHECK_REQUESTED,
        fromBeginning: false
    });

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const data = JSON.parse(message.value.toString());

                console.log("Credit check request received:", data);

                // Demo credit check
                const isApproved = Number(data.creditScore) >= 650;

                await producer.send({
                    topic: isApproved
                        ? events.CREDIT_CHECK_COMPLETED
                        : events.CREDIT_CHECK_FAILED,
                    messages: [
                        {
                            value: JSON.stringify({
                                workflowId: data.workflowId,
                                approved: isApproved,
                                message: isApproved
                                    ? "Credit check passed"
                                    : "Credit check failed"
                            })
                        }
                    ]
                });

                console.log("Credit check result published:", isApproved);

            } catch (error) {
                console.error("Credit processing failed:", error);
            }
        }
    });

    console.log("Credit Kafka consumer started!");
}

app.get("/health", (req, res) => {
    res.status(200).json({
        service: "FlowMaster Credit Service",
        status: "running"
    });
});

app.listen(PORT, () => {
    console.log(`Credit Service running on port ${PORT}`);
    startCreditConsumer().catch(console.error);
});
