
require("dotenv").config();

const { kafka } = require("./kafka");

const topics = [
    "identity.verification.requested",
    "identity.verification.completed",
    "identity.verification.failed",
    "credit.check.requested",
    "credit.check.completed",
    "credit.check.failed",
    "loan.approved",
    "loan.rejected",
    "verification.compensation.requested",
    "verification.compensation.completed"
];

async function createTopics() {
    const admin = kafka.admin();

    try {
        await admin.connect();

        await admin.createTopics({
            topics: topics.map(topic => ({
                topic,
                numPartitions: 3,
                replicationFactor: 1
            }))
        });

        console.log("Kafka topics created successfully!");
    } catch (error) {
        console.error("Topic creation failed:", error.message);
        process.exitCode = 1;
    } finally {
        await admin.disconnect().catch(error => {
            console.error("Kafka admin disconnect failed:", error.message);
        });
    }
}

createTopics();
