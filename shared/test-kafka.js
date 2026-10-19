require("dotenv").config();

const { kafka } = require("./kafka");

async function testKafka() {
    const admin = kafka.admin();

    try {
        await admin.connect();
        console.log("Kafka connected successfully!");

        const topics = await admin.listTopics();
        console.log("Available topics:", topics);
    } catch (error) {
        console.error("Kafka connection failed:", error.message);
    } finally {
        await admin.disconnect();
    }
}

testKafka();