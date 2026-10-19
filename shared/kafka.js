
const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
    clientId: "flowmaster",
    brokers: [
        process.env.KAFKA_BROKER || "localhost:9092"
    ],
    logLevel: logLevel.WARN,
    retry: {
        initialRetryTime: 300,
        retries: 10,
        maxRetryTime: 30000,
        factor: 2,
        multiplier: 2
    }
});

const producer = kafka.producer({
    allowAutoTopicCreation: false
});

module.exports = {
    kafka,
    producer
};
