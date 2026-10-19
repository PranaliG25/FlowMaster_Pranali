const { Pool } = require("pg");

const pool = new Pool({
    host: process.env.POSTGRES_HOST || "localhost",
    port: process.env.POSTGRES_PORT || 5432,
    user: process.env.POSTGRES_USER || "flowmaster",
    password: process.env.POSTGRES_PASSWORD || "flowmaster123",
    database: process.env.POSTGRES_DB || "flowmaster_db"
});

module.exports = pool;