require("dotenv").config();

const fs = require("fs");
const path = require("path");
const pool = require("./database");

async function initializeDatabase() {
    try {
        const schemaPath = path.join(__dirname, "schema.sql");
        const schema = fs.readFileSync(schemaPath, "utf8");

        await pool.query(schema);

        console.log("Database tables created successfully!");
    } catch (error) {
        console.error("Database initialization failed:", error.message);
    } finally {
        await pool.end();
    }
}

initializeDatabase();