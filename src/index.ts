#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
    CallToolRequestSchema,
    ListResourcesRequestSchema,
    ListToolsRequestSchema,
    ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import * as dotenv from 'dotenv';
import * as sql from 'mssql';

dotenv.config();
const SERVERNAME = process.env.SERVERNAME;
const DATABASENAME = process.env.DATABASENAME;
const USERNAME = process.env.USERNAME;
const PASSWORD = process.env.PASSWORD;

const sqlConfig: sql.config = {
    server: SERVERNAME as string,
    database: DATABASENAME as string,
    user: USERNAME as string,
    password: PASSWORD as string,
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
};

const server = new Server(
    {
        name: "mssql-reader",
        version: "1.0.0",
    },
    {
        capabilities: {
            resources: {},
            tools: {},
        },
    }
);


// Store SQL connection pool
let pool: sql.ConnectionPool | null = null;

// Resource base URL for schemas
const SCHEMA_PATH = "schema";
let resourceBaseUrl: URL;

server.setRequestHandler(ListToolsRequestSchema, async () => {
    return {
        tools: [
            {
                name: "executeQuery",
                description: "Execute a read-only SQL query",
                inputSchema: {
                    type: "object",
                    properties: {
                        sql: { type: "string" }
                    },
                    required: ["sql"]
                }
            }
        ]
    };
});

server.setRequestHandler(ListResourcesRequestSchema, async () => {
    if (!pool) {
        return { resources: [] };
    }

    const request = pool.request();
    try {
        const result = await request.query(`
            SELECT 
                s.name AS schema_name,
                t.name AS table_name
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            ORDER BY s.name, t.name
        `);

        return {
            resources: result.recordset.map((row) => ({
                name: `"${row.schema_name}.${row.table_name}" database schema`,
            })),
        };
    } catch (error) {
        console.error('Error listing resources:', error);
        return { resources: [] };
    }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
    if (!pool) {
        throw new Error('SQL connection not initialized');
    }

    const resourceUrl = new URL(request.params.uri);
    const pathComponents = resourceUrl.pathname.split("/");
    const schema = pathComponents.pop(); // schema path
    const tableName = pathComponents.pop(); // table name
    const schemaName = pathComponents.pop(); // schema name

    if (schema !== SCHEMA_PATH) {
        throw new Error("Invalid resource URI");
    }

    const sqlRequest = pool.request();
    try {
        const result = await sqlRequest.query(`
            SELECT 
                c.name AS column_name,
                t.name AS data_type,
                c.is_nullable
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
            INNER JOIN sys.schemas s ON tbl.schema_id = s.schema_id
            WHERE s.name = '${schemaName}'
            AND tbl.name = '${tableName}'
            ORDER BY c.column_id
        `);

        return {
            contents: [
                {
                    text: JSON.stringify(result.recordset, null, 2),
                },
            ],
        };
    } catch (error) {
        console.error('Error reading resource:', error);
        throw error;
    }
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
    try {
        if (request.params.name === "executeQuery") {

            if (pool) {
                await pool.close();
            }

            // Initialize SQL connection pool
            pool = await new sql.ConnectionPool(sqlConfig).connect();

            if (!pool) throw new Error('SQL connection not initialized');
            const { sql: sqlQuery } = request.params.arguments as { sql: string };

            const transaction = new sql.Transaction(pool);
            try {
                await transaction.begin();
                const result = await transaction.request().query(sqlQuery);
                await transaction.rollback();

                return {
                    content: [{ type: "text", text: JSON.stringify(result.recordset, null, 2) }],
                    isError: false
                };
            } catch (error) {
                throw error;
            }
            finally {
                if (transaction) {
                    await transaction.rollback().catch(console.error);
                }
                if (pool) {
                    await pool.close().catch(console.error);
                    pool = null; // Reset pool after use
                }
            }
        }

        throw new Error(`Tool ${request.params.name} not implemented`);
    } catch (error) {
        return {
            content: [{
                type: "text",
                text: `Error: ${error instanceof Error ? error.message : String(error)}`
            }],
            isError: true
        };
    }
});

async function runServer() {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error("SQL Server MCP Server running on stdio");
}

runServer().catch(console.error);