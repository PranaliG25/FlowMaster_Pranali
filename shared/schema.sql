
CREATE TABLE IF NOT EXISTS workflows (
    id UUID PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    current_step VARCHAR(100),
    input_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_events (
    id BIGSERIAL PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    event_type VARCHAR(150) NOT NULL,
    event_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow_id
ON workflow_events(workflow_id);

CREATE TABLE IF NOT EXISTS identity_verifications (
    workflow_id UUID PRIMARY KEY REFERENCES workflows(id),
    identity_number VARCHAR(100),
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
