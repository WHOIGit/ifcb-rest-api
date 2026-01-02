-- PostgreSQL schema for token authentication

-- Tokens table (stores token metadata)
CREATE TABLE IF NOT EXISTS tokens (
    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash TEXT UNIQUE NOT NULL, -- SHA256 hash of the actual token
    name TEXT UNIQUE NOT NULL, -- Human-readable identifier
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP, -- NULL = never expires
    revoked BOOLEAN DEFAULT FALSE,
    revoked_at TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb -- Optional key-value metadata
);

-- Scopes table (permissions)
CREATE TABLE IF NOT EXISTS token_scopes (
    token_id UUID NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    PRIMARY KEY (token_id, scope)
);

-- Audit log (optional - track token usage)
CREATE TABLE IF NOT EXISTS token_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    token_id UUID REFERENCES tokens(token_id) ON DELETE SET NULL,
    action TEXT NOT NULL, -- 'validate', 'create', 'revoke'
    service_name TEXT,
    action_name TEXT,
    result TEXT NOT NULL, -- 'success', 'expired', 'insufficient_scopes', etc.
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tokens_name ON tokens(name) WHERE revoked = FALSE;
CREATE INDEX IF NOT EXISTS idx_tokens_expires_at ON tokens(expires_at) WHERE revoked = FALSE;
CREATE INDEX IF NOT EXISTS idx_token_scopes_token_id ON token_scopes(token_id);
CREATE INDEX IF NOT EXISTS idx_token_audit_token_id ON token_audit(token_id);
CREATE INDEX IF NOT EXISTS idx_token_audit_created_at ON token_audit(created_at DESC);

-- View for active tokens with scopes
CREATE OR REPLACE VIEW active_tokens_with_scopes AS
SELECT
    t.token_id,
    t.token_hash,
    t.name,
    t.created_at,
    t.expires_at,
    t.metadata,
    array_agg(ts.scope ORDER BY ts.scope) as scopes
FROM tokens t
LEFT JOIN token_scopes ts ON t.token_id = ts.token_id
WHERE t.revoked = FALSE
  AND (t.expires_at IS NULL OR t.expires_at > NOW())
GROUP BY t.token_id;
