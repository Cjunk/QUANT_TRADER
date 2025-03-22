CREATE TABLE IF NOT EXISTS trading.bots (
    id SERIAL PRIMARY KEY,
    bot_name TEXT UNIQUE NOT NULL,
    role_id INTEGER NOT NULL REFERENCES trading.bot_roles(id),
    status TEXT DEFAULT 'inactive',
    started_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT now(),
    is_supervisor BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS trading.bot_roles (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,       -- e.g., "websocket", "db", "trade", "supervisor"
    description TEXT
);
