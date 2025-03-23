-- Create the dashboard user with a strong password
CREATE ROLE dashboard_user LOGIN PASSWORD 'your_strong_password';

-- Grant only read access on the relevant tables
GRANT SELECT ON trading.bots TO dashboard_user;
GRANT SELECT ON trading.bot_auth TO dashboard_user;
-- Add more as needed for other dashboard features
GRANT USAGE ON SCHEMA trading TO dashboard_user;
GRANT SELECT ON ALL TABLES IN SCHEMA trading TO dashboard_user;

-- Optional: If using sequences for IDs (rare for dashboard), grant usage
GRANT USAGE ON SEQUENCE trading.bots_id_seq TO dashboard_user;
SHOW config_file;
