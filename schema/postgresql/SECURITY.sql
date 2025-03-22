CREATE ROLE pb_bot_user LOGIN PASSWORD '3899b4209646b95895fb5f07603face879a537a9a54d4414624122ae19b9f4a2';
ALTER ROLE pb_bot_user WITH PASSWORD '3899b4209646b95895fb5f07603face879a537a9a54d4414624122ae19b9f4a2';

CREATE SCHEMA trading AUTHORIZATION pb_bot_user;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA trading TO pb_bot_user;

-- When you have tables:
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA trading TO pb_bot_user;
-- Also for future tables:
ALTER DEFAULT PRIVILEGES IN SCHEMA trading
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pb_bot_user;
-- Needed for SERIAL/sequence columns (like id)
GRANT USAGE, SELECT ON SEQUENCE trading.bots_id_seq TO pb_bot_user;
-- Future sequences too
ALTER DEFAULT PRIVILEGES IN SCHEMA trading
GRANT USAGE, SELECT ON SEQUENCES TO pb_bot_user;


SET search_path TO trading;
SHOW port;
-- ALTER ROLE trader_user RENAME TO pb_bot_user;



