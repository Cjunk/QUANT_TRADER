CREATE ROLE trader_user LOGIN PASSWORD 'strong_password';
CREATE SCHEMA trading AUTHORIZATION trader_user;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA trading TO trader_user;

-- When you have tables:
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA trading TO trader_user;
GRANT USAGE, SELECT ON SEQUENCE trading.bots_id_seq TO trader_user;



SET search_path TO trading;
SHOW port;



