CREATE TABLE trading.bot_auth (
    bot_name TEXT PRIMARY KEY,
    token TEXT NOT NULL
);
INSERT INTO trading.bot_auth (bot_name, token)
VALUES
  ('websocket_bot', 'test1'),
  ('db_bot', 'xyz789adminsecret');

