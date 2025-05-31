-- Table: trading.exchange_tickers

-- DROP TABLE IF EXISTS trading.exchange_tickers;

CREATE TABLE IF NOT EXISTS trading.exchange_tickers
(
    symbol text COLLATE pg_catalog."default" NOT NULL,
    category text COLLATE pg_catalog."default",
    base_coin text COLLATE pg_catalog."default",
    quote_coin text COLLATE pg_catalog."default",
    status text COLLATE pg_catalog."default",
    is_new boolean,
    being_traded boolean,
    last_analysed timestamp without time zone,
    added_on timestamp without time zone,
    extra_data jsonb,
    CONSTRAINT exchange_tickers_pkey PRIMARY KEY (symbol)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS trading.exchange_tickers
    OWNER to postgres;

REVOKE ALL ON TABLE trading.exchange_tickers FROM pb_bot_user;

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE trading.exchange_tickers TO pb_bot_user;

GRANT ALL ON TABLE trading.exchange_tickers TO postgres;