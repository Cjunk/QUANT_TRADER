DO $$
DECLARE
    archive_table TEXT := 'kline_data_' || TO_CHAR(NOW(), 'YYYYMMDD_HH24MI');
BEGIN
    EXECUTE FORMAT('CREATE TABLE %I AS TABLE kline_data', archive_table);
END $$;

GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON ALL TABLES IN SCHEMA trading TO trader_user;
ALTER TABLE trading.orderbook_data OWNER TO trader_user;
ALTER TABLE trading.kline_data OWNER TO trader_user;
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'trading'
    LOOP
        EXECUTE FORMAT(
            'ALTER TABLE trading.%I OWNER TO trader_user',
            r.tablename
        );
    END LOOP;
END $$;
