-- Archive for kline data
CREATE TABLE IF NOT EXISTS kline_data_archive AS TABLE kline_data WITH NO DATA;

-- Archive for trade data
CREATE TABLE IF NOT EXISTS trade_data_archive AS TABLE trade_data WITH NO DATA;

-- Move old Kline data to archive
INSERT INTO kline_data_archive SELECT * FROM kline_data;

-- Move old trade data to archive
INSERT INTO trade_data_archive SELECT * FROM trade_data;

TRUNCATE TABLE kline_data;
TRUNCATE TABLE trade_data;
