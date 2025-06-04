import json

class MacroHandler:
    """
    Handles macro metrics data insertion into the database.
    """
    def __init__(self, conn, logger, db_config):
        self.conn = conn
        self.logger = logger
        self.db_config = db_config

    def handle_macro_metrics(self, data):
        """
        Inserts macro metrics data into the database.
        """
        try:
            cursor = self.conn.cursor()
            insert_sql = f"""
            INSERT INTO {self.db_config.DB_TRADING_SCHEMA}.macro_metrics (
                timestamp, btc_dominance, eth_dominance, total_market_cap, total_volume,
                active_cryptos, markets, fear_greed_index, market_sentiment, btc_open_interest, us_inflation
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
            """
            cursor.execute(insert_sql, (
                data['timestamp'],
                data.get('btc_dominance'),
                data.get('eth_dominance'),
                data.get('total_market_cap'),
                data.get('total_volume'),
                data.get('active_cryptos'),
                data.get('markets'),
                data.get('fear_greed_index'),
                data.get('market_sentiment'),
                data.get('btc_open_interest'),
                data.get('us_inflation')
            ))
            self.conn.commit()
            cursor.close()
            self.logger.info(f"✅ Inserted macro metric @ {data['timestamp']}")
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            self.logger.error(f"❌ Error inserting macro metric: {e}")