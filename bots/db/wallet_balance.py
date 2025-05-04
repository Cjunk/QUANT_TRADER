def store_wallet_balances(self, account_id, balances):
    """
    Store Bybit wallet balances for a given account.
    Expected format for each balance:
    {
        "coin": "USDT",
        "wallet_type": "UNIFIED",
        "total": 1000.00,
        "available": 850.00,
        "timestamp": "2025-05-03T12:30:00Z"
    }
    """
    insert_sql = f"""
        INSERT INTO trading.wallet_balances
        (account_id, coin, wallet_type, total, available, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor = self.conn.cursor()
    try:
        for balance in balances:
            cursor.execute(insert_sql, (
                account_id,
                balance["coin"],
                balance["wallet_type"],
                balance["total"],
                balance["available"],
                balance["timestamp"]
            ))
        self.conn.commit()
        self.logger.info(f"✅ Inserted {len(balances)} wallet rows for account_id {account_id}")
    except Exception as e:
        self.conn.rollback()
        self.logger.error(f"❌ Error inserting wallet balances: {e}")
    finally:
        cursor.close()
