import json
import hashlib
import config.config_db as db_config


class BOTStatusHandler:
    def __init__(self, bot):
        self.bot = bot

    def handle_bot_status_update(self, status_obj):
        cursor = self.bot.conn.cursor()
        bot_name = status_obj.get("bot_name")
        status = status_obj.get("status")
        time_str = status_obj.get("time")
        meta = status_obj.get("metadata", {})
        auth_token = status_obj.get("auth_token")
        self.bot.logger.info(f"📨 Received status update for bot: {bot_name}")
        self.bot.logger.debug(f"Status object: {json.dumps(status_obj, indent=2)}")

        if not all([bot_name, status, time_str, auth_token]):
            self.bot.logger.error(f"❌ Missing required fields in status_obj: {status_obj}")
            cursor.close()
            return

        hashed_auth_token = hashlib.sha256(auth_token.encode()).hexdigest()

        try:
            cursor.execute(f"""
                SELECT token FROM {db_config.DB_TRADING_SCHEMA}.bot_auth
                WHERE bot_name = %s
            """, (bot_name,))
            row = cursor.fetchone()
        except Exception as e:
            self.bot.logger.error(f"❌ Failed auth query for '{bot_name}': {e}")
            cursor.close()
            return

        if not row:
            self.bot.logger.warning(f"❗ No auth record found for bot '{bot_name}'")
            cursor.close()
            return

        db_token = row[0]
        if db_token != hashed_auth_token:
            self.bot.logger.warning(f"❌ Invalid auth token for bot '{bot_name}'")
            cursor.close()
            return

        try:
            cursor.execute(f"""
                SELECT 1 FROM {db_config.DB_TRADING_SCHEMA}.bots
                WHERE bot_name = %s
            """, (bot_name,))
            exists = cursor.fetchone()

            if exists:
                self.bot.logger.info(f"📝 Updating bot '{bot_name}' status to '{status}' at {time_str}")
                cursor.execute(f"""
                    UPDATE {db_config.DB_TRADING_SCHEMA}.bots
                    SET status = %s,
                        last_updated = %s,
                        metadata = %s
                    WHERE bot_name = %s
                """, (status, time_str, json.dumps(meta), bot_name))
                self.bot.conn.commit()
                self.bot.logger.info(f"✅ Update committed for bot '{bot_name}'")
            else:
                self.bot.logger.warning(f"⚠️ Bot '{bot_name}' not found in bots table. Skipping update.")
        except Exception as e:
            self.bot.conn.rollback()
            self.bot.logger.error(f"❌ Failed status update for bot '{bot_name}': {e}")
        finally:
            cursor.close()