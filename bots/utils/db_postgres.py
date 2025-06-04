import psycopg2
import os

class PostgresHandler:
    def __init__(self, logger):
        self.host = os.getenv("DB_HOST")
        self.port = int(os.getenv("DB_PORT"))
        self.database = os.getenv("DB_DATABASE")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.schema = os.getenv("DB_TRADING_SCHEMA")
        self.logger = logger
        self.conn = self._connect()
    def _connect(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.schema}")
        self.logger.info(f"Connected to PostgreSQL @ {self.host}:{self.port}/{self.database}")   
        return conn       
    def close(self):
        if self.conn:
            self.conn.close()
    def debug_db_connection(self):
        try:
            env_schema = os.getenv("DB_TRADING_SCHEMA")
            print(f"[DEBUG] os.getenv('DB_TRADING_SCHEMA') = {env_schema!r}")
            print(f"[DEBUG] self.schema = {self.schema!r}")
            print(f"[DEBUG] Connection params: host={self.host}, port={self.port}, dbname={self.database}, user={self.user}")

            with self.conn.cursor() as cur:
                cur.execute("SELECT current_user, current_database(), current_schema();")
                user, db, schema = cur.fetchone()
                print(f"[DEBUG] Connected as user: {user}")
                print(f"[DEBUG] Connected to database: {db}")
                print(f"[DEBUG] Current schema: {schema}")

                cur.execute("SHOW search_path;")
                search_path = cur.fetchone()[0]
                print(f"[DEBUG] search_path: {search_path}")

                cur.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;")
                schemas = [row[0] for row in cur.fetchall()]
                print(f"[DEBUG] Schemas in database: {schemas}")

                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    ORDER BY table_schema, table_name;
                """)
                tables = cur.fetchall()
                print(f"[DEBUG] All tables in database:")
                for s, t in tables:
                    print(f"  - {s}.{t}")

                cur.execute("""
                    SELECT table_name, privilege_type
                    FROM information_schema.table_privileges
                    WHERE grantee = CURRENT_USER AND table_schema = %s
                    ORDER BY table_name, privilege_type;
                """, (self.schema,))
                privs = cur.fetchall()
                print(f"[DEBUG] Privileges for user '{self.user}' in schema '{self.schema}':")
                for t, p in privs:
                    print(f"  - {t}: {p}")

        except Exception as e:
            print(f"[DEBUG] Error during DB debug: {e}")