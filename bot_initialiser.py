# tools/bot_creator.py

import os
import shutil
import hashlib
import secrets
import psycopg2
import json
from config.config_db import DB_HOST, DB_PORT, DB_DATABASE, DB_USER, DB_PASSWORD

# === CONFIG ===
TEMPLATE_DIR = "templates\\bots"
WS_DIR = "ws"
CONFIG_DIR = "config"
ROLE_ID = 4
SCHEMA = "trading"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_DATABASE,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_bot():
    bot_name = input("Enter bot name (e.g., 'Wallet Service'): ").strip()
    class_name = ''.join(word.capitalize() for word in bot_name.split())     # For class
    file_base = '_'.join(bot_name.lower().split())                           # For filename
    config_name = ''.join(bot_name.lower().split())                          # For config

    token = secrets.token_hex(32)
    hashed_token = hashlib.sha256(token.encode()).hexdigest()

    # File names
    main_filename = f"{file_base}_main.py"
    config_filename = f"config_auto_{config_name}.py"
    bot_filename = f"{file_base}_bot.py"

    # Paths
    target_bot_path = os.path.join(WS_DIR, bot_filename)
    target_main_path = os.path.join(main_filename)
    target_config_path = os.path.join(CONFIG_DIR, config_filename)

    # Copy templates
    shutil.copyfile(os.path.join(TEMPLATE_DIR, "Authenticated_bot_template.py"), target_bot_path)
    shutil.copyfile(os.path.join(TEMPLATE_DIR, "main_template.py"), target_main_path)
    shutil.copyfile(os.path.join(TEMPLATE_DIR, "config_bot_template.py"), target_config_path)

    # Modify bot class
    with open(target_bot_path, 'r+', encoding='utf-8') as f:
        content = f.read()
        content = content.replace("config.{{CONFIG_MODULE}}", f"config.{config_filename[:-3]}")
        content = content.replace("class AuthenticatedBot", f"class {class_name}")
        f.seek(0)
        f.write(content)
        f.truncate()

    # Modify config
    with open(target_config_path, 'r+', encoding='utf-8') as f:
        content = f.read()
        content = content.replace("{{BOT_NAME}}", bot_name)
        content = content.replace("{{BOT_AUTH_TOKEN}}", token)
        content = content.replace("{{LOG_FILENAME}}", f"{file_base}.log")
        f.seek(0)
        f.write(content)
        f.truncate()

    # Modify main
    with open(target_main_path, 'r+', encoding='utf-8') as f:
        content = f.read()
        content = content.replace("{{BOT_SLUG}}", bot_name)
        content = content.replace("{{BOT_CLASS}}", class_name)
        content = content.replace("{{CONFIG_MODULE}}", f"config_auto_{config_name}")
        content = content.replace("{{LOGGER_NAME}}", bot_name.upper())
        content = content.replace("{{BOT_MODULE}}", file_base)
        f.seek(0)
        f.write(content)
        f.truncate()

    # Insert into DB
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(f"""
        INSERT INTO {SCHEMA}.bot_auth (bot_name, token)
        VALUES (%s, %s)
    """, (bot_name, hashed_token))

    metadata = {}
    cur.execute(f"""
        INSERT INTO {SCHEMA}.bots (bot_name, role_id, status, is_supervisor, metadata)
        VALUES (%s, %s, %s, %s, %s)
    """, (bot_name, ROLE_ID, 'stopped', False, json.dumps(metadata)))

    conn.commit()
    cur.close()
    conn.close()

    print("\n✅ Bot setup complete!")
    print(f"🔐 Auth Token: {token}")
    print(f"📁 Bot Code: {target_bot_path}")
    print(f"📁 Main File: {target_main_path}")
    print(f"📁 Config File: {target_config_path}")

if __name__ == "__main__":
    create_bot()


