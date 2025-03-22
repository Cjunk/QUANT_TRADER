import secrets
raw_token = secrets.token_hex(32)
print("Your token:", raw_token)

import hashlib
hashed_token = hashlib.sha256(raw_token.encode()).hexdigest()
print("Hashed for DB:", hashed_token)