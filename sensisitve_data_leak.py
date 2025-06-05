import os
import re

# Patterns to look for (add more as needed)
SENSITIVE_PATTERNS = [
    r'password\s*=\s*[\'"].+[\'"]',
    r'passwd\s*=\s*[\'"].+[\'"]',
    r'api[_-]?key\s*=\s*[\'"].+[\'"]',
    r'secret\s*=\s*[\'"].+[\'"]',
    r'token\s*=\s*[\'"].+[\'"]',
    r'DB_USER\s*=\s*[\'"].+[\'"]',
    r'DB_PASSWORD\s*=\s*[\'"].+[\'"]',
    r'DB_HOST\s*=\s*[\'"].+[\'"]',
    r'DB_PORT\s*=\s*[\'"].+[\'"]',
    r'DB_NAME\s*=\s*[\'"].+[\'"]',
    r'BOT_AUTH_TOKEN\s*=\s*[\'"].+[\'"]',
    r'PRIVATE_KEY\s*=\s*[\'"].+[\'"]',
    r'client_secret\s*=\s*[\'"].+[\'"]',
    r'client_id\s*=\s*[\'"].+[\'"]',
    r'-----BEGIN PRIVATE KEY-----',
    r'-----BEGIN RSA PRIVATE KEY-----',
    r'-----BEGIN EC PRIVATE KEY-----',
    r'-----BEGIN DSA PRIVATE KEY-----',
    r'-----BEGIN OPENSSH PRIVATE KEY-----',
]

# File extensions to scan
SCAN_EXTENSIONS = ('.py', '.php', '.env', '.json', '.yml', '.yaml', '.ini', '.cfg', '.conf', '.txt')

# Read .gitignore to skip ignored files/folders
def load_gitignore():
    ignored = set()
    if os.path.exists('.gitignore'):
        with open('.gitignore', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    ignored.add(line.rstrip('/'))
    return ignored

def is_ignored(path, ignored):
    basename = os.path.basename(path)
    # Ignore this script itself
    if basename == os.path.basename(__file__):
        return True
    for ignore in ignored:
        # Ignore exact matches (e.g., .env, sensisitve_data_leak.py)
        if basename == ignore or path == ignore:
            return True
        # Ignore patterns like config*.py anywhere in the tree
        if ignore.endswith('*'):
            if basename.startswith(ignore.rstrip('*')):
                return True
            # Also check for patterns like */config*.py
            if ignore.startswith('*/') and basename.startswith(ignore.lstrip('*/').rstrip('*')):
                return True
        # Ignore directories (ending with /)
        if ignore.endswith('/') and (path.startswith(ignore.rstrip('/')) or basename == ignore.rstrip('/')):
            return True
    return False

def scan_file(filepath):
    results = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                for pat in SENSITIVE_PATTERNS:
                    if re.search(pat, line, re.IGNORECASE):
                        results.append((i, line.strip()))
    except Exception as e:
        pass
    return results

def main():
    ignored = load_gitignore()
    print("Scanning for sensitive information...\n")
    for root, dirs, files in os.walk('.'):
        # Skip ignored directories
        dirs[:] = [d for d in dirs if not is_ignored(os.path.relpath(os.path.join(root, d), '.'), ignored)]
        for file in files:
            if file.endswith(SCAN_EXTENSIONS):
                rel_path = os.path.relpath(os.path.join(root, file), '.')
                if is_ignored(rel_path, ignored):
                    continue
                matches = scan_file(os.path.join(root, file))
                if matches:
                    print(f"\n[!] Potential sensitive info in {rel_path}:")
                    for lineno, content in matches:
                        print(f"  Line {lineno}: {content}")

if __name__ == "__main__":
    main()