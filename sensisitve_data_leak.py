import os
import re
import pathspec

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
    if os.path.exists('.gitignore'):
        with open('.gitignore', 'r') as f:
            return pathspec.PathSpec.from_lines('gitwildmatch', f)
    return None

def is_ignored(path, spec):
    if spec is None:
        return False
    return spec.match_file(path)

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
    spec = load_gitignore()
    script_name = os.path.basename(__file__)
    print("Scanning for sensitive information...\n")
    for root, dirs, files in os.walk('.'):
        # Remove ignored directories in-place
        dirs[:] = [d for d in dirs if not is_ignored(os.path.relpath(os.path.join(root, d), '.'), spec)]
        for file in files:
            rel_path = os.path.relpath(os.path.join(root, file), '.')
            # Ignore this script itself
            if file == script_name:
                continue
            if is_ignored(rel_path, spec):
                continue
            if file.endswith(SCAN_EXTENSIONS):
                matches = scan_file(os.path.join(root, file))
                if matches:
                    print(f"\n[!] Potential sensitive info in {rel_path}:")
                    for lineno, content in matches:
                        print(f"  Line {lineno}: {content}")

if __name__ == "__main__":
    main()