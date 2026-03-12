import sys
import sqlite3
import hashlib

try:
    import bcrypt
    HAS_BCRYPT = True
except Exception:
    HAS_BCRYPT = False


def verify_password(password: str, stored_hash: str) -> bool:
    if not stored_hash:
        return False
    if stored_hash.startswith("$2"):
        if not HAS_BCRYPT:
            return False
        return bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
    return hashlib.sha256(password.encode("utf-8")).hexdigest() == stored_hash


def main():
    if len(sys.argv) < 2:
        sys.exit(1)

    db_path = sys.argv[1]

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            print("ERR")
            sys.stdout.flush()
            continue

        parts = line.split(" ", 1)
        if len(parts) != 2:
            print("ERR")
            sys.stdout.flush()
            continue

        username, password = parts
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT password_hash, is_enabled, is_blocked FROM users WHERE username=?", (username,))
            row = cur.fetchone()
            conn.close()

            if not row:
                print("ERR")
            else:
                ph, enabled, blocked = row
                if not enabled or blocked:
                    print("ERR")
                elif verify_password(password, ph):
                    print("OK")
                else:
                    print("ERR")
        except Exception:
            print("ERR")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
