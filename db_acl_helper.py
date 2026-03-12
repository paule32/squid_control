import sys
import re
import sqlite3
import ipaddress
from datetime import datetime

WEEKDAY_MAP = {
    0: "mon",
    1: "tue",
    2: "wed",
    3: "thu",
    4: "fri",
    5: "sat",
    6: "sun",
}


def ip_in_network(cur, network_id, src_ip):
    if not network_id:
        return True
    cur.execute("SELECT cidr, is_enabled FROM networks WHERE id=?", (network_id,))
    row = cur.fetchone()
    if not row:
        return False
    cidr, enabled = row
    if not enabled:
        return False
    try:
        return ipaddress.ip_address(src_ip) in ipaddress.ip_network(cidr, strict=False)
    except Exception:
        return False


def time_allowed(cur, time_window_id):
    if not time_window_id:
        return True
    cur.execute("SELECT weekdays, start_time, end_time, is_enabled FROM time_windows WHERE id=?", (time_window_id,))
    row = cur.fetchone()
    if not row:
        return False
    weekdays, start_time, end_time, enabled = row
    if not enabled:
        return False

    now = datetime.now()
    wd = WEEKDAY_MAP[now.weekday()]
    hm = now.strftime("%H:%M")
    allowed = [x.strip().lower() for x in (weekdays or "").split(",") if x.strip()]
    if allowed and wd not in allowed:
        return False
    if start_time and hm < start_time:
        return False
    if end_time and hm > end_time:
        return False
    return True


def find_block(cur, uri):
    cur.execute("""
        SELECT b.id, b.pattern, b.category, b.is_regex, COALESCE(r.file_path, '')
          FROM blocked_urls b
     LEFT JOIN replacement_pages r ON r.id=b.replacement_page_id
         WHERE b.is_enabled=1
    """)
    for block_id, pattern, category, is_regex, repl in cur.fetchall():
        try:
            if is_regex:
                if re.search(pattern, uri, re.IGNORECASE):
                    return block_id, category, repl
            else:
                if pattern.lower() in uri.lower():
                    return block_id, category, repl
        except Exception:
            continue
    return None


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

        parts = line.split(" ", 2)
        if len(parts) < 3:
            print("ERR")
            sys.stdout.flush()
            continue

        username, src_ip, uri = parts

        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("""
                SELECT id, is_enabled, is_blocked, group_id, network_id, time_window_id
                  FROM users WHERE username=?
            """, (username,))
            user = cur.fetchone()

            if not user:
                print("ERR message=unknown_user")
                conn.close()
                sys.stdout.flush()
                continue

            _, enabled, blocked, group_id, network_id, time_window_id = user
            if not enabled or blocked:
                print("ERR message=user_blocked")
                conn.close()
                sys.stdout.flush()
                continue

            if group_id:
                cur.execute("SELECT is_enabled FROM groups WHERE id=?", (group_id,))
                g = cur.fetchone()
                if g and not g[0]:
                    print("ERR message=group_disabled")
                    conn.close()
                    sys.stdout.flush()
                    continue

            if not ip_in_network(cur, network_id, src_ip):
                print("ERR message=network_denied")
                conn.close()
                sys.stdout.flush()
                continue

            if not time_allowed(cur, time_window_id):
                print("ERR message=time_denied")
                conn.close()
                sys.stdout.flush()
                continue

            block = find_block(cur, uri)
            if block:
                _, category, replacement = block
                if replacement:
                    print(f"ERR message=blocked_{category} replacement={replacement}")
                else:
                    print(f"ERR message=blocked_{category}")
            else:
                print("OK")
            conn.close()
        except Exception:
            print("ERR")

        sys.stdout.flush()


if __name__ == "__main__":
    main()
