#!/usr/bin/env python3
"""校园瓜田后端：标准库 HTTP 服务 + SQLite 持久化。

启动：python server.py [端口]
访问：http://localhost:8123/
"""

import json
import mimetypes
import os
import re
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, "melon.db")
INDEX_FILE = "campus-melon.html"
MAX_BODY = 8 * 1024 * 1024

CAMPS = {"jc", "ns", "lh", "yd"}
CATS = {"food", "class", "view", "dorm", "club"}

# (campus, cat, title, text, author, minutes_ago, heat, bias)
SEEDS = [
    ("jc", "food", "三食堂的糖醋排骨，今天甜到犯规", "阿姨像是把整罐糖倒了进去，甜到牙疼但停不下来。", "饭点准时到", 12, 386, 0),
    ("jc", "class", "早八老师用随机数生成器点名", "他说这样公平，但每次都精准抽中我。", "被点到的第七个", 26, 512, 1),
    ("jc", "view", "图书馆后面的银杏道全黄了", "下午四点的光最好，建议逃一节课去看。", "带相机的", 60, 288, 0),
    ("jc", "dorm", "宿舍楼下的小猫蹲在快递柜上收件", "它连快递都不放过，路过的人都在给它递纸箱。", "六楼探子", 120, 640, 0),
    ("jc", "club", "动漫社在广场摆了个巨大痛包", "路过的时候被塞了一颗糖，社恐但很快乐。", "糖纸收藏家", 180, 197, 1),
    ("jc", "food", "二食堂的免费汤，今天只有汤没有料", "排队的人端着碗面面相觑，阿姨说这叫清汤寡水。", "汤里有故事", 240, 233, 3),

    ("ns", "food", "一食堂新开二十四小时关东煮", "凌晨两点还亮着灯，汤底是免费的安慰。", "夜宵雷达", 18, 720, 0),
    ("ns", "class", "实验课把电路接反，全组闻到焦味", "老师沉默三秒，说：这也是一种结果。", "电阻终结者", 45, 468, 1),
    ("ns", "view", "教学楼顶能看见整片晚霞", "保安大叔说可以上去，但别带奶茶。", "天台常客", 60, 350, 0),
    ("ns", "dorm", "隔壁宿舍用投影仪在走廊放电影", "幕布是床单，观众坐满了整段楼梯。", "走廊观众", 120, 590, 0),
    ("ns", "club", "机器人社的机器狗学会了握手", "它握完手还会点头，现场一片惊呼。", "螺丝刀", 180, 410, 0),
    ("ns", "class", "选修课签到码放在讲台上，没人敢拍", "老师说：拍了就当你没来。", "没来的人", 300, 168, 3),

    ("lh", "food", "海风食堂的蛤蜊汤鲜得像刚捞的", "喝到一半才发现窗口写着：今日限定。", "海风常驻", 22, 480, 0),
    ("lh", "view", "操场看台的日落配一轮粉紫色", "整片天像被人调过色，手机根本拍不出来。", "看台第一排", 40, 620, 0),
    ("lh", "class", "老师用电影当课件，连放三周", "这周终于讲到了片尾字幕。", "影迷旁听生", 60, 302, 2),
    ("lh", "dorm", "停水通知贴出后，整层楼都在排队接水", "走廊里全是水桶，像一场静悄悄的迁徙。", "三楼提水工", 120, 388, 1),
    ("lh", "club", "合唱团在操场随机开唱", "没有预告，围了半圈人，最后大家一起唱。", "和声路人", 180, 260, 0),
    ("lh", "food", "校门口新开的炸串，排队比上课还久", "等了二十分钟，吃的时候只剩三分钟。", "卡点选手", 360, 175, 3),

    ("yd", "food", "山顶食堂自产蔬菜，窗口写着今日现摘", "番茄真的有番茄味，这一口有点感动。", "山顶种菜人", 15, 540, 0),
    ("yd", "class", "高数老师上课带了一只鹦鹉", "它只在讲到极限的时候叫了一声。", "极限存在", 33, 690, 0),
    ("yd", "view", "雾散之后的校园像开了滤镜", "整条路从白色里浮出来，像加载完成的场景。", "等雾的人", 60, 610, 0),
    ("yd", "dorm", "宿舍阳台能看到流星雨", "整栋楼同时发出惊叹，宿管也跟着抬头。", "阳台观星", 120, 730, 0),
    ("yd", "club", "天文社搬了三台望远镜到操场", "排队看土星，队伍比食堂还整齐。", "土星环", 180, 355, 1),
    ("yd", "dorm", "熄灯后楼道里有人在跳绳", "数到一百五十下，突然安静了。", "隔墙听众", 300, 210, 3),
]

DB_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id         TEXT PRIMARY KEY,
    campus     TEXT NOT NULL,
    cat        TEXT NOT NULL,
    title      TEXT NOT NULL,
    text       TEXT NOT NULL DEFAULT '',
    author     TEXT NOT NULL,
    img        TEXT,
    art_seed   INTEGER NOT NULL DEFAULT 0,
    heat       INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS votes (
    post_id   TEXT NOT NULL,
    client_id TEXT NOT NULL,
    tier      INTEGER NOT NULL,
    ts        REAL NOT NULL,
    PRIMARY KEY (post_id, client_id)
);
CREATE INDEX IF NOT EXISTS idx_posts_campus ON posts(campus);
"""


def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def make_tiers(bias, total):
    """与前端一致的高斯分布：按偏好生成五档票数。"""
    import math

    weights = [math.exp(-((k - bias) ** 2) / 1.4) for k in range(5)]
    s = sum(weights)
    return [int(round(w / s * total)) for w in weights]


def init_db():
    with DB_LOCK, db() as conn:
        conn.executescript(SCHEMA)
        if conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]:
            return
        now = time.time()
        for i, seed in enumerate(SEEDS):
            campus, cat, title, text, author, mins, heat, bias = seed
            pid = "s%d" % i
            conn.execute(
                "INSERT INTO posts (id, campus, cat, title, text, author, img, art_seed, heat, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (pid, campus, cat, title, text, author, None, i * 131 + 7, heat, now - mins * 60),
            )
            total = max(40, min(260, round(heat * 0.6)))
            rows = []
            n = 0
            for tier, count in enumerate(make_tiers(bias, total)):
                for _ in range(count):
                    rows.append((pid, "seed:%s:%d" % (pid, n), tier, now - mins * 60))
                    n += 1
            conn.executemany(
                "INSERT INTO votes (post_id, client_id, tier, ts) VALUES (?,?,?,?)", rows
            )
        conn.commit()


def rel_time(ts):
    d = max(0, int(time.time() - ts))
    if d < 60:
        return "刚刚"
    if d < 3600:
        return "%d分钟前" % (d // 60)
    if d < 86400:
        return "%d小时前" % (d // 3600)
    return "%d天前" % (d // 86400)


def serial(posts, mine_map):
    """按 post_id 聚合票数，输出前端需要的结构。"""
    out = []
    for p in posts:
        pid = p["id"]
        tiers = [0, 0, 0, 0, 0]
        for tier, cnt in p["counts"] if "counts" in p.keys() else []:
            if 0 <= tier <= 4:
                tiers[tier] = cnt
        out.append(
            {
                "id": pid,
                "campus": p["campus"],
                "cat": p["cat"],
                "title": p["title"],
                "text": p["text"],
                "author": p["author"],
                "time": rel_time(p["created_at"]),
                "heat": p["heat"],
                "tiers": tiers,
                "mine": mine_map.get(pid),
                "img": p["img"],
                "artSeed": p["art_seed"],
            }
        )
    return out


def fetch_posts(conn, campus=None, client=None, post_id=None):
    sql = (
        "SELECT p.*, "
        "  (SELECT COUNT(*) FROM votes v WHERE v.post_id = p.id) AS total"
        " FROM posts p"
    )
    args = []
    where = []
    if post_id:
        where.append("p.id = ?")
        args.append(post_id)
    if campus:
        where.append("p.campus = ?")
        args.append(campus)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY p.created_at DESC"
    rows = conn.execute(sql, args).fetchall()

    tier_rows = conn.execute(
        "SELECT post_id, tier, COUNT(*) AS c FROM votes GROUP BY post_id, tier"
    ).fetchall()
    counts = {}
    for post_id_, tier, c in tier_rows:
        counts.setdefault(post_id_, []).append((tier, c))

    mine = {}
    if client:
        for pid, tier in conn.execute(
            "SELECT post_id, tier FROM votes WHERE client_id = ?", (client,)
        ):
            mine[pid] = tier

    items = []
    for r in rows:
        d = dict(r)
        d["counts"] = counts.get(d["id"], [])
        items.append(d)
    return serial(items, mine)


def one_post(conn, post_id, client):
    items = fetch_posts(conn, client=client, post_id=post_id)
    return items[0] if items else None


class Handler(BaseHTTPRequestHandler):
    server_version = "MelonServer/1.0"

    def log_message(self, fmt, *args):
        sys.stdout.write("%s - %s\n" % (self.address_string(), fmt % args))

    # ---------- 响应工具 ----------

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, text, status=200):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY:
            raise ValueError("bad length")
        return json.loads(self.rfile.read(length).decode("utf-8"))

    # ---------- 静态文件 ----------

    def serve_static(self, path):
        if path in ("/", ""):
            path = "/" + INDEX_FILE
        safe = os.path.normpath(unquote(path)).lstrip("\\/")
        if safe.startswith("..") or os.path.isabs(safe):
            self.send_text("forbidden", 403)
            return
        full = os.path.join(ROOT, safe)
        if not os.path.isfile(full):
            self.send_text("not found", 404)
            return
        ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
        with open(full, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if ctype.startswith("text/") else ""))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # ---------- 路由 ----------

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if not path.startswith("/api/"):
            self.serve_static(path)
            return

        try:
            if path == "/api/posts":
                campus = (query.get("campus") or [""])[0]
                client = (query.get("client") or [""])[0]
                if campus and campus not in CAMPS:
                    self.send_json({"error": "unknown campus"}, 400)
                    return
                with db() as conn:
                    posts = fetch_posts(conn, campus=campus or None, client=client or None)
                self.send_json({"posts": posts})
            elif path == "/api/counts":
                with db() as conn:
                    rows = conn.execute(
                        "SELECT campus, COUNT(*) AS c FROM posts GROUP BY campus"
                    ).fetchall()
                counts = {camp: 0 for camp in CAMPS}
                for campus, c in rows:
                    counts[campus] = c
                self.send_json({"counts": counts})
            elif path == "/api/health":
                self.send_json({"ok": True, "db": os.path.basename(DB_PATH)})
            else:
                self.send_json({"error": "not found"}, 404)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, 500)

    def do_POST(self):
        path = urlparse(self.path).path
        if not path.startswith("/api/"):
            self.send_json({"error": "not found"}, 404)
            return
        try:
            if path == "/api/posts":
                self.create_post()
            elif path == "/api/vote":
                self.cast_vote()
            elif path == "/api/join":
                self.join_crowd()
            else:
                self.send_json({"error": "not found"}, 404)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, 500)

    # ---------- 业务 ----------

    def create_post(self):
        data = self.read_json()
        campus = str(data.get("campus") or "")
        cat = str(data.get("cat") or "")
        title = str(data.get("title") or "").strip()
        text = str(data.get("text") or "").strip()
        author = str(data.get("author") or "我").strip() or "我"
        img = data.get("img")

        if campus not in CAMPS:
            raise ValueError("invalid campus")
        if cat not in CATS:
            raise ValueError("invalid category")
        if not title:
            raise ValueError("title required")
        if len(title) > 120:
            title = title[:120]
        if len(text) > 500:
            text = text[:500]
        if img is not None and not str(img).startswith("data:image/"):
            raise ValueError("invalid image")
        if img is not None and len(img) > 6 * 1024 * 1024:
            raise ValueError("image too large")

        pid = "u%d%03d" % (int(time.time() * 1000), int.from_bytes(os.urandom(2), "big") % 1000)
        art_seed = int.from_bytes(os.urandom(3), "big") % 99999
        with DB_LOCK, db() as conn:
            conn.execute(
                "INSERT INTO posts (id, campus, cat, title, text, author, img, art_seed, heat, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (pid, campus, cat, title, text, author, img, art_seed, 1, time.time()),
            )
            conn.commit()
            post = one_post(conn, pid, data.get("clientId"))
        self.send_json({"post": post})

    def cast_vote(self):
        data = self.read_json()
        post_id = str(data.get("postId") or "")
        client = str(data.get("clientId") or "")
        tier = data.get("tier")
        if not post_id or not client:
            raise ValueError("postId and clientId required")
        if not isinstance(tier, int) or not 0 <= tier <= 4:
            raise ValueError("invalid tier")

        with DB_LOCK, db() as conn:
            row = conn.execute("SELECT 1 FROM posts WHERE id = ?", (post_id,)).fetchone()
            if not row:
                raise ValueError("post not found")
            old = conn.execute(
                "SELECT tier FROM votes WHERE post_id = ? AND client_id = ?", (post_id, client)
            ).fetchone()
            if old is None:
                conn.execute(
                    "INSERT INTO votes (post_id, client_id, tier, ts) VALUES (?,?,?,?)",
                    (post_id, client, tier, time.time()),
                )
                gain = 5
            elif old[0] == tier:
                gain = 0
            else:
                conn.execute(
                    "UPDATE votes SET tier = ?, ts = ? WHERE post_id = ? AND client_id = ?",
                    (tier, time.time(), post_id, client),
                )
                gain = 2
            if gain:
                conn.execute("UPDATE posts SET heat = heat + ? WHERE id = ?", (gain, post_id))
            conn.commit()
            post = one_post(conn, post_id, client)
        self.send_json({"post": post})

    def join_crowd(self):
        data = self.read_json()
        post_id = str(data.get("postId") or "")
        client = str(data.get("clientId") or "")
        try:
            n = int(data.get("n") or 1)
        except (TypeError, ValueError):
            n = 1
        n = max(1, min(5, n))
        if not post_id:
            raise ValueError("postId required")

        with DB_LOCK, db() as conn:
            row = conn.execute("SELECT 1 FROM posts WHERE id = ?", (post_id,)).fetchone()
            if not row:
                raise ValueError("post not found")
            conn.execute("UPDATE posts SET heat = heat + ? WHERE id = ?", (n, post_id))
            conn.commit()
            post = one_post(conn, post_id, client or None)
        self.send_json({"post": post, "n": n})


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8123
    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print("校园瓜田已启动：http://localhost:%d/  (数据库: %s)" % (port, DB_PATH))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
