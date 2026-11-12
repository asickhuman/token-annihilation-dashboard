#!/usr/bin/env python3
"""Token Annihilation Dashboard: transparent Ollama token-counting proxy + tiny dashboard. Stdlib only."""
import http.client, http.server, json, math, os, re, sqlite3, subprocess, sys, threading, time, html
from datetime import datetime
from pathlib import Path
from socketserver import ThreadingMixIn

HOME = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HOME, "stats.db")
PRICES = os.path.join(HOME, "prices.json")
UP_HOST, UP_PORT = "127.0.0.1", 11434
PROXY_BIND, PROXY_PORT = os.environ.get("PROXY_BIND", "0.0.0.0"), int(os.environ.get("PROXY_PORT", 11435))
def _tailscale_ip():
    try:
        ip = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=5).stdout.split()
        return ip[0] if ip else "127.0.0.1"
    except Exception:
        return "127.0.0.1"
UP_HOST = os.environ.get("UPSTREAM_HOST", UP_HOST); UP_PORT = int(os.environ.get("UPSTREAM_PORT", UP_PORT))
DASH_BIND, DASH_PORT = os.environ.get("DASH_BIND", "127.0.0.1"), int(os.environ.get("DASH_PORT", 11480))
TRACK = ("/api/generate", "/api/chat", "/v1/chat/completions", "/v1/completions", "/v1/responses", "/api/embed", "/api/embeddings", "/v1/embeddings")
HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "proxy-connection"}
lock = threading.Lock()

# --- Cloud usage (Codex/GPT, Grok Bot, OpenCode, …) ---
CODEXQ_ROOT = os.path.expanduser("~/codexq")
CODEX_PACE = os.path.join(CODEXQ_ROOT, "state", "pace.txt")
CODEX_LIMITS_CSV = os.path.join(CODEXQ_ROOT, "usage", "codex-limits.csv")
CODEX_SESSIONS = os.path.expanduser("~/.codex/sessions")
CLOUD_DIR = os.path.expanduser("~/.token-annihilator")
GROK_CLOUD_JSON = os.path.join(CLOUD_DIR, "cloud_grok.json")
# Grok cloud JSON exposes plan used_pct only (no token counts on disk).
# Calibrated 2026-10-30 from local Codex Plus week_tok/weekly%:
#   182,433,588 tok / 65.0% ≈ 2,806,671 tok per plan-percent.
# Used as stand-in for Grok Super until real Grok token telemetry exists.
# Override: export GROK_TOKENS_PER_PCT=...
GROK_TOKENS_PER_PCT = float(os.environ.get("GROK_TOKENS_PER_PCT", "2806671"))
try:
    os.makedirs(CLOUD_DIR, exist_ok=True)
except Exception:
    pass


# --- Spark thermal (GB10 SoC CPU + GPU) + RAM ---
# acpitz zones trip critical at 104.8C; warn/throttle stay below that.
CPU_TEMP_WARN_C = 85.0
CPU_TEMP_THROTTLE_C = 95.0
GPU_TEMP_WARN_C = 85.0
GPU_TEMP_HOT_C = 95.0
RAM_AVAIL_WARN_PCT = 10.0
RAM_AVAIL_CRIT_PCT = 5.0

def _read_cpu_temp_c():
    """Max across ACPI thermal zones (acpitz) — SoC/CPU proxy on GB10."""
    temps = []
    try:
        for z in Path("/sys/class/thermal").glob("thermal_zone*"):
            try:
                if (z / "type").read_text().strip() != "acpitz":
                    continue
                temps.append(int((z / "temp").read_text().strip()) / 1000.0)
            except Exception:
                pass
    except Exception:
        pass
    return max(temps) if temps else None

def _read_gpu_temp_c():
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        vals = [float(x) for x in out.replace(",", " ").split() if x]
        return max(vals) if vals else None
    except Exception:
        return None

def _read_ram_avail_pct():
    try:
        mi = {}
        for line in open("/proc/meminfo"):
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            mi[k] = int(v.split()[0])
        return 100.0 * mi["MemAvailable"] / mi["MemTotal"]
    except Exception:
        return None

def _temp_light(kind, temp_c, warn_c, hot_c):
    if temp_c is None:
        return ("%s --C" % kind, False, "amber")
    t = int(round(temp_c))
    label = "%s %dC" % (kind, t)
    if temp_c >= hot_c:
        return (label, True, "red")
    if temp_c >= warn_c:
        return (label, True, "amber")
    return (label, True, "green")

def _ram_light(avail_pct):
    if avail_pct is None:
        return ("RAM --%", False, "amber")
    p = int(round(avail_pct))
    label = "RAM %d%%" % p
    if avail_pct < RAM_AVAIL_CRIT_PCT:
        return (label, True, "red")
    if avail_pct < RAM_AVAIL_WARN_PCT:
        return (label, True, "amber")
    return (label, True, "green")

def spark_temp_lights():
    return [
        _temp_light("GPU", _read_gpu_temp_c(), GPU_TEMP_WARN_C, GPU_TEMP_HOT_C),
        _temp_light("CPU", _read_cpu_temp_c(), CPU_TEMP_WARN_C, CPU_TEMP_THROTTLE_C),
        _ram_light(_read_ram_avail_pct()),
    ]


def _kv_file(path):
    out = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip()
    except Exception:
        pass
    return out

def _fmt_reset(ts):
    try:
        ts = float(ts)
        if ts > 1e12:
            ts /= 1000.0
        return datetime.fromtimestamp(ts).strftime("%b %d %I:%M%p").replace(" 0", " ")
    except Exception:
        return "—"

def _read_tail_lines(path, max_bytes=512_000):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
                f.readline()
            return f.read().decode("utf-8", "replace").splitlines()
    except Exception:
        return []

def _codex_rate_limits_from_rollout():
    """Latest payload.rate_limits from newest rollout jsonl (scan a few recent files)."""
    try:
        paths = []
        for root, _dirs, files in os.walk(CODEX_SESSIONS):
            for name in files:
                if name.startswith("rollout-") and name.endswith(".jsonl"):
                    p = os.path.join(root, name)
                    try:
                        paths.append((os.path.getmtime(p), p))
                    except Exception:
                        pass
        paths.sort(reverse=True)
        for _mtime, p in paths[:12]:
            for line in reversed(_read_tail_lines(p)):
                if "rate_limits" not in line:
                    continue
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                pl = o.get("payload") if isinstance(o.get("payload"), dict) else o
                rl = None
                if isinstance(pl, dict):
                    rl = pl.get("rate_limits")
                    if not rl and isinstance(pl.get("info"), dict):
                        rl = pl["info"].get("rate_limits")
                if isinstance(rl, dict) and ("primary" in rl or "secondary" in rl):
                    return rl, p
    except Exception:
        pass
    return None, None

def _codex_rate_limits_from_csv():
    """Fallback: last row of codex-limits.csv."""
    try:
        with open(CODEX_LIMITS_CSV) as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        if len(lines) < 2:
            return None
        # ts,job,primary_used_pct,primary_window_min,primary_resets_at,secondary_used_pct,secondary_window_min,secondary_resets_at,plan_type,...
        parts = lines[-1].split(",")
        if len(parts) < 9:
            return None
        return {
            "plan_type": parts[8] or None,
            "primary": {
                "used_percent": float(parts[2]),
                "window_minutes": int(float(parts[3])),
                "resets_at": float(parts[4]),
            },
            "secondary": {
                "used_percent": float(parts[5]),
                "window_minutes": int(float(parts[6])),
                "resets_at": float(parts[7]),
            },
        }
    except Exception:
        return None

def _codex_session_token_totals():
    """Sum last total_token_usage.total_tokens per rollout: today, trailing 7d, all stored history."""
    now = time.time()
    week_ago = now - 7 * 86400
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today = week = alltime = 0
    try:
        for root, _dirs, files in os.walk(CODEX_SESSIONS):
            for name in files:
                if not (name.startswith("rollout-") and name.endswith(".jsonl")):
                    continue
                p = os.path.join(root, name)
                try:
                    mtime = os.path.getmtime(p)
                except Exception:
                    continue
                last = None
                for line in _read_tail_lines(p, 256_000):
                    if "token_count" not in line or "total_token_usage" not in line:
                        continue
                    try:
                        o = json.loads(line)
                    except Exception:
                        continue
                    pl = o.get("payload") if isinstance(o.get("payload"), dict) else None
                    if not isinstance(pl, dict) or pl.get("type") != "token_count":
                        continue
                    info = pl.get("info") or {}
                    tu = info.get("total_token_usage") or {}
                    if isinstance(tu, dict) and tu.get("total_tokens") is not None:
                        last = int(tu.get("total_tokens") or 0)
                if last is None:
                    continue
                alltime += last
                if mtime >= week_ago:
                    week += last
                if mtime >= today_start:
                    today += last
    except Exception:
        pass
    return today, week, alltime

def _pct_meter_html(n, pct, rising=True):
    """Segmented gauge; warn/redline at high quota use."""
    try:
        frac = max(0.0, min(1.0, float(pct) / 100.0))
    except Exception:
        frac = 0.0
    lit = int(round(frac * n))
    warn_from, red_from = int(0.70 * n), int(0.85 * n)
    parts = []
    for i in range(n):
        h = (28 + int(72 * (i / max(n - 1, 1)))) if rising else 70
        if i < lit:
            cls = "seg-on redline" if i >= red_from else ("seg-on warn" if i >= warn_from else "seg-on")
        else:
            cls = "seg-off"
        parts.append("<i class='%s' style='height:%d%%'></i>" % (cls, h))
    return "".join(parts)


def cloud_total_tokens():
    """ /cloud hero: Codex all-history session tokens + Grok used_pct * GROK_TOKENS_PER_PCT. """
    try:
        _today, _week, codex = _codex_session_token_totals()
    except Exception:
        codex = 0
    grok_tok = 0
    try:
        with open(GROK_CLOUD_JSON) as f:
            grok = json.load(f)
        if isinstance(grok, dict) and grok.get("used_pct") is not None:
            grok_tok = int(round(float(grok["used_pct"]) * float(GROK_TOKENS_PER_PCT)))
    except Exception:
        grok_tok = 0
    codex = int(codex or 0)
    grok_tok = int(grok_tok or 0)
    return codex + grok_tok, codex, grok_tok


OPENCODE_DB = os.path.expanduser("~/.local/share/opencode/opencode.db")
CLOUD_SNAP_JSONL = os.path.join(CLOUD_DIR, "cloud_snapshots.jsonl")

BOT_AVATARS = {
    "Veil PM": ("#FF6700", "pumpkin"),
    "dr eggbot": ("#F4F2EE", "teardrop"),
    "Zonk Bot": ("#9159FE", "cloud"),
    "Veil Assets": ("#FF309B", "flower"),
    "Veil Triage": ("#FF263C", "wedge"),
    "Model Evals": ("#1084FE", "sparkle"),
    "Token CEO": ("#00C972", "hex"),
}

def _bot_avatar_svg(name, size=28):
    color, shape = BOT_AVATARS.get(name, ("#888888", "hex"))
    c = html.escape(color, quote=True)
    # Near-white / pastel fills need a soft outline so they read on light cards.
    def _lum(hex_color):
        h = hex_color.lstrip("#")
        if len(h) != 6:
            return 0.5
        try:
            r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        except Exception:
            return 0.5
        return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0
    stroke_attr = ' stroke="#2a2a2a" stroke-width="1.35" stroke-opacity="0.55"' if _lum(color) >= 0.78 else ""
    paths = {
        "pumpkin": '<ellipse cx="16" cy="18" rx="11" ry="9.5" fill="%s"%s/><rect x="14.5" y="6" width="3" height="4" rx="1" fill="#6b4a2a"/>' % (c, stroke_attr),
        "teardrop": '<path d="M16 4 C24 14 24 21 16 28 C8 21 8 14 16 4 Z" fill="%s"%s/>' % (c, stroke_attr),
        "cloud": '<circle cx="11" cy="18" r="5.5" fill="%s"%s/><circle cx="17" cy="15" r="7" fill="%s"%s/><circle cx="23" cy="18" r="5" fill="%s"%s/><rect x="9" y="18" width="16" height="6" rx="3" fill="%s"%s/>' % (c, stroke_attr, c, stroke_attr, c, stroke_attr, c, stroke_attr),
        "flower": '<circle cx="16" cy="10" r="4.5" fill="%s"%s/><circle cx="16" cy="22" r="4.5" fill="%s"%s/><circle cx="10" cy="16" r="4.5" fill="%s"%s/><circle cx="22" cy="16" r="4.5" fill="%s"%s/><circle cx="16" cy="16" r="3.2" fill="%s"%s/>' % (c, stroke_attr, c, stroke_attr, c, stroke_attr, c, stroke_attr, c, stroke_attr),
        "wedge": '<path d="M16 5 L27 25 L5 25 Z" fill="%s"%s/>' % (c, stroke_attr),
        "sparkle": '<path d="M16 3 L19 13 L29 16 L19 19 L16 29 L13 19 L3 16 L13 13 Z" fill="%s"%s/>' % (c, stroke_attr),
        "hex": '<path d="M16 4 L27 10.5 L27 21.5 L16 28 L5 21.5 L5 10.5 Z" fill="%s"%s/>' % (c, stroke_attr),
    }
    body = paths.get(shape, paths["hex"])
    return '<svg class="bot-av" width="%d" height="%d" viewBox="0 0 32 32" aria-hidden="true">%s</svg>' % (size, size, body)

def _opencode_model_label(raw):
    if not raw:
        return "local"
    try:
        if isinstance(raw, str) and raw.startswith("{"):
            o = json.loads(raw)
            if isinstance(o, dict):
                return str(o.get("id") or "local")
    except Exception:
        pass
    return str(raw)[:48]

def _cloud_snapshot_log():
    try:
        os.makedirs(CLOUD_DIR, exist_ok=True)
        entry = {"ts": time.time(), "iso": datetime.now().isoformat(timespec="seconds")}
        try:
            with open(GROK_CLOUD_JSON) as f:
                entry["grok"] = json.load(f)
        except Exception:
            entry["grok"] = None
        try:
            today, week, alltime = _codex_session_token_totals()
            entry["codex"] = {"today": today, "week": week, "all": alltime}
        except Exception:
            entry["codex"] = None
        with open(CLOUD_SNAP_JSONL, "a") as f:
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")
    except Exception:
        pass

def _opencode_model_parts(raw):
    """Return (model_id, provider_id) from session_v2.model JSON/string. Never logs secrets."""
    if not raw:
        return ("", "")
    try:
        if isinstance(raw, str) and raw.startswith("{"):
            o = json.loads(raw)
            if isinstance(o, dict):
                return (str(o.get("id") or ""), str(o.get("providerID") or o.get("provider") or ""))
    except Exception:
        pass
    return (str(raw)[:64], "")


def _opencode_bucket(model_id, provider_id):
    """Classify OpenCode session: local spark, deepseek-cloud, or other-opencode-cloud."""
    mid = (model_id or "").lower()
    pid = (provider_id or "").lower()
    if pid in ("spark-local", "spark-ollama") or mid.startswith("qwen") or "ollama" in pid:
        return "local"
    if "deepseek" in mid:
        return "deepseek"
    if pid == "opencode":
        return "opencode-other"
    return "other"


def _opencode_range_bounds_ms(range_key="all"):
    from datetime import timedelta
    today = _today_et()
    def day_start_ms(d):
        try:
            from zoneinfo import ZoneInfo
            dt = datetime(d.year, d.month, d.day, tzinfo=ZoneInfo("America/New_York"))
        except Exception:
            from datetime import timezone, timedelta as td
            dt = datetime(d.year, d.month, d.day, tzinfo=timezone(td(hours=-5)))
        return int(dt.timestamp() * 1000)
    key = (range_key or "all").lower()
    key = RANGE_ALIASES.get(key, key)
    if key not in ("all", "today", "dod", "mtd", "weekly", "monthly", "yearly"):
        key = "all"
    if key == "today":
        return key, day_start_ms(today), day_start_ms(today + timedelta(days=1))
    if key == "dod":
        # Rolling last 24 hours ending now (ms), not calendar day.
        now_ms = int(time.time() * 1000)
        return key, now_ms - 24 * 60 * 60 * 1000, now_ms
    if key == "weekly":
        return key, day_start_ms(today - timedelta(days=6)), day_start_ms(today + timedelta(days=1))
    if key == "mtd":
        return key, day_start_ms(today.replace(day=1)), day_start_ms(today + timedelta(days=1))
    if key == "monthly":
        return key, day_start_ms(today - timedelta(days=29)), day_start_ms(today + timedelta(days=1))
    if key == "yearly":
        return key, day_start_ms(today.replace(month=1, day=1)), day_start_ms(today + timedelta(days=1))
    return key, None, None


def _opencode_agg(range_key="all", bucket="deepseek"):
    """Sum tokens/sessions from opencode.db for a bucket. No credentials touched."""
    import sqlite3
    path = OPENCODE_DB
    empty = {"ok": False, "tin": 0, "tout": 0, "treas": 0, "tot": 0, "n": 0,
             "cost": 0.0, "top_model": "—", "note": "", "range_key": range_key}
    if not os.path.isfile(path):
        empty["note"] = "missing ~/.local/share/opencode/opencode.db"
        return empty
    key, start_ms, end_ms = _opencode_range_bounds_ms(range_key)
    try:
        con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
        try:
            # Per-response attribution: the model that actually served each assistant message.
            q = ("SELECT json_extract(data,'$.model'),"
                 "COALESCE(json_extract(data,'$.tokens.input'),0),"
                 "COALESCE(json_extract(data,'$.tokens.output'),0),"
                 "COALESCE(json_extract(data,'$.tokens.reasoning'),0),"
                 "COALESCE(json_extract(data,'$.cost'),0) FROM session_message WHERE type='assistant'")
            if start_ms is None:
                rows = con.execute(q).fetchall()
            else:
                rows = con.execute(q + " AND time_created>=? AND time_created<?", (start_ms, end_ms)).fetchall()
        finally:
            con.close()
    except Exception as e:
        empty["note"] = "db error: %s" % e
        return empty
    tin = tout = treas = n = 0
    cost = 0.0
    by_model = {}
    for raw, a, b, c, cst in rows:
        mid, pid = _opencode_model_parts(raw)
        if _opencode_bucket(mid, pid) != bucket:
            continue
        a, b, c = int(a or 0), int(b or 0), int(c or 0)
        tin += a; tout += b; treas += c; n += 1
        try:
            cost += float(cst or 0)
        except Exception:
            pass
        by_model[mid or "?"] = by_model.get(mid or "?", 0) + a + b + c
    top = max(by_model.items(), key=lambda kv: kv[1])[0] if by_model else "—"
    return {
        "ok": True, "tin": tin, "tout": tout, "treas": treas,
        "tot": tin + tout + treas, "n": n, "cost": cost,
        "top_model": top, "note": "", "range_key": key,
    }


def _deepseek_via_opencode_provider(range_key="all"):
    """Cloud card: DeepSeek traffic routed through OpenCode (from local session DB)."""
    agg = _opencode_agg(range_key, bucket="deepseek")
    if not agg["ok"] and "missing" in (agg.get("note") or ""):
        return {
            "id": "deepseek-opencode", "title": "DeepSeek (via OpenCode)",
            "status": "not_linked", "plan": "via OpenCode",
            "meters": [], "stats": [],
            "note": "no data yet — need readable ~/.local/share/opencode/opencode.db",
            "range_real": False,
        }
    if not agg["ok"]:
        return {
            "id": "deepseek-opencode", "title": "DeepSeek (via OpenCode)",
            "status": "awaiting", "plan": "via OpenCode",
            "meters": [], "stats": [],
            "note": "no data yet — %s" % (agg.get("note") or "db unreadable"),
            "range_real": False,
        }
    if agg["n"] == 0 and agg["tot"] == 0:
        return {
            "id": "deepseek-opencode", "title": "DeepSeek (via OpenCode)",
            "status": "awaiting", "plan": "via OpenCode",
            "meters": [], "stats": [],
            "note": "no data yet — no DeepSeek sessions in opencode.db for this range",
            "range_real": True, "range_key": agg["range_key"],
        }
    tot, n = agg["tot"], agg["n"]
    key = agg["range_key"]
    pct = min(100.0, 100.0 * tot / 2_000_000.0) if key in ("today", "dod") else (100.0 if tot else 0.0)
    spend_note = (
        "spend $0 in local logs (OpenCode free-tier cost field empty/zero; "
        "paid DeepSeek API billing not present on disk)"
        if agg["cost"] <= 0 else
        ("spend $%.4f from session cost field" % agg["cost"])
    )
    return {
        "id": "deepseek-opencode",
        "title": "DeepSeek (via OpenCode)",
        "status": "live",
        "plan": "via OpenCode · free-tier logged",
        "meters": [{
            "label": "DeepSeek tokens",
            "pct": pct,
            "detail": "%s · %s sessions · %s" % (agg["top_model"], "{:,}".format(n), spend_note),
        }],
        "stats": [
            {"label": "Tokens", "value": tot},
            {"label": "Sessions", "value": n},
            {"label": "In", "value": agg["tin"]},
            {"label": "Out", "value": agg["tout"]},
        ],
        "note": "%s · reasoning %s · %s" % (
            "{:,}".format(n) + " sessions", "{:,}".format(agg["treas"]), spend_note),
        "range_real": True, "range_key": key, "top_model": agg["top_model"],
    }


def _opencode_provider(range_key="all"):
    """Back-compat: cloud uses DeepSeek split."""
    return _deepseek_via_opencode_provider(range_key)


def load_cloud_providers(range_key="all"):
    """Data-driven provider list for the CLOUD USAGE panel. Add entries to extend."""
    _cloud_snapshot_log()
    providers = []
    range_key = (range_key or "all").lower()
    range_key = RANGE_ALIASES.get(range_key, range_key)
    if range_key not in ("all", "today", "dod", "mtd", "weekly", "monthly", "yearly"):
        range_key = "all"

    # 1) Codex / GPT
    pace = _kv_file(CODEX_PACE)
    rl, _src = _codex_rate_limits_from_rollout()
    if not rl:
        rl = _codex_rate_limits_from_csv()
    primary = (rl or {}).get("primary") or {}
    secondary = (rl or {}).get("secondary") or {}
    plan = (rl or {}).get("plan_type") or pace.get("plan") or "—"
    # prefer pace weekly when present
    weekly_pct = None
    try:
        weekly_pct = float(pace.get("weekly%"))
    except Exception:
        try:
            weekly_pct = float(secondary.get("used_percent"))
        except Exception:
            weekly_pct = None
    try:
        primary_pct = float(primary.get("used_percent"))
    except Exception:
        primary_pct = None
    target = pace.get("target%-by-now")
    status = pace.get("status") or ""
    weekly_reset = _fmt_reset(pace.get("secondary_resets_at") or secondary.get("resets_at"))
    primary_reset = _fmt_reset(primary.get("resets_at"))
    today_tok, week_tok, _codex_all = _codex_session_token_totals()
    meters = []
    if primary_pct is not None:
        meters.append({
            "label": "5h window",
            "pct": primary_pct,
            "detail": "resets " + primary_reset,
        })
    if weekly_pct is not None:
        detail = "resets " + weekly_reset
        if target:
            detail = "pace %s%% · %s · %s" % (target, status or "—", detail)
        meters.append({
            "label": "Weekly",
            "pct": weekly_pct,
            "detail": detail,
        })
    stats = []
    if range_key == "dod":
        now_ts = time.time()
        h24 = _codex_tokens_window(now_ts - 86400, now_ts)
        stats.append({"label": "24H tok", "value": h24})
    elif today_tok or week_tok:
        stats.append({"label": "Today tok", "value": today_tok})
        stats.append({"label": "Week tok", "value": week_tok})
    providers.append({
        "id": "codex",
        "title": "Codex / GPT",
        "status": "live" if meters else "awaiting",
        "plan": plan,
        "meters": meters,
        "stats": stats,
        "note": "" if meters else "awaiting data",
    })

    # 2) Grok Bot — external JSON
    grok = None
    try:
        with open(GROK_CLOUD_JSON) as f:
            grok = json.load(f)
    except Exception:
        grok = None
    if isinstance(grok, dict) and grok:
        meters = []
        if grok.get("used_pct") is not None:
            meters.append({
                "label": "Plan used",
                "pct": float(grok["used_pct"]),
                "detail": ("resets " + str(grok["resets"])) if grok.get("resets") else "",
            })
        for b in (grok.get("bots") or []):
            if not isinstance(b, dict):
                continue
            if b.get("pct") is None:
                continue
            meters.append({
                "label": str(b.get("name") or "bot"),
                "pct": float(b["pct"]),
                "detail": "",
            })
        grok_stats = []
        if grok.get("used_pct") is not None:
            try:
                grok_week_tok = int(round(float(grok["used_pct"]) * float(GROK_TOKENS_PER_PCT)))
            except Exception:
                grok_week_tok = 0
            if grok_week_tok:
                grok_stats.append({"label": "Week tok", "value": grok_week_tok})
        providers.append({
            "id": "grok",
            "title": "Grok Bot",
            "status": "live",
            "plan": str(grok.get("plan") or "—"),
            "meters": meters,
            "stats": grok_stats,
            "note": ("updated " + str(grok["updated"])) if grok.get("updated") else "",
        })
    else:
        providers.append({
            "id": "grok",
            "title": "Grok Bot",
            "status": "awaiting",
            "plan": "—",
            "meters": [],
            "stats": [],
            "note": "awaiting data",
        })

    # 3) DeepSeek via OpenCode (cloud). Spark-local OpenCode usage lives on LOCAL.
    providers.append(_deepseek_via_opencode_provider(range_key))
    return providers

def render_cloud_usage_cards(providers, compact=False, show_stats=None):
    """Render provider cards from the JSON-shaped provider list."""
    cards = []
    segs = 16 if compact else 20
    if show_stats is None:
        show_stats = bool(compact)  # mobile shows tok totals; desktop side-col hides them
    for p in providers:
        status = p.get("status") or "awaiting"
        plan = html.escape(str(p.get("plan") or "—"))
        title = html.escape(str(p.get("title") or p.get("id") or "provider"))
        note = html.escape(str(p.get("note") or ""))
        body = []
        if status == "not_linked":
            body.append("<div class='cluse-note'>not linked yet</div>")
        elif status == "awaiting" and not p.get("meters"):
            body.append("<div class='cluse-note'>awaiting data</div>")
        else:
            for m in p.get("meters") or []:
                pct = m.get("pct")
                try:
                    pct_f = float(pct)
                    pct_s = ("%d" % int(round(pct_f))) if abs(pct_f - round(pct_f)) < 0.05 else ("%.1f" % pct_f)
                except Exception:
                    pct_s = "—"
                    pct_f = 0.0
                raw_lab = str(m.get("label") or "Usage")
                lab = html.escape(raw_lab)
                det = html.escape(str(m.get("detail") or ""))
                meter = _pct_meter_html(segs, pct_f, True)
                av = (_bot_avatar_svg(raw_lab) + " ") if (p.get("id") == "grok" and raw_lab in BOT_AVATARS) else ""
                body.append(
                    "<div class=cluse-meter>"
                    "<div class=cluse-meter-hd>"
                    "<span class=lbl>%s%s</span>"
                    "<span class='cluse-pct vfd'>%s%%</span>"
                    "</div>"
                    "<div class='tach-track cluse-tach'>%s</div>"
                    "%s"
                    "</div>"
                    % (av, lab, pct_s, meter,
                       ("<div class='cluse-detail muted'>%s</div>" % det) if det else "")
                )
            for s in (p.get("stats") or []) if show_stats else []:
                lab = html.escape(str(s.get("label") or ""))
                try:
                    val = int(s.get("value") or 0)
                    val_s = format(val, ",")
                except Exception:
                    val_s = html.escape(str(s.get("value") or "—"))
                body.append(
                    "<div class=cluse-stat>"
                    "<span class=lbl>%s</span>"
                    "<span class='cluse-stat-val vfd'>%s</span>"
                    "</div>" % (lab, val_s)
                )
            if note and status == "live":
                body.append("<div class='cluse-detail muted'>%s</div>" % note)
        cards.append(
            "<div class='card cluse-card status-%s' data-provider='%s'>"
            "<div class=cluse-card-hd>"
            "<div class=cluse-title>%s</div>"
            "<div class='cluse-plan lbl'>plan %s</div>"
            "</div>"
            "%s</div>"
            % (html.escape(status), html.escape(str(p.get("id") or "")),
               title, plan, "".join(body))
        )
    return "".join(cards)


def db():
    c = sqlite3.connect(DB, timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY, ts REAL, model TEXT, endpoint TEXT,
      input_tokens INTEGER, output_tokens INTEGER, duration_s REAL, eval_s REAL, status INTEGER, source TEXT, dedupe TEXT UNIQUE, request_count INTEGER DEFAULT 1)""")
    return c

def log_row(**r):
    try:
        with lock:
            c = db(); c.execute("INSERT OR IGNORE INTO requests(ts,model,endpoint,input_tokens,output_tokens,duration_s,eval_s,status,source,dedupe) VALUES(?,?,?,?,?,?,?,?,?,?)",
              (r.get("ts"), r.get("model"), r.get("endpoint"), r.get("inp"), r.get("out"), r.get("dur"), r.get("eval_s"), r.get("status"), r.get("source"), r.get("dedupe"))); c.commit(); c.close()
    except Exception as e:
        print("log error:", e, file=sys.stderr)

def parse_usage(endpoint, req_body, tail):
    model = inp = out = eval_s = None
    try: model = json.loads(req_body or b"{}").get("model")
    except Exception: pass
    text = tail.decode("utf-8", "replace")
    cands = [text] + [l[5:].strip() if l.startswith("data:") else l for l in reversed(text.splitlines())]
    for cand in cands:
        try: o = json.loads(cand)
        except Exception: continue
        if not isinstance(o, dict): continue
        model = o.get("model") or model
        if "prompt_eval_count" in o or "eval_count" in o:
            inp, out = o.get("prompt_eval_count"), o.get("eval_count")
            if o.get("eval_duration"): eval_s = o["eval_duration"] / 1e9
            break
        u = o.get("usage")
        if isinstance(u, dict):
            inp = u.get("prompt_tokens", u.get("input_tokens"))
            out = u.get("completion_tokens", u.get("output_tokens", 0))
            break
    return model, inp, out, eval_s

class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def handle_any(self):
        t0 = time.time()
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else None
        up = http.client.HTTPConnection(UP_HOST, UP_PORT, timeout=3600)
        hdrs = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
        try:
            up.request(self.command, self.path, body=body, headers=hdrs)
            r = up.getresponse()
        except Exception as e:
            self.send_error(502, "upstream error: %s" % e); return
        path = self.path.split("?")[0]
        track = self.command == "POST" and path in TRACK
        self.send_response(r.status, r.reason)
        chunked = r.getheader("Content-Length") is None and self.command != "HEAD" and r.status not in (204, 304)
        for k, v in r.getheaders():
            if k.lower() not in HOP: self.send_header(k, v)
        if chunked: self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        tail = bytearray()
        try:
            while True:
                chunk = r.read1(65536) if self.command != "HEAD" else b""
                if not chunk: break
                if chunked: self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                else: self.wfile.write(chunk)
                self.wfile.flush()
                if track:
                    tail += chunk
                    if len(tail) > 4_000_000: del tail[:-262144]
            if chunked: self.wfile.write(b"0\r\n\r\n"); self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError): pass
        finally: up.close()
        if track:
            try:
                model, inp, out, eval_s = parse_usage(path, body, bytes(tail))
                log_row(ts=t0, model=model, endpoint=path, inp=inp, out=out, dur=time.time() - t0, eval_s=eval_s, status=r.status, source="proxy")
            except Exception as e: print("parse error:", e, file=sys.stderr)
    do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = do_OPTIONS = do_PATCH = handle_any

GIN = re.compile(r'\[GIN\] (\d{4}/\d\d/\d\d - \d\d:\d\d:\d\d) \| (\d+) \|\s+([\d.]+)(ns|µs|us|ms|s|m)?\S* \|\s+(\S+) \| (\w+)\s+"([^"]+)"')
UNIT = {"ns": 1e-9, "µs": 1e-6, "us": 1e-6, "ms": 1e-3, "s": 1, "m": 60, None: 1}
def backfill(before=None):
    out = subprocess.run(["journalctl", "-u", "ollama", "--no-pager", "-o", "cat"], capture_output=True, text=True, errors="replace").stdout
    n = 0
    for line in out.splitlines():
        m = GIN.search(line)
        if not m: continue
        ts_s, status, dur, unit, ip, meth, path = m.groups()
        if meth != "POST" or path.split("?")[0] not in TRACK: continue
        ts = datetime.strptime(ts_s, "%Y/%m/%d - %H:%M:%S").timestamp()
        if before and ts >= before: continue
        log_row(ts=ts, model=None, endpoint=path, inp=None, out=None, dur=float(dur) * UNIT.get(unit, 1), status=int(status), source="backfill", dedupe=line[-160:]); n += 1
    print("backfill rows seen:", n)

def fmt(n): return "{:,}".format(int(n or 0))
def money(x): return "${:,.4f}".format(x) if x < 1 else "${:,.2f}".format(x)
def ghost_digits(s):
    return "".join("8" if c.isdigit() else c for c in str(s))

RANGES = ("all", "today", "dod", "mtd", "weekly", "monthly", "yearly")
RANGE_META = {
    "all": ("ALL", "TOTAL TOKENS ANNIHILATED", "Lifetime · all recorded proxy traffic"),
    "today": ("TODAY", "TOKENS ANNIHILATED, TODAY", "Today (America/New_York)"),
    "dod": ("24H", "TOKENS ANNIHILATED, 24H", "Rolling · last 24 hours vs prior 24 hours"),
    "mtd": ("MTD", "TOKENS ANNIHILATED, MTD", "Month to date · 1st through today"),
    "weekly": ("7D", "TOKENS ANNIHILATED, 7D", "Rolling week · last 7 days"),
    "monthly": ("30D", "TOKENS ANNIHILATED, 30D", "Rolling month · last 30 days"),
    "yearly": ("YTD", "TOKENS ANNIHILATED, YTD", "Year to date · Jan 1 through today"),
}
RANGE_ALIASES = {"7d": "weekly", "30d": "monthly", "ytd": "yearly", "week": "weekly", "month": "monthly", "year": "yearly", "24h": "dod", "24hr": "dod", "last24": "dod"}

def _today_et():
    """Calendar date in America/New_York."""
    from datetime import datetime, timezone, timedelta
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).date()
    except Exception:
        return (datetime.now(timezone.utc) - timedelta(hours=5)).date()

def _range_dates(key):
    from datetime import timedelta
    today = _today_et()
    key = (key or "all").lower()
    key = RANGE_ALIASES.get(key, key)
    if key not in RANGES:
        key = "all"
    if key == "all":
        return None, None, None, None, key
    if key == "today":
        return today.isoformat(), today.isoformat(), None, None, key
    if key == "dod":
        # Rolling windows are computed in stats_payload via timestamps.
        return "rolling-24h", "now", "prior-24h", "rolling-24h", key
    if key == "mtd":
        return today.replace(day=1).isoformat(), today.isoformat(), None, None, key
    if key == "weekly":
        return (today - timedelta(days=6)).isoformat(), today.isoformat(), None, None, key
    if key == "monthly":
        return (today - timedelta(days=29)).isoformat(), today.isoformat(), None, None, key
    return today.replace(month=1, day=1).isoformat(), today.isoformat(), None, None, key

def _sum_range(c, start, end):
    P = "source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local')"
    if start is None:
        tin, tout, nreq = c.execute(
            f"SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),SUM(COALESCE(request_count,1)) FROM requests WHERE {P}"
        ).fetchone()
        tps = c.execute(
            f"SELECT SUM(output_tokens)/SUM(eval_s) FROM requests WHERE {P} AND eval_s>0 AND output_tokens>0"
        ).fetchone()[0]
    else:
        tin, tout, nreq = c.execute(
            f"SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),SUM(COALESCE(request_count,1)) FROM requests WHERE {P} AND date(ts,'unixepoch','localtime') BETWEEN ? AND ?",
            (start, end),
        ).fetchone()
        tps = c.execute(
            f"SELECT SUM(output_tokens)/SUM(eval_s) FROM requests WHERE {P} AND eval_s>0 AND output_tokens>0 AND date(ts,'unixepoch','localtime') BETWEEN ? AND ?",
            (start, end),
        ).fetchone()[0]
    return int(tin or 0), int(tout or 0), int(nreq or 0), tps

def _sum_range_ts(c, start_ts, end_ts):
    """Sum proxy/hermes tokens in a half-open unix-timestamp window [start_ts, end_ts)."""
    P = "source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local')"
    tin, tout, nreq = c.execute(
        f"SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),SUM(COALESCE(request_count,1)) FROM requests WHERE {P} AND ts>=? AND ts<?",
        (float(start_ts), float(end_ts)),
    ).fetchone()
    tps = c.execute(
        f"SELECT SUM(output_tokens)/SUM(eval_s) FROM requests WHERE {P} AND eval_s>0 AND output_tokens>0 AND ts>=? AND ts<?",
        (float(start_ts), float(end_ts)),
    ).fetchone()[0]
    return int(tin or 0), int(tout or 0), int(nreq or 0), tps

def _codex_tokens_window(start_ts, end_ts):
    """Sum uncached Codex token deltas from rollout events in [start_ts, end_ts)."""
    total = 0
    cutoff = float(start_ts) - 86400
    try:
        for root, _d, files in os.walk(CODEX_SESSIONS):
            for name in files:
                if not (name.startswith("rollout-") and name.endswith(".jsonl")):
                    continue
                p = os.path.join(root, name)
                try:
                    if os.path.getmtime(p) < cutoff:
                        continue
                    prev = None
                    with open(p, errors="ignore") as f:
                        for line in f:
                            if "total_token_usage" not in line:
                                continue
                            try:
                                o = json.loads(line)
                                tu = ((o.get("payload") or {}).get("info") or {}).get("total_token_usage") or {}
                                cur = (
                                    int(tu.get("input_tokens") or 0),
                                    int(tu.get("cached_input_tokens") or 0),
                                    int(tu.get("output_tokens") or 0),
                                )
                                ets = datetime.fromisoformat(o["timestamp"].replace("Z", "+00:00")).timestamp()
                            except Exception:
                                continue
                            dlt = cur if (prev is None or cur[0] < prev[0]) else tuple(a - b for a, b in zip(cur, prev))
                            prev = cur
                            if start_ts <= ets < end_ts:
                                total += max(0, dlt[0] - dlt[1]) + dlt[2]
                except Exception:
                    continue
    except Exception:
        pass
    return int(total)

def cloud_tokens_range(range_key="all", window=None):
    """Cloud tokens by serving model: Codex (uncached in + out) + OpenCode DeepSeek (in + out + reasoning, cache reads excluded).
    Grok is excluded: it only reports a usage percentage, not tokens."""
    if window is not None:
        s_ts, e_ts = window
        codex = _codex_tokens_window(s_ts, e_ts)
        ds = _opencode_window_tokens(int(s_ts * 1000), int(e_ts * 1000), "deepseek")
        return codex + ds, codex, ds
    key, s_ms, e_ms = _opencode_range_bounds_ms(range_key)
    if s_ms is None:
        try:
            codex = int(_codex_session_token_totals()[2] or 0)
        except Exception:
            codex = 0
    else:
        codex = _codex_tokens_window(s_ms / 1000.0, e_ms / 1000.0)
    ds = int(_opencode_agg(key, bucket="deepseek").get("tot") or 0)
    return codex + ds, codex, ds


def _opencode_window_tokens(s_ms, e_ms, bucket):
    import sqlite3
    try:
        con = sqlite3.connect("file:%s?mode=ro" % OPENCODE_DB, uri=True)
        try:
            rows = con.execute("SELECT json_extract(data,'$.model'),COALESCE(json_extract(data,'$.tokens.input'),0)+COALESCE(json_extract(data,'$.tokens.output'),0)+COALESCE(json_extract(data,'$.tokens.reasoning'),0) FROM session_message WHERE type='assistant' AND time_created>=? AND time_created<?", (s_ms, e_ms)).fetchall()
        finally:
            con.close()
    except Exception:
        return 0
    t = 0
    for raw, n in rows:
        mid, pid = _opencode_model_parts(raw)
        if _opencode_bucket(mid, pid) == bucket:
            t += int(n or 0)
    return t


def stats_payload(range_key="all", cloud=False):
    start, end, ps, pe, key = _range_dates(range_key)
    short, hero_lbl, base_sub = RANGE_META[key]
    c = db()
    try:
        delta = None
        if key == "dod":
            now_ts = time.time()
            cur_start, cur_end = now_ts - 86400, now_ts
            prev_start, prev_end = now_ts - 2 * 86400, now_ts - 86400
            tin, tout, nreq, tps = _sum_range_ts(c, cur_start, cur_end)
            ptin, ptout, pnreq, _ = _sum_range_ts(c, prev_start, prev_end)
            ptot = ptin + ptout
            tot = tin + tout
            abs_d = tot - ptot
            pct = (100.0 * abs_d / ptot) if ptot else (100.0 if tot else 0.0)
            delta = {
                "prev_tot": ptot, "abs": abs_d, "pct": round(pct, 1),
                "dir": "up" if abs_d > 0 else ("down" if abs_d < 0 else "flat"),
                "label": "vs prior 24H",
            }
            start = datetime.fromtimestamp(cur_start).strftime("%Y-%m-%d %H:%M")
            end = datetime.fromtimestamp(cur_end).strftime("%Y-%m-%d %H:%M")
            subtitle = "Last 24 hours · %s – %s ET vs prior 24H" % (start, end)
        else:
            tin, tout, nreq, tps = _sum_range(c, start, end)
            if key == "today":
                subtitle = "Today (America/New_York) · %s" % start
            elif key == "all":
                subtitle = "Lifetime"
            else:
                subtitle = "%s · %s – %s" % (base_sub.split("·")[0].strip(), start, end)
    finally:
        c.close()
    tot = tin + tout
    local_tot = tot
    cloud_tot = codex_t = ds_t = None
    if cloud:
        if key == "dod":
            cloud_tot, codex_t, ds_t = cloud_tokens_range(window=(cur_start, cur_end))
            pc, _, _ = cloud_tokens_range(window=(prev_start, prev_end))
            ptot2 = ptot + pc
            tot2 = tot + cloud_tot
            abs_d = tot2 - ptot2
            pct = (100.0 * abs_d / ptot2) if ptot2 else (100.0 if tot2 else 0.0)
            delta = {"prev_tot": ptot2, "abs": abs_d, "pct": round(pct, 1),
                     "dir": "up" if abs_d > 0 else ("down" if abs_d < 0 else "flat"),
                     "label": "vs prior 24H"}
        else:
            cloud_tot, codex_t, ds_t = cloud_tokens_range(key)
        tot = tot + cloud_tot
        hero_lbl = hero_lbl + " + CLOUD"
    tps_v = float(tps) if tps else 0.0
    return {
        "cloud": bool(cloud), "local_tot": local_tot, "cloud_tot": cloud_tot,
        "cloud_codex": codex_t, "cloud_deepseek": ds_t,
        "range": key, "short": short, "hero_label": hero_lbl, "subtitle": subtitle,
        "start": start, "end": end,
        "tin": tin, "tout": tout, "tot": tot, "nreq": nreq,
        "tps": tps_v, "tps_disp": (("%.1f" % tps_v) if tps else "-"),
        "delta": delta,
        "tot_fmt": fmt(tot),
        "ghost_tot": ghost_digits(fmt(tot)),
    }

SPEED_LIVE_WIN = 600  # seconds: semi-live tach window
def speed_payload(range_key="all"):
    """Tach: average output tok/s (SUM out / SUM eval_s) over the selected period, plus all-time single-request peak."""
    rk, lo_ms, hi_ms = _opencode_range_bounds_ms(range_key)
    P = "source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local')"
    c = db()
    try:
        now = time.time()
        lo = (lo_ms / 1000.0) if lo_ms is not None else 0.0
        hi = (hi_ms / 1000.0) if hi_ms is not None else now + 1
        r = c.execute(f"SELECT SUM(output_tokens), SUM(eval_s), COUNT(*) FROM requests WHERE {P} AND eval_s>0 AND output_tokens>0 AND ts>=? AND ts<?", (lo, hi)).fetchone()
        pk = c.execute(f"SELECT CAST(output_tokens AS REAL)/eval_s AS t, ts, model FROM requests WHERE {P} AND eval_s>=2 AND output_tokens>=64 ORDER BY t DESC LIMIT 1").fetchone()
    finally:
        c.close()
    live = (float(r[0]) / float(r[1])) if (r and r[1]) else 0.0
    peak = float(pk[0]) if pk else 0.0
    scale = max(50, int(math.ceil(max(peak, live) * 1.05 / 20.0)) * 20)
    step = 20 if scale <= 160 else 40
    ticks = list(range(0, scale + 1, step))
    if pk:
        mdl = str(pk[2] or "?").split(":")[0]
        when = datetime.fromtimestamp(pk[1]).strftime("%b %-d %-I:%M %p")
        label = "PEAK %.1f · %s · %s ET" % (peak, mdl, when)
    else:
        label = "PEAK -"
    return {"live": live, "live_disp": ("%.1f" % live) if live else "0.0", "live_reqs": int(r[2] or 0) if r else 0,
            "range": rk, "range_label": RANGE_META.get(rk, ("ALL",))[0], "peak": peak, "peak_label": label, "scale": scale, "ticks": ticks}

def range_tabs_html(active="all", include_mobile=False, include_cloud=False):
    btns = []
    for key in RANGES:
        short, _, full = RANGE_META[key]
        on = " on" if key == active else ""
        title = html.escape(full, quote=True)
        btns.append(
            '<button type="button" class="rtab%s" data-range="%s" role="tab" aria-selected="%s" title="%s">%s</button>'
            % (on, key, "true" if key == active else "false", title, short)
        )
    if include_cloud:
        btns.append('<button type="button" class="rtab ctog" id="cloud-tog" aria-pressed="false" title="Add cloud tokens (Codex + DeepSeek) to the local total">+ CLOUD</button>')
    if include_mobile:
        btns.append('<a class="rtab rtab-link" href="/m" title="Open mobile view">MOBILE</a>')
    return "".join(btns)

def hof_card_html(rank_lbl, name, tokens, nreq, pct):
    return (
        "<div class='card hof'><div class=lbl>%s</div><div class=mname>%s</div>"
        "<div class='big dseg seg' data-val='%s' data-ghost='%s'>%s</div>"
        "<div class=muted>%s requests · %.0f%% of total</div></div>"
        % (html.escape(rank_lbl), html.escape(name), fmt(tokens), ghost_digits(fmt(tokens)),
           fmt(tokens), fmt(nreq), pct)
    )


def calendar(c):
    from datetime import date, timedelta
    rows = c.execute("SELECT date(ts,'unixepoch','localtime'), SUM(COALESCE(input_tokens,0)+COALESCE(output_tokens,0)) FROM requests WHERE source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local') GROUP BY 1").fetchall()
    per = {d: t or 0 for d, t in rows}
    today = date.today(); end = today + timedelta(days=(5 - today.weekday()) % 7)
    start = end - timedelta(days=52 * 7 - 1 + 7); start -= timedelta(days=(start.weekday() + 1) % 7)
    mx = max(per.values() or [0]) or 0
    shades = ["#0b120e", "#1faa2e", "#7CFF3A", "#FFCC00", "#FF4A1A"]
    empty_stroke = "#3d5240"
    cell, gap, ox, oy = 9, 2, 28, 16
    step = cell + gap
    cells, months, d, col, lastm, last_mcol = [], [], start, 0, None, -99
    while d <= end:
        r = (d.weekday() + 1) % 7
        if r == 0 and d.month != lastm:
            if col - last_mcol >= 3:
                months.append("<text x='%d' y='11' class='ct ct-month'>%s</text>" % (ox + col * step, d.strftime("%b")))
                last_mcol = col
            lastm = d.month
        if d <= today:
            t = per.get(d.isoformat(), 0)
            lvl = 0 if not t or not mx else min(4, 1 + int(4 * t / mx - 1e-9))
            x, y = ox + col * step, oy + r * step
            tip = html.escape("%s: %s tokens annihilated" % (d.strftime("%a %b %d, %Y"), fmt(t)), quote=True)
            if lvl == 0:
                cells.append("<rect class='day empty' x='%d' y='%d' width='%d' height='%d' rx='1.5' fill='%s' stroke='%s' stroke-width='1' data-t='%s'/>" % (x, y, cell, cell, shades[0], empty_stroke, tip))
            else:
                cells.append("<rect class='day lit' x='%d' y='%d' width='%d' height='%d' rx='1.5' fill='%s' data-t='%s'/>" % (x, y, cell, cell, shades[lvl], tip))
        if r == 6: col += 1
        d += timedelta(days=1)
    labels = "".join("<text x='0' y='%d' class='ct ct-dow'>%s</text>" % (oy + r * step + cell - 1, n) for r, n in ((1, "Mon"), (3, "Wed"), (5, "Fri")))
    legend = "".join("<span class='sw %s' style='background:%s;border-color:%s'></span>" % (("empty" if i == 0 else "lit"), s, empty_stroke if i == 0 else s) for i, s in enumerate(shades))
    vb_w = ox + col * step + 4
    vb_h = oy + 7 * step + 2
    return (
        "<div class=cal><svg viewBox='0 0 %d %d' width='100%%' preserveAspectRatio='xMidYMid meet'>%s%s%s</svg>"
        "<div class=legend><span class=leg-lbl>Mild</span> %s <span class=leg-lbl>Apocalyptic</span></div><div id=tip></div></div>"
    ) % (vb_w, vb_h, "".join(months), labels, "".join(cells), legend)


def devastation_day_rows(q, where_clause, n=5):
    """Exactly n calendar days ending today (ET): today + previous n-1. Missing days pad to 0."""
    from datetime import timedelta
    rows = q(
        f"SELECT date(ts,'unixepoch','localtime') d, "
        f"SUM(COALESCE(input_tokens,0)), SUM(COALESCE(output_tokens,0)) "
        f"FROM requests WHERE {where_clause} GROUP BY d ORDER BY d DESC LIMIT ?",
        n + 14,
    )
    by_day = {d: (int(a or 0), int(b or 0)) for d, a, b in rows}
    today = _today_et()
    out = []
    for i in range(n):
        d = (today - timedelta(days=i)).isoformat()
        a, b = by_day.get(d, (0, 0))
        out.append((d, a, b))
    return out

def months_join(m):
    return "".join(m)

def page():
    c = db(); q = lambda s, *a: c.execute(s, a).fetchall()
    P = "source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local')"
    tin, tout, nreq = q(f"SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),SUM(COALESCE(request_count,1)) FROM requests WHERE {P}")[0]
    tps = q(f"SELECT SUM(output_tokens)/SUM(eval_s) FROM requests WHERE {P} AND eval_s>0 AND output_tokens>0")[0][0]
    days = devastation_day_rows(q, P, 5)
    models = q(f"SELECT COALESCE(model,'?'), SUM(COALESCE(request_count,1)), SUM(COALESCE(input_tokens,0)+COALESCE(output_tokens,0)) t FROM requests WHERE {P} GROUP BY 1 ORDER BY t DESC LIMIT 40")
    cal = calendar(c); c.close()
    try: prices = json.load(open(PRICES))["models"]
    except Exception: prices = []
    mx_day = max([a + b for _, a, b in days] or [1]) or 1

    def seg_tach(n, frac, rising=True):
        frac = max(0.0, min(1.0, float(frac)))
        lit = int(round(frac * n))
        warn_from, red_from = int(0.70 * n), int(0.85 * n)
        parts = []
        for i in range(n):
            h = (28 + int(72 * (i / max(n - 1, 1)))) if rising else 70
            if i < lit:
                cls = "seg-on redline" if i >= red_from else ("seg-on warn" if i >= warn_from else "seg-on")
            else:
                cls = "seg-off"
            parts.append("<i class='%s' style='height:%d%%'></i>" % (cls, h))
        return "".join(parts)

    dayrows = []
    for d, a, b in days:
        tot_d = a + b
        frac = tot_d / mx_day if mx_day else 0
        dayrows.append(
            "<tr><td class=day-col>%s</td><td class=gauge-col><div class='tach-track day-tach'>%s</div></td>"
            "<td class='seg day-io'>%s</td><td class='seg day-io'>%s</td></tr>"
            % (d, seg_tach(20, frac, False), fmt(a), fmt(b))
        )
    dayrows = "".join(dayrows) or (
        "<tr><td colspan=4 class=muted>The silence before the storm. Point a client at port %d and let the carnage begin.</td></tr>" % PROXY_PORT
    )
    _tot = sum(t or 0 for _, _, t in models) or 1
    # OpenCode → Spark local usage as a Hall of Fame entry (same card style).
    # Tokens already flow through the proxy; do not inflate _tot.
    # Attribution by serving model: OpenCode's local Flash calls already sit in the proxy model rows.
    _medal = {0: "#1 legend", 1: "#2 champion", 2: "#3 master"}
    top3 = models[:3]
    rest = models[3:]
    modrows = "".join(
        hof_card_html(_medal.get(i, "#%d" % (i + 1)), m, t or 0, n, 100 * (t or 0) / _tot)
        for i, (m, n, t) in enumerate(top3)
    ) or "<div class=card><div class=muted>No champions yet. The arena awaits.</div></div>"
    if rest:
        m0, n0, t0 = rest[0]
        opts = []
        for i, (m, n, t) in enumerate(rest):
            rank = i + 4
            opts.append(
                '<option value="%d"%s>#%d %s</option>'
                % (rank, " selected" if i == 0 else "", rank, html.escape(m))
            )
        hof_extras_json = html.escape(json.dumps([
            {"rank": i + 4, "name": m, "tokens": int(t or 0), "nreq": int(n or 0),
             "pct": round(100 * (t or 0) / _tot, 0),
             "tokens_fmt": fmt(t), "nreq_fmt": fmt(n), "ghost": ghost_digits(fmt(t))}
            for i, (m, n, t) in enumerate(rest)
        ]), quote=True)
        slot4_html = (
            "<div class='card hof hof-pick' id='hof-slot4' data-hof-extras='%s'>"
            "<div class='hof-pick-bar'><span class=lbl id=hof4-lbl>#4+ contenders</span>"
            "<select id=hof-pick-sel class=hof-sel aria-label='Choose Hall of Fame model'>%s</select></div>"
            "<div class=mname id=hof4-name>%s</div>"
            "<div class='big dseg seg' id=hof4-tok data-val='%s' data-ghost='%s'>%s</div>"
            "<div class=muted id=hof4-meta>%s requests · %.0f%% of total</div></div>"
        ) % (hof_extras_json, "".join(opts), html.escape(m0),
             fmt(t0), ghost_digits(fmt(t0)), fmt(t0),
             fmt(n0), 100 * (t0 or 0) / _tot)
        modrows = modrows + slot4_html
    sav = ""
    for i, p in enumerate(prices):
        if p.get("input_per_mtok") is None or p.get("output_per_mtok") is None:
            sav += (
                "<div class='card bill'><div class=bill-row>"
                "<div class='big vfd seg'>n/a</div>"
                "<div class=bill-meta><div class=lbl title='%s'>%s</div>"
                "<div class=muted>price unverified</div></div></div></div>"
                % (html.escape(p["label"], quote=True), html.escape(p["label"]))
            )
            continue
        v = tin / 1e6 * p["input_per_mtok"] + tout / 1e6 * p["output_per_mtok"]
        mv = money(v)
        mv_num = mv[1:] if mv.startswith("$") else mv
        sav += (
            "<div class='card bill'><div class=bill-row>"
            "<div class='bill-amt'><span class='bill-cur'>$</span>"
            "<span class='big dseg seg' data-val='%s' data-ghost='%s'>%s</span></div>"
            "<div class=bill-meta><div class=lbl title='%s'>Kept from %s</div>"
            "<div class=muted>$%s/$%s per M in/out</div></div></div></div>"
            % (mv_num, ghost_digits(mv_num), mv_num, html.escape(p["label"], quote=True), html.escape(p["label"]),
               p["input_per_mtok"], p["output_per_mtok"])
        )

    SP = speed_payload()
    tps_v = SP["live"]
    tps_max = float(SP["scale"])
    tot = tin + tout
    tps_disp = SP["live_disp"]
    tach = seg_tach(36, tps_v / tps_max, True)
    tach_scale = "".join("<span>%d</span>" % v for v in SP["ticks"])
    peak_label = html.escape(SP["peak_label"])
    peak_short = "%.1f" % SP["peak"]
    peak_mark = "<b class=peak-mark id=peak-mark style='left:%.2f%%' title='%s'></b>" % (100.0 * min(1.0, SP["peak"] / tps_max), peak_label)
    lights = [("IGN", True, "green"), ("PROXY", True, "green"), (str(PROXY_PORT), True, "green"),
              ("LOCAL ONLY", True, "green")]
    lights.extend(spark_temp_lights())
    lights_html = "".join('<span class="wl %s%s">%s</span>' % (c, " on" if on else "", lab) for lab, on, c in lights)

    range_tabs = range_tabs_html("all", include_mobile=False, include_cloud=True)
    T = "DGX Token Annihilator Dashboard"
    VFD_B64 = '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'

    return f"""<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><meta http-equiv=refresh content=60><title>{T}</title><style>
@font-face{{font-family:"VFDPixel";src:url(data:font/woff2;base64,{VFD_B64}) format("woff2");font-weight:400;font-style:normal;font-display:swap;}}@font-face{{font-family:"DSEG7";src:url("/font/dseg7.woff2") format("woff2"),url("/font/dseg7.ttf") format("truetype");font-weight:700;font-style:italic;font-display:swap;}}
:root{{
  --bg:#030505;--panel:#0a100c;--green:#39ff14;--green-bright:#c8ff9a;--green-mid:#6dff4a;
  --line:#2a6b2a;--line-strong:#3a9a3a;--unlit:#152218;--unlit-edge:#2a3e2a;
  --warn:#ffcc00;--redline:#ff3d1a;--muted:#a8d898;--label:#f0ffe8;--cyan:#7ef0ff;
  --vfd:"VFDPixel",ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;--dseg:"DSEG7","VFDPixel",ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{height:100%;width:100%;overflow:hidden;background:var(--bg);color:var(--green);}}
body{{
  font-family:ui-monospace,"Cascadia Mono","SF Mono",Menlo,Consolas,monospace;
  width:100vw;height:100vh;padding:clamp(5px,.55vh,10px) clamp(8px,.85vw,14px);
  display:flex;flex-direction:column;
}}
body::before{{content:"";pointer-events:none;position:fixed;inset:0;z-index:9999;
  background:repeating-linear-gradient(0deg,rgba(0,0,0,.08) 0,rgba(0,0,0,.08) 1px,transparent 1px,transparent 3px);opacity:.16;}}
.cluster{{
  position:relative;
  --cluster-gap:clamp(5px,.5vh,8px);
  --cluster-pad-y:clamp(8px,.7vh,12px);
  --cluster-pad-x:clamp(10px,.85vw,14px);
  flex:1;min-height:0;width:100%;overflow:hidden;
  display:grid;gap:var(--cluster-gap);
  grid-template-rows:auto auto auto minmax(0,1fr) minmax(0,.9fr) minmax(0,1fr) auto minmax(158px,1.3fr) auto;
  border:3px solid var(--line-strong);border-radius:10px;
  padding:var(--cluster-pad-y) var(--cluster-pad-x);
  background:linear-gradient(180deg,#07100a,#030505);
  box-shadow:inset 0 0 24px #39ff1412,0 0 12px #39ff1420;
}}
@media (min-height:1000px){{
  .cluster{{grid-template-rows:auto auto auto minmax(0,1fr) minmax(0,.9fr) minmax(0,1.1fr) auto minmax(158px,1.2fr) auto;}}
}}

.hdr{{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;}}
h1{{margin:0;color:var(--green-bright);letter-spacing:.08em;text-transform:uppercase;
  font-size:clamp(14px,1.55vw,20px);font-weight:800;
  text-shadow:0 0 5px rgba(57,255,20,.35),0 0 1px rgba(200,255,154,.55);}}
.wl-row{{display:flex;flex-wrap:wrap;gap:6px;justify-content:flex-end;}}
.hdr-right{{display:flex;flex-direction:column;align-items:flex-end;gap:2px;flex-shrink:0;max-width:58%;}}
.range-tog{{display:inline-flex;flex-wrap:nowrap;gap:1px;align-items:stretch;padding:1px;
  border:1px solid var(--line-strong);border-radius:4px;background:#040806;}}
.rtab{{appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;
  font:700 8px/1 ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;letter-spacing:.04em;
  text-transform:uppercase;padding:2px 5px;border-radius:2px;text-align:center;white-space:nowrap;
  text-decoration:none;display:inline-flex;align-items:center;justify-content:center;}}
.rtab:hover{{color:var(--green-mid);border-color:var(--line);}}
.rtab.on{{color:#041204;background:var(--green);border-color:var(--green-bright);
  box-shadow:0 0 5px rgba(57,255,20,.45);}}
.rtab.ctog{{color:#5a1a1a;background:#120606;border-color:#3a1212;box-shadow:none;text-shadow:none;}}
.rtab.ctog:hover{{color:#a33;border-color:#5a1a1a;}}
.rtab.ctog.on{{color:#fff4f0;background:#ff1a1a;border-color:#ff6a5a;text-shadow:0 0 4px rgba(255,220,210,.9);
  box-shadow:0 0 6px rgba(255,30,30,.9),0 0 14px rgba(255,30,30,.55),inset 0 0 4px rgba(255,255,255,.35);}}
.rtab.rtab-link{{color:var(--muted);}}
.rtab.rtab-link:hover{{color:var(--green-bright);}}
.dash-toggle{{
  position:absolute;right:10px;bottom:8px;z-index:20;
  appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;
  font:700 8px/1 ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;letter-spacing:.04em;
  text-transform:uppercase;padding:2px 5px;border-radius:2px;text-align:center;white-space:nowrap;
  text-decoration:none;display:inline-flex;align-items:center;justify-content:center;
}}
.dash-toggle:hover{{color:var(--green-mid);border-color:var(--line);}}
.view-tog,.mode-tog{{
  display:inline-flex;flex-wrap:nowrap;gap:0;align-items:stretch;padding:0;
  border:2px solid #5a3a00;border-radius:4px;background:#0a0a0a;overflow:hidden;
}}
.view-tog{{position:absolute;top:8px;left:10px;z-index:50;}}
.mode-tog{{position:absolute;right:0;top:50%;transform:translateY(-50%);z-index:20;}}
.view-tog .vtab,.mode-tog .mtab{{
  appearance:none;cursor:pointer;border:0;border-right:1px solid #5a3a00;background:#0a0a0a;color:#9a7a30;
  font:700 11px/1 ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;letter-spacing:.04em;
  text-transform:uppercase;padding:4px 9px;text-align:center;white-space:nowrap;
  text-decoration:none;display:inline-flex;align-items:center;justify-content:center;
}}
.view-tog .vtab:last-child,.mode-tog .mtab:last-child{{border-right:0;}}
.view-tog .vtab:hover,.mode-tog .mtab:hover{{color:#c4a04a;background:#120e04;}}
.view-tog .vtab.on,.mode-tog .mtab.on{{color:#1a1000;background:var(--warn);box-shadow:inset 0 0 5px rgba(255,255,255,.12);}}

.hero .odo-lbl,.hero .odo{{white-space:nowrap;}}
.range-delta{{display:none;}}
.range-delta.show{{display:block;text-align:center;font-size:11px;font-weight:800;letter-spacing:.03em;
  color:var(--green-mid);line-height:1.2;margin:0;padding:0;max-height:1.2em;overflow:hidden;
  white-space:nowrap;text-overflow:ellipsis;width:100%;}}
.range-delta.show.up{{color:var(--green-bright);}}
.range-delta.show.down{{color:var(--redline);}}
.hof-pick-bar{{display:flex;align-items:center;justify-content:space-between;gap:4px;margin:0;flex-shrink:0;min-height:0;}}
.hof-pick-bar .lbl{{margin:0;flex:0 1 auto;min-width:0;}}
.hof-sel{{appearance:auto;max-width:58%;min-width:0;background:#0a120c;color:var(--green-mid);
  border:1px solid var(--line);border-radius:3px;font:700 9px/1.1 ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;
  padding:1px 3px;cursor:pointer;height:auto;}}
.hof-pick .mname{{margin-top:clamp(1px,.15vh,4px);}}
.hof-sel:focus{{outline:1px solid var(--warn);}}

.wl{{font-size:clamp(11px,.85vw,13px);letter-spacing:.08em;padding:3px 9px;border:2px solid #333;color:#777;
  background:#0a0a0a;border-radius:3px;text-transform:uppercase;font-weight:700;}}
.wl.green.on{{color:#041204;background:var(--green);border-color:var(--green);
  box-shadow:0 0 6px rgba(57,255,20,.5),inset 0 0 5px rgba(255,255,255,.15);}}
.wl.amber{{border-color:#5a3a00;color:#9a7a30;}}
.wl.amber.on{{color:#1a1000;background:var(--warn);border-color:var(--warn);box-shadow:0 0 6px rgba(255,204,0,.5),inset 0 0 5px rgba(255,255,255,.12);}}
.wl.red{{border-color:#5a1000;color:#9a4030;}}
.wl.red.on{{color:#1a0500;background:var(--redline);border-color:var(--redline);box-shadow:0 0 6px rgba(255,61,26,.5),inset 0 0 5px rgba(255,255,255,.12);}}
.tagline{{color:var(--muted);font-size:clamp(11px,.9vw,13px);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.hero{{
  position:relative;
  display:flex;flex-direction:column;align-items:center;justify-content:flex-start;gap:clamp(4px,.4vh,8px);
  padding:clamp(10px,1vh,16px) clamp(8px,.8vh,14px) clamp(6px,.6vh,12px);
  border:3px solid var(--line-strong);border-radius:10px;background:#050a07;
  box-shadow:inset 0 0 18px #39ff1418;overflow:hidden;flex-shrink:0;
}}
.hero .range-tog{{
  position:absolute;top:8px;right:10px;z-index:50;pointer-events:auto;
  flex-wrap:wrap;justify-content:flex-end;
  max-width:min(52vw,calc(100% - 150px));
}}
.hero .range-tog .rtab{{
  position:relative;z-index:1;pointer-events:auto;
  font-size:10px;padding:2px 6px;border-radius:3px;
}}
/* hof uses .mode-tog */

.hero .odo-lbl,.hero .odo-unit{{font-size:clamp(12px,1.05vw,16px);color:var(--label);letter-spacing:.18em;font-weight:800;
  margin:0 0 clamp(2px,.2vh,4px);padding:0;line-height:1.2;position:relative;z-index:2;
  background:transparent;flex-shrink:0;pointer-events:none;}}
.hero .odo{{
  position:relative;display:inline-block;isolation:isolate;overflow:hidden;flex-shrink:0;pointer-events:none;
  font-family:var(--dseg);font-size:clamp(28px,4.2vw,64px);font-weight:700;font-style:italic;color:var(--green-bright);
  letter-spacing:.06em;font-variant-numeric:tabular-nums;line-height:1.45;z-index:1;
  padding:.28em .2em .32em;margin:0;
  text-shadow:
    0 0 1px rgba(230,255,210,.95),
    0 0 3px rgba(57,255,20,.9),
    0 0 7px rgba(57,255,20,.6),
    0 0 12px rgba(57,255,20,.35),
    0 0 18px rgba(57,255,20,.18);
}}
.dseg{{
  font-family:var(--dseg);font-weight:700;font-style:italic;position:relative;z-index:1;
  text-shadow:
    0 0 1px rgba(230,255,210,.9),
    0 0 3px rgba(57,255,20,.8),
    0 0 7px rgba(57,255,20,.45),
    0 0 12px rgba(57,255,20,.25),
    0 0 18px rgba(57,255,20,.12);
}}
.odo::after,.big.dseg::after{{
  content:attr(data-val);position:absolute;left:0;top:0;right:0;bottom:0;
  color:rgba(57,255,20,.5);pointer-events:none;z-index:-1;
  filter:blur(.5px);opacity:.85;
  text-shadow:0 0 6px rgba(57,255,20,.4),0 0 10px rgba(57,255,20,.22);
  padding:inherit;box-sizing:border-box;
}}
.card.hof .big.dseg::after{{
  filter:blur(.25px);opacity:.75;
  text-shadow:0 0 2px rgba(57,255,20,.3),0 0 4px rgba(57,255,20,.15);
}}
/* Let HoF digit bloom breathe past tight card chrome without clipping ink */
.card.hof{{overflow:visible;}}
.panel:has(.hof-grid){{overflow:visible;}}
.odo::before,.big.dseg::before{{
  content:attr(data-ghost);position:absolute;left:0;top:0;right:0;bottom:0;
  color:rgba(40,90,50,.34);pointer-events:none;z-index:-2;
  filter:blur(.35px);text-shadow:none;opacity:.8;
  padding:inherit;box-sizing:border-box;
}}
.big.dseg{{
  display:inline-block;line-height:1.6;flex-shrink:0;
  padding:.3em .14em .36em;
  margin:clamp(4px,.35vh,8px) 0 clamp(4px,.35vh,8px);
}}
.vfd{{font-family:var(--vfd);font-weight:400;letter-spacing:.06em;
  text-shadow:0 0 6px rgba(57,255,20,.4),0 0 1px rgba(200,255,154,.6);}}
.panel{{
  min-height:0;overflow:hidden;display:flex;flex-direction:column;gap:clamp(4px,.4vh,7px);
  border:2px solid var(--line);border-radius:8px;padding:clamp(7px,.55vh,11px) clamp(9px,.65vw,12px);
  background:#050a07;
}}
.panel.year .cal{{min-height:0;}}
.panel > h2{{flex-shrink:0;}}
.panel > .grid,.panel > table,.panel > .cal{{flex:1;min-height:0;overflow:hidden;}}
.panel > .hof-grid{{overflow:visible;}}
h2{{font-size:clamp(13px,1.1vw,15px);text-transform:uppercase;letter-spacing:.12em;color:var(--label);
  display:flex;align-items:center;gap:8px;font-weight:800;margin:0;
  text-shadow:0 0 3px rgba(57,255,20,.28);}}
h2::before,h2::after{{content:"";flex:1;height:2px;background:linear-gradient(90deg,transparent,var(--line-strong),transparent);}}
.grid{{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));
  gap:clamp(6px,.5vw,10px);align-content:stretch;min-height:0;height:100%;}}
.card{{background:var(--panel);border:2px solid var(--line);border-radius:6px;
  padding:clamp(8px,.6vh,12px) clamp(9px,.65vw,12px);min-height:0;overflow:hidden;
  display:flex;flex-direction:column;justify-content:center;gap:clamp(2px,.2vh,4px);}}
.card.hof{{padding:clamp(4px,.35vh,10px) clamp(8px,.65vw,14px);gap:clamp(0px,.12vh,4px);}}
.card.hof .muted{{font-size:clamp(9px,.75vw,12px);flex-shrink:0;min-height:1.05em;margin-top:clamp(1px,.15vh,4px);line-height:1.2;}}
.card.speed{{grid-column:span 2;justify-content:flex-start;gap:clamp(4px,.35vh,7px);
  border-color:var(--line-strong);box-shadow:inset 0 0 14px #39ff1414;}}
.card.speed > .lbl{{margin:0 0 clamp(2px,.2vh,4px);align-self:flex-start;width:100%;flex-shrink:0;line-height:1.2;}}
.speed-row{{display:flex;align-items:center;gap:clamp(10px,.9vw,16px);width:100%;min-height:0;margin-top:auto;flex:1;}}
.speed-tach{{flex:0 1 78%;width:78%;max-width:80%;min-width:0;display:flex;flex-direction:column;gap:2px;}}
.speed-tach .tach-track{{width:100%;flex-shrink:0;}}
.speed-tach .tach-scale{{width:100%;}}
.speed-readout{{flex:0 0 auto;display:flex;flex-direction:column;align-items:flex-end;justify-content:center;
  gap:1px;}}
.speed-readout .big.dseg{{margin:0;padding:.08em .1em .1em;}}
.speed-unit{{font-size:clamp(9px,.65vw,11px);color:var(--muted);font-weight:800;letter-spacing:.08em;
  text-transform:uppercase;line-height:1;white-space:nowrap;}}
.panel.body .card{{padding:clamp(7px,.55vh,11px) clamp(9px,.6vw,12px);}}
.panel.body .big.dseg{{margin:clamp(5px,.35vh,8px) 0 clamp(2px,.2vh,5px);}}
.panel.body .lbl{{margin-bottom:clamp(5px,.35vh,8px);}}
.big{{font-family:var(--vfd);font-size:clamp(20px,2.3vw,30px);font-weight:400;color:var(--green-bright);
  letter-spacing:.06em;line-height:1.15;
  text-shadow:0 0 6px rgba(57,255,20,.4),0 0 1px rgba(200,255,154,.6);}}
.big.dseg{{font-family:var(--dseg);font-weight:700;font-style:italic;letter-spacing:.04em;}}
.card.hof .big{{font-size:clamp(12px,1.35vw,24px);}}
.card.hof .big.dseg{{padding:.32em .14em .42em;line-height:1.6;}}
.card.speed .big{{font-size:clamp(18px,2.6vw,42px);line-height:1.35;}}
.seg{{font-variant-numeric:tabular-nums;}}
.lbl{{color:var(--label);font-size:clamp(13px,1.15vw,16px);letter-spacing:.03em;margin:0 0 clamp(6px,.45vh,10px);font-weight:800;
  line-height:1.3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.muted{{color:var(--muted);font-size:clamp(11px,.9vw,13px);line-height:1.35;margin-top:clamp(6px,.45vh,10px);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.mname{{font-size:clamp(14px,1.25vw,18px);font-weight:800;color:var(--cyan);margin:0 0 clamp(6px,.5vh,10px);word-break:break-all;
  line-height:1.3;text-shadow:0 0 4px rgba(126,240,255,.3);}}
.card.hof .mname{{font-size:clamp(14px,1.3vw,19px);margin-bottom:clamp(4px,.35vh,10px);}}
.tach-track{{display:flex;align-items:flex-end;gap:2px;height:clamp(28px,5vh,52px);border-bottom:2px solid var(--line-strong);width:100%;}}
.card.speed .tach-track{{height:clamp(28px,6.5vh,64px);gap:3px;border-bottom-width:3px;flex-shrink:0;}}
.day-tach{{height:clamp(7px,1.05vh,11px);gap:1px;border-bottom-width:1px;align-items:stretch;}}
.day-tach i{{height:100% !important;border-radius:1px;min-width:1px;}}
.panel.daily{{gap:clamp(2px,.25vh,5px);padding:clamp(5px,.4vh,8px) clamp(8px,.6vw,11px);}}
.panel.daily table.day-log{{height:auto;}}
.panel.daily td{{padding:clamp(2px,.2vh,4px) clamp(4px,.4vw,7px);}}
.panel.daily td.day-col{{width:8%;padding-right:2px;white-space:nowrap;}} .panel.daily td.gauge-col{{width:67%;padding-left:2px;}} .panel.daily td.seg{{width:13%;font-size:clamp(12px,1.1vw,16px);}} .panel.daily td.seg:last-child{{width:12%;}}
.panel.daily td.day-io{{
  font-family:ui-monospace,"Cascadia Mono","SF Mono",Menlo,Consolas,monospace;
  font-weight:400;letter-spacing:0;text-shadow:none;color:var(--muted);
  font-size:clamp(12px,1.05vw,15px);line-height:1.25;text-align:right;
}}
.tach-track i{{display:block;flex:1;min-width:2px;border-radius:1px 1px 0 0;border:1px solid transparent;}}
.seg-on{{background:var(--green);border-color:#6dff4a;box-shadow:0 0 5px rgba(57,255,20,.45),inset 0 0 3px rgba(255,255,255,.18);}}
.seg-on.warn{{background:var(--warn);border-color:#ffe066;box-shadow:0 0 5px rgba(255,204,0,.45),inset 0 0 3px rgba(255,255,255,.18);}}
.seg-on.redline{{background:var(--redline);border-color:#ff8a6a;box-shadow:0 0 5px rgba(255,61,26,.5),inset 0 0 3px rgba(255,255,255,.18);}}
.seg-off{{background:var(--unlit);border-color:var(--unlit-edge);}}
.speed-track{{position:relative;}}.pk-short{{color:#ff8a6a;}}.speed-tach .speed-peak{{display:none;}}.peak-mark{{position:absolute;top:-4px;bottom:-4px;width:3px;margin-left:-1px;background:#ff3d1a;border-radius:1px;box-shadow:0 0 6px rgba(255,61,26,.9),0 0 12px rgba(255,61,26,.5);pointer-events:auto;}}.peak-mark::before{{content:'';position:absolute;top:-5px;left:-3px;border:4.5px solid transparent;border-top:5px solid #ff3d1a;}}.speed-peak{{font-size:10px;color:#ff8a6a;font-weight:800;letter-spacing:.06em;text-transform:uppercase;opacity:.9;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.tach-scale{{display:flex;justify-content:space-between;font-size:clamp(10px,.75vw,12px);color:var(--muted);font-weight:700;}}
table{{width:100%;border-collapse:collapse;font-size:clamp(12px,1.05vw,15px);table-layout:fixed;height:100%;}}
td{{padding:clamp(4px,.35vh,6px) clamp(5px,.45vw,8px);border-bottom:2px solid var(--line);
  color:var(--green-mid);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:middle;}}
td.day-col{{width:10%}} td.gauge-col{{width:65%}} td.seg{{width:13%;font-size:clamp(14px,1.3vw,18px);}}
.cal{{display:flex;flex-direction:column;gap:3px;min-height:0;overflow:hidden;flex:1;}}
.cal svg{{width:100%;flex:1;min-height:0;}}
.ct{{fill:var(--label);font-family:ui-monospace,monospace;font-weight:700;}}
.ct-month{{font-size:9px;}}.ct-dow{{font-size:8px;fill:var(--muted);}}
.day.lit{{filter:drop-shadow(0 0 1.5px rgba(255,200,80,.4));}}
rect.day:hover{{stroke:#e8ffe8;stroke-width:1.2}}
.legend{{display:flex;align-items:center;justify-content:flex-end;gap:5px;font-size:clamp(10px,.8vw,12px);color:var(--muted);font-weight:700;flex-shrink:0;min-height:14px;padding-top:2px;}}
.leg-lbl{{letter-spacing:.05em;text-transform:uppercase;}}
.sw{{display:inline-block;width:11px;height:11px;border-radius:2px;vertical-align:middle;border:1.5px solid transparent;flex-shrink:0;}}
.sw.empty{{border-color:#3d5240 !important;}}
#tip{{position:fixed;display:none;background:#000;border:2px solid var(--green);color:var(--green-bright);
  font-size:12px;padding:5px 9px;border-radius:4px;pointer-events:none;z-index:10000;}}
.foot{{text-align:center;color:var(--muted);font-size:clamp(11px,.85vw,13px);letter-spacing:.04em;flex-shrink:0;margin:0;}}.foot-bar{{position:relative;display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:calc(-1 * var(--cluster-gap));margin-bottom:calc(-1 * var(--cluster-pad-y));min-height:calc(clamp(26px,3vh,34px) + var(--cluster-gap) + var(--cluster-pad-y));padding:0 clamp(2px,.3vw,6px);box-sizing:border-box;z-index:5;}}.foot-bar .foot{{flex:1;min-width:0;padding:0 110px 0 0;box-sizing:border-box;}}
.bill-grid{{grid-template-columns:repeat(auto-fit,minmax(min(180px,100%),1fr));height:auto;align-content:stretch;}}
.card.bill{{justify-content:center;padding:clamp(5px,.4vh,9px) clamp(6px,.5vw,9px);overflow:visible;}}
.bill-row{{display:flex;align-items:center;gap:clamp(5px,.45vw,9px);min-width:0;width:100%;}}
.bill-amt{{display:inline-flex;align-items:baseline;gap:0;flex:0 0 auto;overflow:visible;}}
.bill-cur{{font-family:var(--dseg);font-weight:700;font-style:italic;color:var(--green-bright);
  font-size:clamp(13px,1.45vw,22px);letter-spacing:.02em;margin-right:.1em;
  text-shadow:0 0 3px rgba(57,255,20,.8);line-height:1.35;}}
.bill-row .big{{flex:0 0 auto;margin:0;font-size:clamp(13px,1.45vw,22px);}}
.bill-row .big.dseg,.bill-amt .big.dseg{{padding:.12em .1em .14em;margin:0;line-height:1.35;overflow:visible;}}
.bill-meta{{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;gap:1px;justify-content:center;}}
.bill-meta .lbl{{white-space:normal;overflow:hidden;text-overflow:ellipsis;margin:0;font-size:clamp(9px,.8vw,12px);line-height:1.2;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;}}
.bill-meta .muted{{white-space:normal;overflow:hidden;text-overflow:ellipsis;margin:0;font-size:clamp(8px,.7vw,11px);line-height:1.2;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;}}
.hof-grid{{grid-template-columns:repeat(4,minmax(0,1fr));height:auto;overflow:hidden;align-content:end;min-height:0;padding-bottom:0;box-sizing:border-box;}}
.panel:has(.hof-grid){{position:relative;overflow:hidden;min-height:0;}}
.card.hof{{overflow:hidden;min-height:0;}}
.foot{{position:relative;z-index:2;background:var(--bg);}}
.card.hof .mname{{margin-bottom:clamp(1px,.15vh,5px);flex-shrink:0;min-height:1.15em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;word-break:normal;}}
.card.hof .big.dseg{{padding:.12em .08em .14em;line-height:1.15;font-size:clamp(13px,1.25vw,22px);flex-shrink:1;min-height:0;}}
.card.hof .muted{{flex-shrink:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.card.hof .lbl{{flex-shrink:0;}}

.card.hof{{justify-content:flex-start;}}
.card.hof .lbl{{flex-shrink:0;margin-bottom:clamp(1px,.15vh,4px);}}
@media(max-width:900px){{
  .grid{{grid-template-columns:repeat(2,minmax(0,1fr));}}
  .card.speed{{grid-column:span 2;}}
}}
</style></head><body><div class=cluster>
<div class=hdr>
  <h1>{T}</h1>
  <div class=hdr-right>
    <div class=wl-row>{lights_html}</div>
  </div>
</div>
<div class=tagline>Every token counted by the proxy on port {PROXY_PORT} is a token the cloud will never bill you for. Last carnage report: {datetime.now():%b %d %H:%M}.</div>

<div class=hero>
  <div class="view-tog" aria-label="Desktop or mobile">
    <a class="vtab on" href="/" title="Desktop view">DESKTOP</a>
    <a class="vtab" href="/m" title="Mobile view">MOBILE</a>
  </div>
  <nav class="range-tog" role="tablist" aria-label="Time range">{range_tabs}</nav>
  <div class=odo-lbl id=hero-lbl>TOTAL TOKENS ANNIHILATED</div>
  <div class="odo dseg seg" id=hero-odo data-val="{fmt(tot)}" data-ghost="{ghost_digits(fmt(tot))}">{fmt(tot)}</div>
  <div class="range-delta" id="range-delta"></div>
</div>

<div class="panel body">
  <h2>Body count</h2>
  <div class=grid>
    <div class=card><div class=lbl>Tokens annihilated</div><div class="big dseg seg" data-val="{fmt(tot)}" data-ghost="{ghost_digits(fmt(tot))}">{fmt(tot)}</div></div>
    <div class=card><div class=lbl>Prompts devoured (input)</div><div class="big dseg seg" data-val="{fmt(tin)}" data-ghost="{ghost_digits(fmt(tin))}">{fmt(tin)}</div></div>
    <div class=card><div class=lbl>Words unleashed (output)</div><div class="big dseg seg" data-val="{fmt(tout)}" data-ghost="{ghost_digits(fmt(tout))}">{fmt(tout)}</div></div>
    <div class=card><div class=lbl>Requests vanquished</div><div class="big dseg seg" data-val="{fmt(nreq)}" data-ghost="{ghost_digits(fmt(nreq))}">{fmt(nreq)}</div></div>
    <div class="card speed">
      <div class=lbl id=speed-lbl>Annihilation speed (avg · ALL)</div>
      <div class=speed-row>
        <div class=speed-tach>
          <div class='tach-track speed-track' id=speed-tach>{tach}{peak_mark}</div>
          <div class=tach-scale id=speed-scale>{tach_scale}</div>
          <div class=speed-peak id=speed-peak>{peak_label}</div>
        </div>
        <div class=speed-readout>
          <div class="big dseg seg" id=speed-val data-val="{tps_disp}" data-ghost="{ghost_digits(tps_disp)}">{tps_disp}</div>
          <div class=speed-unit>tok/s · <span class=pk-short id=speed-pk title='{peak_label}'>peak {peak_short}</span></div>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="panel cloud">
  <h2>Cloud bills obliterated (at list price)</h2>
  <div class="grid bill-grid">{sav}</div>
</div>

<div class="panel year">
  <h2>Year of destruction</h2>
  {cal}
</div>

<div class="panel daily">
  <h2>Daily devastation log</h2>
  <table class=day-log><tr class=muted><td class=day-col>Day</td><td class=gauge-col></td><td class="seg day-io">In</td><td class="seg day-io">Out</td></tr>{dayrows}</table>
</div>

<div class=panel>
  <h2>Hall of fame: hungriest models</h2>
  <div class="grid hof-grid">{modrows}</div>
</div>

<div class="foot-bar">
  <p class=foot>No GPUs were harmed. Several cloud invoices were.</p>
  <div class="mode-tog" aria-label="Local or cloud">
    <a class="mtab on" href="/" title="Local annihilator">LOCAL</a>
    <a class="mtab" href="/cloud" title="Cloud usage">CLOUD</a>
  </div>
</div>
</div>
<script>var t=document.getElementById('tip');document.querySelectorAll('rect[data-t]').forEach(function(r){{r.onmousemove=function(e){{t.textContent=r.dataset.t;t.style.display='block';t.style.left=(e.clientX+12)+'px';t.style.top=(e.clientY+12)+'px'}};r.onmouseleave=function(){{t.style.display='none'}}}});</script>
<script>(function(){{
var KEY='tad-range2';
var HOFKEY='tad-hof-slot4';
var active=localStorage.getItem(KEY)||'all';
if(['all','today','dod','mtd','weekly','monthly','yearly'].indexOf(active)<0) active='all';
function $(id){{return document.getElementById(id);}}
function setDseg(el, text){{
  if(!el) return;
  el.textContent=text;
  el.setAttribute('data-val', text);
  el.setAttribute('data-ghost', String(text).replace(/\d/g,'8'));
}}
function fmt(n){{return String(Math.trunc(n||0)).replace(/\B(?=(\d{{3}})+(?!\d))/g,',');}}
function applyHero(d){{
  var lbl=$('hero-lbl'); if(lbl) lbl.textContent=d.hero_label;
  setDseg($('hero-odo'), d.tot_fmt);
  var delta=$('range-delta');
  if(delta){{
    if(d.delta){{
      var arrow=d.delta.dir==='up'?'\u25b2':(d.delta.dir==='down'?'\u25bc':'\u25cf');
      var sign=d.delta.abs>0?'+':'';
      delta.className='range-delta show '+(d.delta.dir||'flat');
      delta.textContent=arrow+' '+sign+fmt(d.delta.abs)+' ('+sign+d.delta.pct+'%) '+d.delta.label;
    }} else {{ delta.className='range-delta'; delta.textContent=''; }}
  }}
  document.querySelectorAll('.rtab[data-range]').forEach(function(b){{
    var on=b.getAttribute('data-range')===d.range;
    b.classList.toggle('on', on);
    b.setAttribute('aria-selected', on?'true':'false');
  }});
}}
function applySpeed(s){{
  var t=$('speed-tach'); if(!t||!s||s.error) return;
  var segs=t.querySelectorAll('i'); var n=segs.length;
  var lit=Math.round(Math.max(0,Math.min(1,s.live/s.scale))*n);
  var wf=Math.floor(0.70*n), rf=Math.floor(0.85*n);
  segs.forEach(function(el,i){{ el.className = i<lit ? (i>=rf?'seg-on redline':(i>=wf?'seg-on warn':'seg-on')) : 'seg-off'; }});
  setDseg($('speed-val'), s.live_disp);
  var pm=$('peak-mark'); if(pm){{ pm.style.left=(100*Math.min(1,s.peak/s.scale))+'%'; pm.title=s.peak_label; }}
  var pl=$('speed-peak'); if(pl) pl.textContent=s.peak_label;
  var lb=$('speed-lbl'); if(lb&&s.range_label) lb.textContent='Annihilation speed (avg · '+s.range_label+')';
  var pk=$('speed-pk'); if(pk){{ pk.textContent='peak '+s.peak.toFixed(1); pk.title=s.peak_label; }}
  var sc=$('speed-scale'); if(sc&&s.ticks) sc.innerHTML=s.ticks.map(function(v){{return '<span>'+v+'</span>';}}).join('');
}}
function pollSpeed(){{ fetch('/api/speed?range='+encodeURIComponent(curRange||'all'),{{cache:'no-store'}}).then(function(r){{return r.json();}}).then(applySpeed).catch(function(){{}}); }}
setInterval(pollSpeed, 30000);
var CKEY='tad-cloud';
var cloudOn=localStorage.getItem(CKEY)==='1';
var curRange=active;
function paintCloud(){{ var b=$('cloud-tog'); if(!b) return; b.classList.toggle('on', cloudOn); b.setAttribute('aria-pressed', cloudOn?'true':'false'); }}
function loadRange(range, save){{
  curRange=range; pollSpeed();
  fetch('/api/stats?range='+encodeURIComponent(range)+(cloudOn?'&cloud=1':''))
    .then(function(r){{ if(!r.ok) throw new Error('bad'); return r.json(); }})
    .then(function(d){{ applyHero(d); if(save!==false) localStorage.setItem(KEY, d.range); }})
    .catch(function(e){{ console.warn('range fetch failed', e); }});
}}
document.querySelectorAll('.rtab[data-range]').forEach(function(b){{
  b.addEventListener('click', function(){{ var r=b.getAttribute('data-range'); if(r) loadRange(r, true); }});
}});
(function(){{ var b=$('cloud-tog'); if(!b) return; paintCloud();
  b.addEventListener('click', function(){{ cloudOn=!cloudOn; localStorage.setItem(CKEY, cloudOn?'1':'0'); paintCloud(); loadRange(curRange, false); }}); }})();
if(active!=='all' || cloudOn) loadRange(active, true); else localStorage.setItem(KEY,'all');

var slot=$('hof-slot4');
var sel=$('hof-pick-sel');
if(slot && sel){{
  var extras=[];
  try{{ extras=JSON.parse(slot.getAttribute('data-hof-extras')||'[]'); }}catch(e){{ extras=[]; }}
  function applySlot4(rank){{
    var m=null;
    for(var i=0;i<extras.length;i++){{ if(String(extras[i].rank)===String(rank)){{ m=extras[i]; break; }} }}
    if(!m && extras.length) m=extras[0];
    if(!m) return;
    sel.value=String(m.rank);
    var lbl=$('hof4-lbl'); if(lbl) lbl.textContent='#4+ contenders';
    var nm=$('hof4-name'); if(nm) nm.textContent=m.name;
    setDseg($('hof4-tok'), m.tokens_fmt||fmt(m.tokens));
    var meta=$('hof4-meta'); if(meta) meta.textContent=(m.nreq_fmt||fmt(m.nreq))+' requests · '+Math.round(m.pct)+'% of total';
    localStorage.setItem(HOFKEY, String(m.rank));
  }}
  sel.addEventListener('change', function(){{ applySlot4(sel.value); }});
  var saved=localStorage.getItem(HOFKEY);
  if(saved && extras.some(function(e){{ return String(e.rank)===String(saved); }})) applySlot4(saved);
  else if(extras.length) applySlot4(extras[0].rank);
}}
}})();</script>
</body></html>"""


def page_mobile():
    """Portrait mobile layout. Same data as page(); separate markup/CSS only."""
    c = db(); q = lambda s, *a: c.execute(s, a).fetchall()
    P = "source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local')"
    tin, tout, nreq = q(f"SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),SUM(COALESCE(request_count,1)) FROM requests WHERE {P}")[0]
    tps = q(f"SELECT SUM(output_tokens)/SUM(eval_s) FROM requests WHERE {P} AND eval_s>0 AND output_tokens>0")[0][0]
    days = devastation_day_rows(q, P, 5)
    models = q(f"SELECT COALESCE(model,'?'), SUM(COALESCE(request_count,1)), SUM(COALESCE(input_tokens,0)+COALESCE(output_tokens,0)) t FROM requests WHERE {P} GROUP BY 1 ORDER BY t DESC LIMIT 3")
    cal = calendar(c); c.close()
    try: prices = json.load(open(PRICES))["models"]
    except Exception: prices = []
    mx_day = max([a + b for _, a, b in days] or [1]) or 1

    def seg_tach(n, frac, rising=True):
        frac = max(0.0, min(1.0, float(frac)))
        lit = int(round(frac * n))
        warn_from, red_from = int(0.70 * n), int(0.85 * n)
        parts = []
        for i in range(n):
            h = (28 + int(72 * (i / max(n - 1, 1)))) if rising else 70
            if i < lit:
                cls = "seg-on redline" if i >= red_from else ("seg-on warn" if i >= warn_from else "seg-on")
            else:
                cls = "seg-off"
            parts.append("<i class='%s' style='height:%d%%'></i>" % (cls, h))
        return "".join(parts)

    dayrows = []
    for d, a, b in days:
        tot_d = a + b
        frac = tot_d / mx_day if mx_day else 0
        dayrows.append(
            "<li class=day-item>"
            "<div class=day-top><span class=day-date>%s</span>"
            "<span class=day-io><span class=io-lab>In</span> %s"
            " <span class=io-lab>Out</span> %s</span></div>"
            "<div class='tach-track day-tach'>%s</div></li>"
            % (d, fmt(a), fmt(b), seg_tach(24, frac, False))
        )
    dayrows = "".join(dayrows) or (
        "<li class=muted>The silence before the storm. Point a client at port %d.</li>" % PROXY_PORT
    )
    _tot = sum(t or 0 for _, _, t in models) or 1
    # OpenCode → Spark local usage as a Hall of Fame entry (same card style).
    # Tokens already flow through the proxy; do not inflate _tot.
    # Attribution by serving model: OpenCode's local Flash calls already sit in the proxy model rows.
    _medal = {0: "#1 legend", 1: "#2 champion", 2: "#3 master"}
    models = models[:3]
    modrows = "".join(
        hof_card_html(_medal.get(i, "#%d" % (i + 1)), m, t or 0, n, 100 * (t or 0) / _tot)
        for i, (m, n, t) in enumerate(models)
    ) or "<div class=card><div class=muted>No champions yet. The arena awaits.</div></div>"
    sav = ""
    for i, p in enumerate(prices):
        if p.get("input_per_mtok") is None or p.get("output_per_mtok") is None:
            sav += (
                "<div class='card bill'><div class=bill-row>"
                "<div class='big vfd seg'>n/a</div>"
                "<div class=bill-meta><div class=lbl title='%s'>%s</div>"
                "<div class=muted>price unverified</div></div></div></div>"
                % (html.escape(p["label"], quote=True), html.escape(p["label"]))
            )
            continue
        v = tin / 1e6 * p["input_per_mtok"] + tout / 1e6 * p["output_per_mtok"]
        mv = money(v)
        mv_num = mv[1:] if mv.startswith("$") else mv
        sav += (
            "<div class='card bill'><div class=bill-row>"
            "<div class='bill-amt'><span class='bill-cur'>$</span>"
            "<span class='big dseg seg' data-val='%s' data-ghost='%s'>%s</span></div>"
            "<div class=bill-meta><div class=lbl title='%s'>Kept from %s</div>"
            "<div class=muted>$%s/$%s per M in/out</div></div></div></div>"
            % (mv_num, ghost_digits(mv_num), mv_num, html.escape(p["label"], quote=True), html.escape(p["label"]),
               p["input_per_mtok"], p["output_per_mtok"])
        )

    SP = speed_payload()
    tps_v = SP["live"]
    tps_max = float(SP["scale"])
    tot = tin + tout
    tps_disp = SP["live_disp"]
    tach = seg_tach(28, tps_v / tps_max, True)
    tach_scale = "".join("<span>%d</span>" % v for v in SP["ticks"])
    peak_label = html.escape(SP["peak_label"])
    peak_short = "%.1f" % SP["peak"]
    peak_mark = "<b class=peak-mark id=peak-mark style='left:%.2f%%' title='%s'></b>" % (100.0 * min(1.0, SP["peak"] / tps_max), peak_label)
    lights = [("IGN", True, "green"), ("PROXY", True, "green"), (str(PROXY_PORT), True, "green"),
              ("LOCAL", True, "green"), ("CLOUD", False, "amber")]
    lights.extend(spark_temp_lights())
    lights_html = "".join('<span class="wl %s%s">%s</span>' % (c, " on" if on else "", lab) for lab, on, c in lights)

    range_tabs = range_tabs_html("all", include_mobile=False, include_cloud=True)
    T = "DGX Token Annihilator"
    # reuse same embedded VFD font as desktop by reading it from page()'s constant if present;
    # fall back to serving without re-embedding (DSEG7 still loads from /font/)
    VFD_B64 = ''
    try:
        # pull from already-defined page source string is hard; keep empty and rely on DSEG7 + system mono
        pass
    except Exception:
        pass


    return f"""<!doctype html><html lang=en><head>
<meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#030505">
<meta http-equiv=refresh content=60>
<title>{T} · Mobile</title>
<style>
@font-face{{font-family:"DSEG7";src:url("/font/dseg7.woff2") format("woff2"),url("/font/dseg7.ttf") format("truetype");font-weight:700;font-style:italic;font-display:swap;}}
:root{{
  --bg:#030505;--panel:#0a100c;--green:#39ff14;--green-bright:#c8ff9a;--green-mid:#6dff4a;
  --line:#2a6b2a;--line-strong:#3a9a3a;--unlit:#152218;--unlit-edge:#2a3e2a;
  --warn:#ffcc00;--redline:#ff3d1a;--muted:#a8d898;--label:#f0ffe8;--cyan:#7ef0ff;
  --mono:ui-monospace,"Cascadia Mono","SF Mono",Menlo,Consolas,monospace;
  --dseg:"DSEG7",ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;
  --safe-t:env(safe-area-inset-top,0px);--safe-b:env(safe-area-inset-bottom,0px);
  --safe-l:env(safe-area-inset-left,0px);--safe-r:env(safe-area-inset-right,0px);
}}
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{width:100%;min-height:100%;background:var(--bg);color:var(--green);overflow-x:hidden;}}
body{{
  font-family:var(--mono);font-size:15px;line-height:1.35;
  padding:calc(10px + var(--safe-t)) calc(12px + var(--safe-r)) calc(16px + var(--safe-b)) calc(12px + var(--safe-l));
  max-width:100vw;
}}
body::before{{content:"";pointer-events:none;position:fixed;inset:0;z-index:9999;
  background:repeating-linear-gradient(0deg,rgba(0,0,0,.08) 0,rgba(0,0,0,.08) 1px,transparent 1px,transparent 3px);opacity:.14;}}
.cluster{{
  position:relative;
  --cluster-gap:12px;--cluster-pad-y:12px;--cluster-pad-x:10px;
  width:100%;max-width:480px;margin:0 auto;display:flex;flex-direction:column;gap:var(--cluster-gap);
  border:3px solid var(--line-strong);border-radius:12px;padding:var(--cluster-pad-y) var(--cluster-pad-x);
  overflow-x:hidden;
  background:linear-gradient(180deg,#07100a,#030505);
  box-shadow:inset 0 0 24px #39ff1412,0 0 12px #39ff1420;
}}
.hdr{{display:flex;flex-direction:column;gap:8px;}}
.hdr-top{{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;}}
h1{{margin:0;color:var(--green-bright);letter-spacing:.06em;text-transform:uppercase;
  font-size:15px;font-weight:800;line-height:1.25;
  text-shadow:0 0 5px rgba(57,255,20,.35),0 0 1px rgba(200,255,154,.55);}}
.desk-link{{font-size:12px;color:var(--muted);text-decoration:none;border:1px solid var(--line);padding:4px 8px;border-radius:4px;white-space:nowrap;}}
.dash-toggle{{
  position:static;z-index:20;align-self:flex-end;
  appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;
  font:700 8px/1 var(--mono);letter-spacing:.04em;text-transform:uppercase;padding:2px 5px;border-radius:2px;
  text-align:center;white-space:nowrap;text-decoration:none;display:inline-flex;align-items:center;justify-content:center;
}}
.dash-toggle:hover,.dash-toggle:active{{color:var(--green-bright);border-color:var(--line);}}
.foot-bar{{position:relative;display:flex;align-items:center;justify-content:center;width:100%;box-sizing:border-box;z-index:5;
  margin-top:calc(-1 * var(--cluster-gap));margin-bottom:calc(-1 * var(--cluster-pad-y));
  min-height:calc(32px + var(--cluster-gap) + var(--cluster-pad-y));padding:2px 0;}}
.foot-bar .foot{{padding:0 100px 0 0;margin:0;text-align:center;overflow-wrap:anywhere;flex:1;min-width:0;}}
.foot-bar .dash-toggle{{margin-left:auto;}}
.view-tog,.mode-tog{{display:inline-flex;flex-wrap:nowrap;gap:0;align-items:stretch;padding:0;
  border:2px solid #5a3a00;border-radius:4px;background:#0a0a0a;overflow:hidden;}}
.mode-tog{{position:absolute;right:0;top:50%;transform:translateY(-50%);z-index:20;}}
.mode-tog .mtab{{appearance:none;cursor:pointer;border:0;border-right:1px solid #5a3a00;background:#0a0a0a;color:#9a7a30;
  font:700 11px/1 var(--mono);letter-spacing:.04em;text-transform:uppercase;padding:4px 9px;text-align:center;
  white-space:nowrap;text-decoration:none;display:inline-flex;align-items:center;justify-content:center;}}
.mode-tog .mtab:last-child{{border-right:0;}}
.mode-tog .mtab.on{{color:#1a1000;background:var(--warn);box-shadow:inset 0 0 5px rgba(255,255,255,.12);}}
.wl-row{{display:flex;flex-wrap:wrap;gap:5px;}}
.range-tog{{display:flex;flex-wrap:wrap;gap:3px;width:100%;max-width:100%;padding:2px;border:1px solid var(--line-strong);
  border-radius:4px;background:#040806;justify-content:center;margin-top:4px;overflow:hidden;}}
.rtab{{appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;
  font:700 10px/1 var(--mono);letter-spacing:.04em;text-transform:uppercase;padding:6px 7px;border-radius:3px;
  text-align:center;white-space:nowrap;text-decoration:none;flex:1 1 auto;min-width:40px;
  display:inline-flex;align-items:center;justify-content:center;}}
.rtab.on{{color:#041204;background:var(--green);border-color:var(--green-bright);}}
.rtab.ctog{{color:#5a1a1a;background:#120606;border-color:#3a1212;box-shadow:none;text-shadow:none;}}
.rtab.ctog:hover{{color:#a33;border-color:#5a1a1a;}}
.rtab.ctog.on{{color:#fff4f0;background:#ff1a1a;border-color:#ff6a5a;text-shadow:0 0 4px rgba(255,220,210,.9);
  box-shadow:0 0 6px rgba(255,30,30,.9),0 0 14px rgba(255,30,30,.55),inset 0 0 4px rgba(255,255,255,.35);}}
.rtab.rtab-link{{color:var(--muted);flex:0 0 auto;}}
.hero .odo{{white-space:nowrap;}}
.range-delta{{display:none;}}
.range-delta.show{{display:block;text-align:center;font-size:11px;font-weight:800;color:var(--green-mid);}}
.range-delta.show.up{{color:var(--green-bright);}}
.range-delta.show.down{{color:var(--redline);}}
.wl{{font-size:11px;letter-spacing:.06em;padding:3px 7px;border:2px solid #333;color:#777;
  background:#0a0a0a;border-radius:3px;text-transform:uppercase;font-weight:700;}}
.wl.green.on{{color:#041204;background:var(--green);border-color:var(--green);
  box-shadow:0 0 6px rgba(57,255,20,.5);}}
.wl.amber{{border-color:#5a3a00;color:#9a7a30;}}
.wl.amber.on{{color:#1a1000;background:var(--warn);border-color:var(--warn);box-shadow:0 0 6px rgba(255,204,0,.5),inset 0 0 5px rgba(255,255,255,.12);}}
.wl.red{{border-color:#5a1000;color:#9a4030;}}
.wl.red.on{{color:#1a0500;background:var(--redline);border-color:var(--redline);box-shadow:0 0 6px rgba(255,61,26,.5),inset 0 0 5px rgba(255,255,255,.12);}}
.tagline{{color:var(--muted);font-size:12px;line-height:1.4;white-space:normal;}}
.hero{{
  display:flex;flex-direction:column;align-items:center;gap:8px;
  padding:14px 8px 12px;border:3px solid var(--line-strong);border-radius:10px;background:#050a07;
  box-shadow:inset 0 0 18px #39ff1418;overflow:hidden;
}}
.hero .odo-lbl{{font-size:12px;color:var(--label);letter-spacing:.14em;font-weight:800;text-align:center;}}
.hero .odo{{
  position:relative;display:inline-block;isolation:isolate;overflow:hidden;
  font-family:var(--dseg);font-size:clamp(26px,11vw,42px);font-weight:700;font-style:italic;color:var(--green-bright);
  letter-spacing:.05em;font-variant-numeric:tabular-nums;line-height:1.45;
  padding:.28em .12em .32em;
  text-shadow:0 0 1px rgba(230,255,210,.95),0 0 3px rgba(57,255,20,.9),0 0 8px rgba(57,255,20,.45);
}}
.dseg{{
  font-family:var(--dseg);font-weight:700;font-style:italic;position:relative;z-index:1;
  text-shadow:0 0 1px rgba(230,255,210,.9),0 0 3px rgba(57,255,20,.8),0 0 8px rgba(57,255,20,.4);
}}
.odo::after,.big.dseg::after{{
  content:attr(data-val);position:absolute;left:0;top:0;right:0;bottom:0;
  color:rgba(57,255,20,.45);pointer-events:none;z-index:-1;filter:blur(.45px);opacity:.8;
  text-shadow:0 0 6px rgba(57,255,20,.35);padding:inherit;box-sizing:border-box;
}}
.odo::before,.big.dseg::before{{
  content:attr(data-ghost);position:absolute;left:0;top:0;right:0;bottom:0;
  color:rgba(40,90,50,.34);pointer-events:none;z-index:-2;filter:blur(.3px);opacity:.75;
  text-shadow:none;padding:inherit;box-sizing:border-box;
}}
.big.dseg{{display:inline-block;line-height:1.55;padding:.28em .12em .34em;margin:4px 0;}}
.vfd{{letter-spacing:.05em;text-shadow:0 0 6px rgba(57,255,20,.4);}}
.panel{{
  display:flex;flex-direction:column;gap:10px;
  border:2px solid var(--line);border-radius:8px;padding:12px 10px;background:#050a07;
  overflow:hidden;
}}
h2{{font-size:13px;text-transform:uppercase;letter-spacing:.1em;color:var(--label);
  display:flex;align-items:center;gap:8px;font-weight:800;margin:0 0 2px;
  text-shadow:0 0 3px rgba(57,255,20,.28);}}
h2::before,h2::after{{content:"";flex:1;height:2px;background:linear-gradient(90deg,transparent,var(--line-strong),transparent);}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
.card{{background:var(--panel);border:2px solid var(--line);border-radius:6px;padding:10px 9px;
  display:flex;flex-direction:column;gap:4px;min-width:0;overflow:hidden;}}
.card.speed{{grid-column:1 / -1;border-color:var(--line-strong);box-shadow:inset 0 0 14px #39ff1414;}}
.lbl{{color:var(--label);font-size:13px;font-weight:800;letter-spacing:.02em;line-height:1.25;
  white-space:normal;overflow-wrap:anywhere;}}
.big{{font-size:clamp(18px,6.5vw,26px);color:var(--green-bright);letter-spacing:.04em;line-height:1.2;}}
.big.dseg{{font-size:clamp(18px,7vw,28px);}}
.grid .card:not(.speed) .big.dseg{{
  font-size:clamp(12px,3.6vw,16px);letter-spacing:.02em;display:block;width:100%;
  max-width:100%;overflow:hidden;white-space:nowrap;line-height:1.55;
}}
.grid .card:not(.speed) .big.dseg::before,.grid .card:not(.speed) .big.dseg::after{{
  white-space:nowrap;overflow:hidden;
}}
.muted{{color:var(--muted);font-size:12px;line-height:1.35;margin-top:2px;white-space:normal;overflow-wrap:anywhere;}}
.mname{{font-size:14px;font-weight:800;color:var(--cyan);margin:2px 0 4px;overflow-wrap:anywhere;
  text-shadow:0 0 4px rgba(126,240,255,.3);}}
.speed-stack{{display:flex;flex-direction:column;gap:8px;width:100%;}}
.tach-track{{display:flex;align-items:flex-end;gap:2px;height:48px;border-bottom:3px solid var(--line-strong);width:100%;}}
.day-tach{{height:10px;gap:1px;border-bottom-width:1px;align-items:stretch;}}
.day-tach i{{height:100% !important;border-radius:1px;min-width:1px;}}
.tach-track i{{display:block;flex:1;min-width:2px;border-radius:1px 1px 0 0;border:1px solid transparent;}}
.seg-on{{background:var(--green);border-color:#6dff4a;box-shadow:0 0 5px rgba(57,255,20,.45);}}
.seg-on.warn{{background:var(--warn);border-color:#ffe066;box-shadow:0 0 5px rgba(255,204,0,.45);}}
.seg-on.redline{{background:var(--redline);border-color:#ff8a6a;box-shadow:0 0 5px rgba(255,61,26,.5);}}
.seg-off{{background:var(--unlit);border-color:var(--unlit-edge);}}
.speed-track{{position:relative;}}.pk-short{{color:#ff8a6a;}}.speed-tach .speed-peak{{display:none;}}.peak-mark{{position:absolute;top:-4px;bottom:-4px;width:3px;margin-left:-1px;background:#ff3d1a;border-radius:1px;box-shadow:0 0 6px rgba(255,61,26,.9),0 0 12px rgba(255,61,26,.5);pointer-events:auto;}}.peak-mark::before{{content:'';position:absolute;top:-5px;left:-3px;border:4.5px solid transparent;border-top:5px solid #ff3d1a;}}.speed-peak{{font-size:10px;color:#ff8a6a;font-weight:800;letter-spacing:.06em;text-transform:uppercase;opacity:.9;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.tach-scale{{display:flex;justify-content:space-between;font-size:11px;color:var(--muted);font-weight:700;}}
.speed-readout{{display:flex;align-items:baseline;justify-content:center;gap:8px;}}
.speed-readout .big.dseg{{margin:0;font-size:clamp(22px,8vw,34px);}}
.speed-unit{{font-size:12px;color:var(--muted);font-weight:800;letter-spacing:.08em;text-transform:uppercase;}}
.bill-grid,.hof-grid{{display:flex;flex-direction:column;gap:8px;}}
.bill-row{{display:flex;align-items:center;gap:8px;min-width:0;width:100%;}}
.bill-amt{{display:inline-flex;align-items:baseline;overflow:visible;}}
.bill-cur{{font-family:var(--dseg);font-weight:700;font-style:italic;color:var(--green-bright);
  font-size:clamp(16px,5.5vw,22px);margin-right:.1em;text-shadow:0 0 3px rgba(57,255,20,.8);}}
.bill-row .big{{flex:0 0 auto;margin:0;font-size:clamp(16px,5.5vw,22px);}}
.bill-row .big.dseg,.bill-amt .big.dseg{{padding:.12em .1em .14em;margin:0;line-height:1.35;overflow:visible;}}
.bill-meta{{flex:1 1 auto;min-width:0;}}
.bill-meta .lbl,.bill-meta .muted{{margin:0;}}
.cal{{display:flex;flex-direction:column;gap:6px;width:100%;overflow:hidden;}}
.cal svg{{width:100%;height:auto;display:block;max-width:100%;}}
.ct{{fill:var(--label);font-family:var(--mono);font-weight:700;}}
.ct-month{{font-size:9px;}}.ct-dow{{font-size:8px;fill:var(--muted);}}
.day.lit{{filter:drop-shadow(0 0 1.5px rgba(255,200,80,.4));}}
.legend{{display:flex;align-items:center;justify-content:flex-end;gap:5px;font-size:11px;color:var(--muted);font-weight:700;flex-wrap:wrap;}}
.leg-lbl{{letter-spacing:.05em;text-transform:uppercase;}}
.sw{{display:inline-block;width:11px;height:11px;border-radius:2px;vertical-align:middle;border:1.5px solid transparent;flex-shrink:0;}}
.sw.empty{{border-color:#3d5240 !important;}}
#tip{{position:fixed;left:max(12px,var(--safe-l));right:max(12px,var(--safe-r));
  bottom:max(12px,var(--safe-b));display:none;background:#000;border:2px solid var(--green);
  color:var(--green-bright);font-size:13px;padding:14px 44px 14px 14px;border-radius:10px;
  pointer-events:auto;z-index:10000;box-shadow:0 0 18px rgba(57,255,20,.28),0 -6px 28px rgba(0,0,0,.65);
  line-height:1.4;max-height:40vh;overflow:auto;-webkit-overflow-scrolling:touch;}}
#tip .tip-x{{position:absolute;top:4px;right:6px;width:36px;height:36px;border:0;background:transparent;
  color:var(--green-bright);font:700 26px/1 var(--mono);cursor:pointer;padding:0;
  text-shadow:0 0 8px rgba(57,255,20,.55);}}
#tip .tip-txt{{display:block;}}
#tip-scrim{{position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9999;display:none;}}
#tip-scrim.open{{display:block;}}
.day-log{{list-style:none;display:flex;flex-direction:column;gap:8px;}}
.day-item{{border-bottom:1px solid var(--line);padding-bottom:8px;}}
.day-item:last-child{{border-bottom:0;padding-bottom:0;}}
.day-top{{display:flex;justify-content:space-between;align-items:baseline;gap:8px;margin-bottom:4px;}}
.day-date{{font-size:13px;font-weight:700;color:var(--green-mid);}}
.day-io{{font-family:var(--mono);font-size:12px;color:var(--muted);font-weight:400;text-shadow:none;white-space:normal;overflow-wrap:anywhere;text-align:right;}}
.io-lab{{color:var(--label);font-size:11px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;margin-right:2px;}}
.foot{{text-align:center;color:var(--muted);font-size:12px;letter-spacing:.03em;padding-top:4px;}}
.seg{{font-variant-numeric:tabular-nums;}}
</style></head><body><div class=cluster>
<div class=hdr>
  <div class=hdr-top>
    <h1>{T}</h1>
  </div>
  <div class=wl-row>{lights_html}</div>
  <nav class="range-tog" role="tablist" aria-label="Time range">{range_tabs}</nav>
</div>
<div class=tagline>Every token on port {PROXY_PORT} is one the cloud will never bill. Last report: {datetime.now():%b %d %H:%M}.</div>

<div class=hero>
  <div class=odo-lbl id=hero-lbl>TOTAL TOKENS ANNIHILATED</div>
  <div class="odo dseg seg" id=hero-odo data-val="{fmt(tot)}" data-ghost="{ghost_digits(fmt(tot))}">{fmt(tot)}</div>
  <div class="range-delta" id="range-delta"></div>
</div>

<div class="panel body">
  <h2>Body count</h2>
  <div class=grid>
    <div class="card speed">
      <div class=lbl id=speed-lbl>Annihilation speed (avg · ALL)</div>
      <div class=speed-stack>
        <div class='tach-track speed-track' id=speed-tach>{tach}{peak_mark}</div>
        <div class=tach-scale id=speed-scale>{tach_scale}</div>
        <div class=speed-peak id=speed-peak>{peak_label}</div>
        <div class=speed-readout>
          <div class="big dseg seg" id=speed-val data-val="{tps_disp}" data-ghost="{ghost_digits(tps_disp)}">{tps_disp}</div>
          <div class=speed-unit>tok/s · <span class=pk-short id=speed-pk title='{peak_label}'>peak {peak_short}</span></div>
        </div>
      </div>
    </div>
    <div class=card><div class=lbl>Tokens annihilated</div><div class="big dseg seg" data-val="{fmt(tot)}" data-ghost="{ghost_digits(fmt(tot))}">{fmt(tot)}</div></div>
    <div class=card><div class=lbl>Prompts (input)</div><div class="big dseg seg" data-val="{fmt(tin)}" data-ghost="{ghost_digits(fmt(tin))}">{fmt(tin)}</div></div>
    <div class=card><div class=lbl>Words (output)</div><div class="big dseg seg" data-val="{fmt(tout)}" data-ghost="{ghost_digits(fmt(tout))}">{fmt(tout)}</div></div>
    <div class=card><div class=lbl>Requests</div><div class="big dseg seg" data-val="{fmt(nreq)}" data-ghost="{ghost_digits(fmt(nreq))}">{fmt(nreq)}</div></div>
  </div>
</div>

<div class="panel cloud">
  <h2>Cloud bills obliterated</h2>
  <div class="bill-grid">{sav}</div>
</div>

<div class="panel year">
  <h2>Year of destruction</h2>
  {cal}
</div>

<div class="panel daily">
  <h2>Daily devastation log</h2>
  <ul class=day-log>{dayrows}</ul>
</div>

<div class=panel>
  <h2>Hall of fame</h2>
  <div class="hof-grid">{modrows}</div>
</div>

<div class="foot-bar">
<p class=foot>No GPUs were harmed. Several cloud invoices were.</p>
<div class="mode-tog" aria-label="Local or cloud"><a class="mtab on" href="/m">LOCAL</a><a class="mtab" href="/cloud/m">CLOUD</a></div>
</div>
</div>
<script>(function(){{
var t=document.getElementById('tip');
if(!t)return;
var scrim=document.createElement('div');
scrim.id='tip-scrim';
document.body.appendChild(scrim);
var active=null;
function closeTip(){{
  t.style.display='none';
  scrim.classList.remove('open');
  active=null;
}}
function openTip(r){{
  if(active===r){{closeTip();return;}}
  active=r;
  t.innerHTML='<button type="button" class="tip-x" aria-label="Close">\u00d7</button><span class="tip-txt"></span>';
  t.querySelector('.tip-txt').textContent=r.dataset.t||'';
  t.querySelector('.tip-x').addEventListener('click',function(e){{e.preventDefault();e.stopPropagation();closeTip();}});
  t.style.display='block';
  scrim.classList.add('open');
}}
document.querySelectorAll('rect[data-t]').forEach(function(r){{
  r.style.cursor='pointer';
  r.addEventListener('click',function(e){{
    e.preventDefault();
    e.stopPropagation();
    openTip(r);
  }});
}});
scrim.addEventListener('click',function(e){{e.preventDefault();closeTip();}});
t.addEventListener('click',function(e){{e.stopPropagation();}});
document.addEventListener('keydown',function(e){{if(e.key==='Escape')closeTip();}});
}})();</script>
<script>(function(){{
var KEY='tad-range2';
var active=localStorage.getItem(KEY)||'all';
if(['all','today','dod','mtd','weekly','monthly','yearly'].indexOf(active)<0) active='all';
function $(id){{return document.getElementById(id);}}
function setDseg(el, text){{
  if(!el) return;
  el.textContent=text;
  el.setAttribute('data-val', text);
  el.setAttribute('data-ghost', String(text).replace(/\d/g,'8'));
}}
function fmt(n){{return String(Math.trunc(n||0)).replace(/\B(?=(\d{{3}})+(?!\d))/g,',');}}
function applyHero(d){{
  var lbl=$('hero-lbl'); if(lbl) lbl.textContent=d.hero_label;
  setDseg($('hero-odo'), d.tot_fmt);
  var delta=$('range-delta');
  if(delta){{
    if(d.delta){{
      var arrow=d.delta.dir==='up'?'\u25b2':(d.delta.dir==='down'?'\u25bc':'\u25cf');
      var sign=d.delta.abs>0?'+':'';
      delta.className='range-delta show '+(d.delta.dir||'flat');
      delta.textContent=arrow+' '+sign+fmt(d.delta.abs)+' ('+sign+d.delta.pct+'%) '+d.delta.label;
    }} else {{ delta.className='range-delta'; delta.textContent=''; }}
  }}
  document.querySelectorAll('.rtab[data-range]').forEach(function(b){{
    var on=b.getAttribute('data-range')===d.range;
    b.classList.toggle('on', on);
    b.setAttribute('aria-selected', on?'true':'false');
  }});
}}
function applySpeed(s){{
  var t=$('speed-tach'); if(!t||!s||s.error) return;
  var segs=t.querySelectorAll('i'); var n=segs.length;
  var lit=Math.round(Math.max(0,Math.min(1,s.live/s.scale))*n);
  var wf=Math.floor(0.70*n), rf=Math.floor(0.85*n);
  segs.forEach(function(el,i){{ el.className = i<lit ? (i>=rf?'seg-on redline':(i>=wf?'seg-on warn':'seg-on')) : 'seg-off'; }});
  setDseg($('speed-val'), s.live_disp);
  var pm=$('peak-mark'); if(pm){{ pm.style.left=(100*Math.min(1,s.peak/s.scale))+'%'; pm.title=s.peak_label; }}
  var pl=$('speed-peak'); if(pl) pl.textContent=s.peak_label;
  var lb=$('speed-lbl'); if(lb&&s.range_label) lb.textContent='Annihilation speed (avg · '+s.range_label+')';
  var pk=$('speed-pk'); if(pk){{ pk.textContent='peak '+s.peak.toFixed(1); pk.title=s.peak_label; }}
  var sc=$('speed-scale'); if(sc&&s.ticks) sc.innerHTML=s.ticks.map(function(v){{return '<span>'+v+'</span>';}}).join('');
}}
function pollSpeed(){{ fetch('/api/speed?range='+encodeURIComponent(curRange||'all'),{{cache:'no-store'}}).then(function(r){{return r.json();}}).then(applySpeed).catch(function(){{}}); }}
setInterval(pollSpeed, 30000);
var CKEY='tad-cloud';
var cloudOn=localStorage.getItem(CKEY)==='1';
var curRange=active;
function paintCloud(){{ var b=$('cloud-tog'); if(!b) return; b.classList.toggle('on', cloudOn); b.setAttribute('aria-pressed', cloudOn?'true':'false'); }}
function loadRange(range, save){{
  curRange=range; pollSpeed();
  fetch('/api/stats?range='+encodeURIComponent(range)+(cloudOn?'&cloud=1':''))
    .then(function(r){{ if(!r.ok) throw new Error('bad'); return r.json(); }})
    .then(function(d){{ applyHero(d); if(save!==false) localStorage.setItem(KEY, d.range); }})
    .catch(function(e){{ console.warn('range fetch failed', e); }});
}}
document.querySelectorAll('.rtab[data-range]').forEach(function(b){{
  b.addEventListener('click', function(){{ var r=b.getAttribute('data-range'); if(r) loadRange(r, true); }});
}});
(function(){{ var b=$('cloud-tog'); if(!b) return; paintCloud();
  b.addEventListener('click', function(){{ cloudOn=!cloudOn; localStorage.setItem(CKEY, cloudOn?'1':'0'); paintCloud(); loadRange(curRange, false); }}); }})();
if(active!=='all' || cloudOn) loadRange(active, true); else localStorage.setItem(KEY,'all');
}})();</script>
</body></html>"""


def page_cloud(mobile=False, range_key="all"):
    """Dedicated Cloud Usage dashboard (Codex/GPT, Grok Bot, DeepSeek via OpenCode)."""
    rk = (range_key or "all").lower()
    rk = RANGE_ALIASES.get(rk, rk)
    if rk not in RANGES: rk = "all"
    providers = load_cloud_providers(rk)
    cov_bits = []
    for _p in providers:
        if _p.get("id") == "deepseek-opencode":
            cov_bits.append("DeepSeek (via OpenCode): real (%s) from opencode.db deepseek* sessions" % RANGE_META.get(rk, (rk,))[0].lower())
        elif _p.get("id") == "codex":
            cov_bits.append("Codex: today/week/all + rolling 24H from rollout event deltas; other tabs approximate")
        elif _p.get("id") == "grok":
            cov_bits.append("Grok: current snapshot only; historical ranges filling via cloud_snapshots.jsonl")
    range_cov = " · ".join(cov_bits)
    range_tabs = range_tabs_html(rk, include_mobile=False)
    if rk == "dod":
        for _p in providers:
            if _p.get("id") == "codex":
                _p["stats"] = [s for s in (_p.get("stats") or []) if s.get("label") == "24H tok"]
            if _p.get("id") == "grok":
                _p["note"] = ((_p.get("note") or "") + " · snapshot=current (no rolling token counts)").strip(" ·")
    elif rk == "today":
        for _p in providers:
            if _p.get("id") == "codex":
                _p["stats"] = [s for s in (_p.get("stats") or []) if s.get("label") == "Today tok"]
            if _p.get("id") == "grok":
                _p["note"] = ((_p.get("note") or "") + " · snapshot=current").strip(" ·")
    elif rk not in ("all", "weekly"):
        for _p in providers:
            if _p.get("id") == "grok":
                _p["note"] = ((_p.get("note") or "") + " · snapshot=current (filling)").strip(" ·")

    cards = render_cloud_usage_cards(providers, compact=bool(mobile), show_stats=True)
    cloud_tot, _codex_tot, _grok_tot = cloud_total_tokens()
    tot_s = fmt(cloud_tot)
    tot_ghost = ghost_digits(tot_s)
    odo_fs = "clamp(18px,6.6vw,34px)" if mobile else "clamp(26px,3.6vw,56px)"
    T = "DGX Cloud Usage"
    back_href = "/m" if mobile else "/"
    back_lbl = "LOCAL"
    # desktop uses range-tab look; mobile matches /m rtab
    if mobile:
        tog_css = """.dash-toggle{
  position:absolute;right:10px;bottom:8px;z-index:20;
  appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;
  font:700 8px/1 var(--mono);letter-spacing:.04em;text-transform:uppercase;padding:2px 5px;border-radius:2px;
  text-align:center;white-space:nowrap;text-decoration:none;display:inline-flex;align-items:center;justify-content:center;
}
.dash-toggle:hover,.dash-toggle:active{color:var(--green-bright);border-color:var(--line);}"""
        body_pad = "calc(14px + var(--safe-t)) calc(12px + var(--safe-r)) calc(64px + var(--safe-b)) calc(12px + var(--safe-l))"
        grid_cols = "1fr"
        cluster_max = "480px"
        hero_tog_css = """.hero{padding-top:46px;}
.hero .range-tog{position:static;display:flex;flex-wrap:wrap;justify-content:center;gap:2px;width:100%;max-width:100%;border:1px solid var(--line-strong);border-radius:4px;background:#040806;padding:2px;overflow:hidden;}
.hero .range-tog .rtab{appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;font:700 10px/1 var(--mono);letter-spacing:.04em;text-transform:uppercase;padding:3px 6px;border-radius:2px;text-decoration:none;display:inline-flex;flex:1 1 auto;min-width:40px;justify-content:center;white-space:nowrap;}
.hero .range-tog .rtab.on{color:#041204;background:var(--green);border-color:var(--green-bright);}"""
    else:
        tog_css = """.dash-toggle{
  position:absolute;right:10px;bottom:8px;z-index:20;
  appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;
  font:700 8px/1 ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;letter-spacing:.04em;
  text-transform:uppercase;padding:2px 5px;border-radius:2px;text-align:center;white-space:nowrap;
  text-decoration:none;display:inline-flex;align-items:center;justify-content:center;
}
.dash-toggle:hover{color:var(--green-mid);border-color:var(--line);}"""
        body_pad = "calc(14px + var(--safe-t)) calc(16px + var(--safe-r)) calc(52px + var(--safe-b)) calc(16px + var(--safe-l))"
        grid_cols = "repeat(3,minmax(0,1fr))"
        cluster_max = "1100px"
        hero_tog_css = """.hero .range-tog{position:absolute;top:8px;right:10px;z-index:50;display:flex;flex-wrap:wrap;justify-content:flex-end;gap:1px;max-width:min(52vw,calc(100% - 150px));border:1px solid var(--line-strong);border-radius:4px;background:#040806;padding:1px;}
.hero .range-tog .rtab{appearance:none;cursor:pointer;border:1px solid #1a2e1a;background:#0a120c;color:#5a7a5a;font:700 10px/1 var(--mono);letter-spacing:.04em;text-transform:uppercase;padding:2px 6px;border-radius:2px;text-decoration:none;display:inline-flex;justify-content:center;white-space:nowrap;}
.hero .range-tog .rtab.on{color:#041204;background:var(--green);border-color:var(--green-bright);}"""

    return f"""<!doctype html><html lang=en><head>
<meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#030505">
<meta http-equiv=refresh content=60>
<title>{T}</title>
<style>
@font-face{{font-family:"DSEG7";src:url("/font/dseg7.woff2") format("woff2"),url("/font/dseg7.ttf") format("truetype");font-weight:700;font-style:italic;font-display:swap;}}
:root{{
  --bg:#030505;--panel:#0a100c;--green:#39ff14;--green-bright:#c8ff9a;--green-mid:#6dff4a;
  --line:#2a6b2a;--line-strong:#3a9a3a;--unlit:#152218;--unlit-edge:#2a3e2a;
  --warn:#ffcc00;--redline:#ff3d1a;--muted:#a8d898;--label:#f0ffe8;--cyan:#7ef0ff;
  --mono:ui-monospace,"Cascadia Mono","SF Mono",Menlo,Consolas,monospace;
  --dseg:"DSEG7",ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;
  --safe-t:env(safe-area-inset-top,0px);--safe-b:env(safe-area-inset-bottom,0px);
  --safe-l:env(safe-area-inset-left,0px);--safe-r:env(safe-area-inset-right,0px);
}}
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{width:100%;min-height:100%;background:var(--bg);color:var(--green);overflow-x:hidden;}}
body{{
  font-family:var(--mono);font-size:15px;line-height:1.35;
  padding:{body_pad};
}}
body::before{{content:"";pointer-events:none;position:fixed;inset:0;z-index:9999;
  background:repeating-linear-gradient(0deg,rgba(0,0,0,.08) 0,rgba(0,0,0,.08) 1px,transparent 1px,transparent 3px);opacity:.14;}}
.cluster{{
  position:relative;
  --cluster-gap:14px;--cluster-pad-y:16px;--cluster-pad-x:14px;
  width:100%;max-width:{cluster_max};margin:0 auto;display:flex;flex-direction:column;gap:var(--cluster-gap);
  border:3px solid var(--line-strong);border-radius:12px;padding:var(--cluster-pad-y) var(--cluster-pad-x);
  overflow-x:hidden;
  background:linear-gradient(180deg,#07100a,#030505);
  box-shadow:inset 0 0 24px #39ff1412,0 0 12px #39ff1420;
}}
.hdr{{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;}}
h1{{margin:0;color:var(--green-bright);letter-spacing:.08em;text-transform:uppercase;
  font-size:clamp(16px,2.2vw,22px);font-weight:800;line-height:1.25;
  text-shadow:0 0 5px rgba(57,255,20,.35),0 0 1px rgba(200,255,154,.55);}}
.tagline{{color:var(--muted);font-size:13px;line-height:1.4;}}
.panel.cluse{{
  display:flex;flex-direction:column;gap:12px;
  border:2px solid var(--line);border-radius:8px;padding:14px 12px;background:#050a07;
}}
.panel.cluse > h2{{
  margin:0;padding:0 2px;font-size:14px;text-transform:uppercase;letter-spacing:.12em;
  color:var(--label);font-weight:800;display:flex;align-items:center;gap:8px;
  text-shadow:0 0 3px rgba(57,255,20,.28);
}}
.panel.cluse > h2::before,.panel.cluse > h2::after{{content:"";flex:1;height:2px;
  background:linear-gradient(90deg,transparent,var(--line-strong),transparent);}}

.hero{{
  position:relative;
  display:flex;flex-direction:column;align-items:center;justify-content:flex-start;gap:clamp(4px,.4vh,8px);
  padding:clamp(10px,1vh,16px) clamp(8px,.8vh,14px) clamp(6px,.6vh,12px);
  border:3px solid var(--line-strong);border-radius:10px;background:#050a07;
  box-shadow:inset 0 0 18px #39ff1418;overflow:hidden;flex-shrink:0;
}}
.odo-lbl{{font-size:12px;color:var(--label);letter-spacing:.14em;font-weight:800;text-align:center;}}
.odo{{white-space:nowrap;}}
.hero .odo{{
  position:relative;display:inline-block;isolation:isolate;overflow:visible;flex-shrink:0;pointer-events:none;
  font-family:var(--dseg);font-size:{odo_fs};font-weight:700;font-style:italic;color:var(--green-bright);
  letter-spacing:.04em;font-variant-numeric:tabular-nums;line-height:1.45;z-index:1;
  padding:.28em .08em .32em;margin:0;max-width:100%;
  text-shadow:0 0 1px rgba(230,255,210,.95),0 0 3px rgba(57,255,20,.9),0 0 8px rgba(57,255,20,.45);
}}
.dseg{{
  font-family:var(--dseg);font-weight:700;font-style:italic;position:relative;z-index:1;
  text-shadow:0 0 1px rgba(230,255,210,.9),0 0 3px rgba(57,255,20,.8),0 0 8px rgba(57,255,20,.4);
}}
.odo::after{{
  content:attr(data-val);position:absolute;left:0;top:0;right:0;bottom:0;
  color:rgba(57,255,20,.5);pointer-events:none;z-index:-1;
  filter:blur(.5px);opacity:.85;
  text-shadow:0 0 6px rgba(57,255,20,.4),0 0 10px rgba(57,255,20,.22);
  padding:inherit;box-sizing:border-box;
}}
.odo::before{{
  content:attr(data-ghost);position:absolute;left:0;top:0;right:0;bottom:0;
  color:rgba(40,90,50,.34);pointer-events:none;z-index:-2;
  filter:blur(.35px);text-shadow:none;opacity:.8;
  padding:inherit;box-sizing:border-box;
}}
.cluse-grid{{
  display:grid;grid-template-columns:{grid_cols};
  gap:12px;align-content:start;
}}
.cluse-card{{
  display:flex;flex-direction:column;gap:8px;
  padding:12px 12px;border:2px solid var(--line);border-radius:6px;background:#050a07;
  min-width:0;overflow:hidden;box-shadow:inset 0 0 10px #39ff140c;
}}
.cluse-card-hd{{display:flex;align-items:baseline;justify-content:space-between;gap:6px;}}
.cluse-title{{
  font-size:clamp(13px,1.4vw,16px);font-weight:800;letter-spacing:.1em;text-transform:uppercase;
  color:var(--green-bright);text-shadow:0 0 4px rgba(57,255,20,.35);
}}
.cluse-plan{{font-size:11px;color:var(--muted);letter-spacing:.06em;flex-shrink:0;}}
.cluse-meter{{display:flex;flex-direction:column;gap:3px;min-width:0;}}
.cluse-meter-hd{{display:flex;align-items:baseline;justify-content:space-between;gap:6px;}}
.cluse-meter-hd .lbl{{font-size:11px;margin:0;color:var(--label);font-weight:700;letter-spacing:.04em;text-transform:uppercase;}}
.cluse-pct{{
  font-family:var(--dseg);font-size:clamp(16px,2vw,22px);color:var(--green-bright);font-weight:700;font-style:italic;
  text-shadow:0 0 5px rgba(57,255,20,.45);letter-spacing:.04em;
}}
.tach-track{{display:flex;align-items:flex-end;gap:2px;height:22px;border-bottom:2px solid var(--line-strong);width:100%;}}
.tach-track i{{display:block;flex:1;min-width:2px;border-radius:1px 1px 0 0;border:1px solid transparent;}}
.seg-on{{background:var(--green);border-color:#6dff4a;box-shadow:0 0 5px rgba(57,255,20,.45);}}
.seg-on.warn{{background:var(--warn);border-color:#ffe066;box-shadow:0 0 5px rgba(255,204,0,.45);}}
.seg-on.redline{{background:var(--redline);border-color:#ff8a6a;box-shadow:0 0 5px rgba(255,61,26,.5);}}
.seg-off{{background:var(--unlit);border-color:var(--unlit-edge);}}
.cluse-tach{{height:18px;gap:1px;}}
.cluse-detail{{font-size:11px;line-height:1.3;color:var(--muted);}}
.cluse-stat{{display:flex;align-items:baseline;justify-content:space-between;gap:6px;}}
.cluse-stat .lbl{{font-size:11px;margin:0;color:var(--label);font-weight:700;}}
.cluse-stat-val{{font-size:14px;color:var(--green-bright);text-shadow:0 0 4px rgba(57,255,20,.4);}}
.cluse-note{{
  color:var(--muted);font-size:13px;font-weight:700;letter-spacing:.08em;
  text-transform:uppercase;padding:18px 0;text-align:center;
}}
.cluse-card.status-not_linked{{opacity:.72;}}
.bot-av{{display:inline-block;vertical-align:middle;margin-right:4px;}}
.cluse-meter-hd .lbl{{display:inline-flex;align-items:center;gap:4px;}}
.range-cov{{font-size:11px;color:var(--muted);letter-spacing:.03em;margin:0 0 8px;line-height:1.35;}}
.view-tog,.mode-tog{{display:inline-flex;gap:0;border:2px solid #5a3a00;border-radius:4px;background:#0a0a0a;overflow:hidden;}}
.view-tog{{position:absolute;top:8px;left:10px;z-index:50;}}
.mode-tog{{position:absolute;right:0;top:50%;transform:translateY(-50%);z-index:20;}}
.view-tog .vtab,.mode-tog .mtab{{border:0;border-right:1px solid #5a3a00;background:#0a0a0a;color:#9a7a30;font:700 11px/1 var(--mono);letter-spacing:.04em;text-transform:uppercase;padding:4px 9px;text-decoration:none;display:inline-flex;align-items:center;}}
.view-tog .vtab:last-child,.mode-tog .mtab:last-child{{border-right:0;}}
.view-tog .vtab.on,.mode-tog .mtab.on{{color:#1a1000;background:var(--warn);}}
{hero_tog_css}

.cluse-card.status-awaiting .cluse-note{{color:var(--warn);}}
.muted{{color:var(--muted);}}
.foot{{text-align:center;color:var(--muted);font-size:12px;letter-spacing:.03em;padding:0;margin:0;}}.foot-bar{{position:relative;display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:calc(-1 * var(--cluster-gap));margin-bottom:calc(-1 * var(--cluster-pad-y));min-height:calc(32px + var(--cluster-gap) + var(--cluster-pad-y));padding:0 2px;box-sizing:border-box;z-index:5;}}.foot-bar .foot{{flex:1;min-width:0;padding:0 110px 0 0;box-sizing:border-box;}}
{tog_css}
</style></head><body><div class=cluster>
<div class=hdr>
  <h1>{T}</h1>
</div>
<div class=tagline>Non-Spark cloud spend and quota · Codex / GPT · Grok Bot · DeepSeek (via OpenCode) · refreshed {datetime.now():%b %d %H:%M}.</div>
<div class=hero>
  <div class="view-tog" aria-label="Desktop or mobile">
    <a class="vtab{" on" if not mobile else ""}" href="/cloud" title="Desktop cloud">DESKTOP</a>
    <a class="vtab{" on" if mobile else ""}" href="/cloud/m" title="Mobile cloud">MOBILE</a>
  </div>
  <nav class="range-tog" role="tablist" aria-label="Time range">{range_tabs}</nav>
  <div class=odo-lbl>TOTAL CLOUD TOKENS</div>
  <div class="odo dseg seg" id=hero-odo data-val="{tot_s}" data-ghost="{tot_ghost}">{tot_s}</div>
</div>
<div class="panel cluse">
  <h2>Cloud usage</h2>
  <p class="range-cov">{range_cov}</p>
  <div class="cluse-grid">{cards}</div>
</div>
<div class="foot-bar"><p class=foot>Quota meters only — Token CEO does not pause queues.</p><div class="mode-tog" aria-label="Local or cloud"><a class="mtab" href="{back_href}" title="Local annihilator">LOCAL</a><a class="mtab on" href="/cloud{"/m" if mobile else ""}" title="Cloud usage">CLOUD</a></div></div>
</div>
<script>(function(){{
var tabs=document.querySelectorAll('.hero .range-tog .rtab[data-range]');
tabs.forEach(function(btn){{
  btn.addEventListener('click', function(){{
    var r=btn.getAttribute('data-range')||'all';
    var u=new URL(location.href);
    if(r==='all') u.searchParams.delete('range'); else u.searchParams.set('range', r);
    location.href=u.pathname+(u.search||'');
  }});
}});
}})();</script>
</body></html>"""




# --- Office status feed (Tailscale-only; ring buffer, 12h TTL) ---
OFFICE_TTL_SEC = 12 * 3600
OFFICE_STATUSES = ("working", "done", "idle", "waiting_user", "error")
_office_lock = threading.Lock()
_office_cond = threading.Condition(_office_lock)
_office_seq = 0
_office_events = []  # [{id, ts, bot, status, step}]


def _office_prune(now=None):
    now = time.time() if now is None else now
    cutoff = now - OFFICE_TTL_SEC
    # keep in place under caller lock
    i = 0
    while i < len(_office_events):
        if _office_events[i]["ts"] < cutoff:
            del _office_events[i]
        else:
            i += 1


def office_ingest(payload):
    """Accept dict or plain text; return stored event or raise ValueError."""
    global _office_seq
    if isinstance(payload, str):
        payload = payload.strip()
        if not payload:
            raise ValueError("empty body")
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            # ntfy-style plain: "bot|status|step" or just treat as step for unknown
            parts = [p.strip() for p in payload.split("|", 2)]
            if len(parts) >= 2:
                data = {"bot": parts[0], "status": parts[1], "step": parts[2] if len(parts) > 2 else ""}
            else:
                raise ValueError("plain body must be bot|status|step or JSON")
    elif isinstance(payload, dict):
        data = payload
    else:
        raise ValueError("unsupported body")
    bot = str(data.get("bot") or "").strip()
    status = str(data.get("status") or "").strip().lower()
    step = str(data.get("step") or "").strip()
    if not bot:
        raise ValueError("bot required")
    if len(bot) > 64:
        raise ValueError("bot name too long")
    if status == "clear":
        with _office_cond:
            _office_events[:] = [e for e in _office_events if e["bot"] != bot]
            _office_cond.notify_all()
        return {"cleared": bot}
    if status not in OFFICE_STATUSES:
        raise ValueError("status must be one of: " + ", ".join(OFFICE_STATUSES))
    if len(step) > 80:
        step = step[:80]
    now = time.time()
    with _office_cond:
        _office_prune(now)
        _office_seq += 1
        ev = {"id": _office_seq, "ts": now, "bot": bot, "status": status, "step": step}
        _office_events.append(ev)
        _office_cond.notify_all()
        return dict(ev)


def office_events_since(since_id=0, now=None):
    now = time.time() if now is None else now
    with _office_lock:
        _office_prune(now)
        return [dict(e) for e in _office_events if e["id"] > since_id]


def office_latest(now=None):
    now = time.time() if now is None else now
    with _office_lock:
        _office_prune(now)
        latest = {}
        for e in _office_events:
            latest[e["bot"]] = dict(e)
        return latest


def _codex_daily_deltas(days):
    """Per-ET-day Codex tokens from rollout token_count events: delta of total_token_usage between events,
    bucketed by event time. Returns (uncached, with_cached); uncached = input - cached_input + output."""
    unc, raw = {}, {}
    cutoff = time.time() - (len(days) + 1) * 86400
    for root, _d, files in os.walk(CODEX_SESSIONS):
        for name in files:
            if not (name.startswith("rollout-") and name.endswith(".jsonl")):
                continue
            p = os.path.join(root, name)
            try:
                if os.path.getmtime(p) < cutoff:
                    continue
                prev = None
                with open(p, errors="ignore") as f:
                    for line in f:
                        if "total_token_usage" not in line:
                            continue
                        try:
                            o = json.loads(line)
                            tu = ((o.get("payload") or {}).get("info") or {}).get("total_token_usage") or {}
                            cur = (int(tu.get("input_tokens") or 0), int(tu.get("cached_input_tokens") or 0), int(tu.get("output_tokens") or 0))
                            ts = datetime.fromisoformat(o["timestamp"].replace("Z", "+00:00")).astimezone().date().isoformat()
                        except Exception:
                            continue
                        dlt = cur if (prev is None or cur[0] < prev[0]) else tuple(a - b for a, b in zip(cur, prev))
                        prev = cur
                        if ts in days:
                            unc[ts] = unc.get(ts, 0) + max(0, dlt[0] - dlt[1]) + dlt[2]
                            raw[ts] = raw.get(ts, 0) + dlt[0] + dlt[2]
            except Exception:
                continue
    return unc, raw

def office2d_recent_payload(hours=168, limit=60):
    """Recent finished work for /office2d idle replays, from vault files already written
    by Spark jobs and ops notes (mtime + first heading). No model calls."""
    vault = Path.home() / "obsidian-vault" / "output"
    cutoff = time.time() - hours * 3600
    items = []
    def owner(kind, name):
        n = name.lower()
        if kind == "ops":
            return "Token CEO"
        if any(w in n for w in ("art", "boss-", "sprite", "comfy", "gallery", "style", "portrait", "tile")):
            return "Zonk Bot"
        return "Veil PM"
    for kind in ("ops", "jobs"):
        d = vault / kind
        if not d.is_dir():
            continue
        for fp in d.rglob("*.md"):
            try:
                mt = fp.stat().st_mtime
                if mt < cutoff or re.match(r"^\d{4}-\d{2}-\d{2}\.md$", fp.name):
                    continue
                title = ""
                with open(fp, errors="ignore") as f:
                    for line in f:
                        if line.startswith("#"):
                            title = line.lstrip("#").strip(); break
                if not title:
                    title = fp.stem.replace("-", " ")
                items.append({"bot": owner(kind, fp.name), "text": title[:70], "ts": mt})
            except Exception:
                continue
    items.sort(key=lambda x: -x["ts"])
    return {"items": items[:limit]}


def office2d_roster_payload():
    """Active bots for /office2d: this week's Grok per-bot share >= min_pct (from cloud_grok.json,
    written by the Grok dashboard routine) plus any bot with a live office status event.
    Floor comes from keyword rules in office2d/roles.json. No model calls."""
    base = Path(__file__).resolve().parent
    try:
        roles = json.loads((base / "office2d" / "roles.json").read_text())
    except Exception:
        roles = {}
    rules = roles.get("rules") or []
    descs = dict(roles.get("desc") or {})
    dflt = int(roles.get("default_floor") or 4)
    min_pct = float(roles.get("min_pct") or 2)
    try:
        g = json.loads((Path.home() / ".token-annihilator" / "cloud_grok.json").read_text())
    except Exception:
        g = {}
    active = {}
    for b in g.get("bots") or []:
        name = str(b.get("name") or "").strip()
        if not name:
            continue
        if b.get("desc"):
            descs[name] = str(b["desc"])
        try:
            pct = float(b.get("pct") or 0)
        except Exception:
            pct = 0.0
        if pct >= min_pct:
            active[name] = {"pct": pct, "why": "grok"}
    for name in office_latest().keys():
        active.setdefault(name, {"pct": None, "why": "status"})
    def floor_for(name):
        text = (name + " " + descs.get(name, "")).lower()
        for r in rules:
            if any(w in text for w in r.get("words") or []):
                return int(r.get("floor") or dflt)
        return dflt
    out = []
    for name, info in sorted(active.items(), key=lambda kv: -(kv[1]["pct"] or 0)):
        out.append({"name": name, "floor": floor_for(name), "id": re.sub(r"\W+", "-", name.lower()).strip("-"),
                    "pct": info["pct"], "why": info["why"]})
    return {"updated": g.get("updated"), "min_pct": min_pct, "bots": out}


def office2d_tokens_payload(n=7):
    """Read-only daily token series for /office2d finance screens. None = no data for that day."""
    from datetime import timedelta
    today = _today_et()
    days = [(today - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]
    local = None
    try:
        c = db()
        try:
            q = lambda sql, *a: c.execute(sql, a).fetchall()
            rows = devastation_day_rows(q, "source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local')", n)
        finally:
            c.close()
        m = {d: a + b for d, a, b in rows}
        local = [m.get(d, 0) for d in days]
    except Exception:
        local = None
    # cloud = Codex (max per-day "today" from cloud snapshots) + OpenCode cloud sessions (deepseek/other) per ET day.
    try:
        codex, codex_raw = _codex_daily_deltas(days)
    except Exception:
        codex, codex_raw = {}, {}
    oc = {}
    try:
        import sqlite3 as _sq
        if os.path.isfile(OPENCODE_DB):
            _k, start_ms, _e = _opencode_range_bounds_ms("monthly")
            con = _sq.connect("file:%s?mode=ro" % OPENCODE_DB, uri=True)
            try:
                rows = con.execute("SELECT model,time_created,COALESCE(tokens_input,0)+COALESCE(tokens_output,0)+COALESCE(tokens_reasoning,0) FROM session_v2 WHERE time_created>=?", (start_ms,)).fetchall()
            finally:
                con.close()
            for raw, tc, tot in rows:
                mid, pid = _opencode_model_parts(raw)
                if _opencode_bucket(mid, pid) not in ("deepseek", "opencode-other"):
                    continue
                d = datetime.fromtimestamp(tc / 1000).date().isoformat()
                if d in days:
                    oc[d] = oc.get(d, 0) + int(tot or 0)
    except Exception:
        pass
    cloud = [(codex[d] if d in codex else None) for d in days]
    cloud = [None if (v is None and d not in oc) else (v or 0) + oc.get(d, 0) for v, d in zip(cloud, days)]
    if all(v is None for v in cloud):
        cloud = None
    return {"days": days, "cloud": cloud, "local": local,
            "cloud_sources": "codex per-day deltas from rollouts, cached input excluded + opencode cloud sessions; grok excluded (plan % only, no token counts)",
            "codex_with_cached": [codex_raw.get(d) for d in days], "local_source": "stats.db requests source IN ('proxy','hermes-direct','eval-direct','strata-direct','opencode-local')", "now": time.time()}

def office_json_payload(since_sec=None):
    now = time.time()
    latest = office_latest(now)
    if since_sec is not None:
        cutoff = now - since_sec
        latest = {k: v for k, v in latest.items() if v["ts"] >= cutoff}
    return {"now": now, "ttl_sec": OFFICE_TTL_SEC, "bots": latest}




def office3d_root():
    return Path(__file__).resolve().parent / "office3d"


def office3d_safe_file(rel):
    """Resolve a path under office3d/; return Path or None if escape/missing."""
    root = office3d_root().resolve()
    rel = (rel or "").lstrip("/")
    if not rel or ".." in rel.split("/"):
        return None
    fp = (root / rel).resolve()
    if not str(fp).startswith(str(root)):
        return None
    if not fp.is_file():
        return None
    return fp


OFFICE3D_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".woff2": "font/woff2",
    ".woff": "font/woff",
    ".map": "application/json",
}

def page_office():
    # Simple own page (not Bodila51). Dark tokens matching Token Annihilator.
    return """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Office · Token Annihilator</title>
<style>
:root{--bg:#030505;--panel:#0a100c;--green:#39ff14;--green-bright:#c8ff9a;--muted:#a8d898;--label:#f0ffe8;
--warn:#ffcc00;--redline:#ff3d1a;--cyan:#7ef0ff;--line:#1a3a22;--line-strong:#2a5a32;--waiting:#ff9f1a}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%;background:var(--bg);color:var(--green);
font:14px/1.4 ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace}
header{display:flex;align-items:baseline;gap:16px;padding:18px 22px 8px;border-bottom:2px solid var(--line-strong)}
h1{margin:0;font-size:18px;letter-spacing:.12em;text-transform:uppercase;color:var(--green-bright)}
.sub{color:var(--muted);font-size:12px}
a{color:var(--muted)} a:hover{color:var(--green-bright)}
main{padding:18px 22px 40px;display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(260px,1fr))}
.card{background:var(--panel);border:2px solid var(--line);border-radius:8px;padding:14px 16px;min-height:110px}
.card.idle{opacity:.72}
.name{font-size:15px;font-weight:800;color:var(--cyan);margin:0 0 8px;word-break:break-word}
.chip{display:inline-block;font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;
padding:3px 8px;border-radius:3px;border:1px solid;margin-bottom:10px}
.chip.working{color:#041204;background:var(--green);border-color:var(--green-bright)}
.chip.done{color:#041204;background:var(--green-bright);border-color:var(--green)}
.chip.idle{color:var(--muted);background:#0a120c;border-color:var(--line)}
.chip.waiting_user{color:#1a1000;background:var(--waiting);border-color:var(--warn)}
.chip.error{color:#1a0500;background:var(--redline);border-color:var(--redline)}
.step{color:var(--label);font-size:13px;min-height:2.6em;word-break:break-word}
.age{color:var(--muted);font-size:11px;margin-top:10px}
.empty{grid-column:1/-1;color:var(--muted);padding:24px 0}
</style></head><body>
<header>
  <h1>Office</h1>
  <span class="sub">live bot status · 12h · <a href="/">dashboard</a> · <a href="/m">mobile</a></span>
</header>
<main id="grid"><div class="empty">waiting for status…</div></main>
<script>
const grid = document.getElementById('grid');
const bots = {};
function age(ts){
  const s = Math.max(0, Math.floor(Date.now()/1000 - ts));
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm ago';
}
function render(){
  const names = Object.keys(bots).sort((a,b)=>a.localeCompare(b));
  if (!names.length){ grid.innerHTML = '<div class="empty">no status in the last 12h</div>'; return; }
  grid.innerHTML = names.map(n=>{
    const e = bots[n];
    return '<div class="card '+e.status+'"><div class="name">'+esc(e.bot)+'</div>'
      +'<span class="chip '+e.status+'">'+esc(e.status)+'</span>'
      +'<div class="step">'+esc(e.step||'—')+'</div>'
      +'<div class="age" data-ts="'+e.ts+'">'+age(e.ts)+'</div></div>';
  }).join('');
}
function esc(s){ return String(s).replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function apply(e){ if(!e||!e.bot) return; bots[e.bot]=e; render(); }
function boot(snap){
  Object.keys(bots).forEach(k=>delete bots[k]);
  const map = (snap && snap.bots) || {};
  Object.keys(map).forEach(k=>bots[k]=map[k]);
  render();
}
fetch('/office/json').then(r=>r.json()).then(boot).catch(()=>{});
const es = new EventSource('/office/events');
es.addEventListener('status', ev=>{ try{ apply(JSON.parse(ev.data)); }catch(e){} });
es.addEventListener('clear', ev=>{ try{ const d=JSON.parse(ev.data); delete bots[d.bot]; render(); }catch(e){} });
es.onmessage = ev=>{ try{ apply(JSON.parse(ev.data)); }catch(e){} };
setInterval(()=>{ document.querySelectorAll('.age[data-ts]').forEach(el=>{ el.textContent = age(+el.dataset.ts); }); }, 5000);
</script></body></html>"""



class Dash(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _send(self, code, body, ctype, extra_headers=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _read_json_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return raw.decode("utf-8", errors="replace")

    def do_POST(self):
        path = (self.path or "/").split("?", 1)[0]
        if path == "/office/status":
            try:
                payload = self._read_json_body()
                ev = office_ingest(payload)
                self._send(200, json.dumps(ev), "application/json; charset=utf-8")
            except ValueError as e:
                self._send(400, json.dumps({"error": str(e)}), "application/json; charset=utf-8")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}), "application/json; charset=utf-8")
            return
        self._send(404, b"not found", "text/plain; charset=utf-8")

    def do_DELETE(self):
        path = (self.path or "/").split("?", 1)[0]
        if path == "/office/status":
            try:
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(self.path).query)
                bot = (qs.get("bot") or [""])[0].strip()
                if not bot:
                    body = self._read_json_body()
                    if isinstance(body, dict):
                        bot = str(body.get("bot") or "").strip()
                if not bot:
                    raise ValueError("bot required")
                ev = office_ingest({"bot": bot, "status": "clear"})
                self._send(200, json.dumps(ev), "application/json; charset=utf-8")
            except ValueError as e:
                self._send(400, json.dumps({"error": str(e)}), "application/json; charset=utf-8")
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}), "application/json; charset=utf-8")
            return
        self._send(404, b"not found", "text/plain; charset=utf-8")

    def _office_sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        last_id = 0
        # snapshot first
        try:
            for e in office_events_since(0):
                last_id = max(last_id, e["id"])
                line = "event: status\ndata: %s\n\n" % json.dumps(e)
                self.wfile.write(line.encode("utf-8"))
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return
        # stream new events up to ~10 minutes then let client reconnect
        deadline = time.time() + 600
        try:
            while time.time() < deadline:
                with _office_cond:
                    _office_cond.wait(timeout=15)
                    batch = [dict(e) for e in _office_events if e["id"] > last_id]
                if not batch:
                    try:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        return
                    continue
                for e in batch:
                    last_id = max(last_id, e["id"])
                    line = "event: status\ndata: %s\n\n" % json.dumps(e)
                    self.wfile.write(line.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_GET(self):
        path = (self.path or "/").split("?", 1)[0]
        font_map = {
            "/font/dseg7.woff2": ("DSEG7Classic-BoldItalic.woff2", "font/woff2"),
            "/font/dseg7.ttf": ("DSEG7Classic-BoldItalic.ttf", "font/ttf"),
        }
        if path in font_map:
            name, ctype = font_map[path]
            fp = Path(__file__).resolve().parent / name
            try:
                b = fp.read_bytes(); code = 200
            except Exception as e:
                b = ("font missing: %s" % e).encode(); code = 404; ctype = "text/plain; charset=utf-8"
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers(); self.wfile.write(b); return
        if path == "/api/speed":
            try:
                from urllib.parse import urlparse, parse_qs
                _rk = (parse_qs(urlparse(self.path).query).get("range") or ["all"])[0]
                payload = json.dumps(speed_payload(_rk)).encode(); code = 200
            except Exception as e:
                payload = json.dumps({"error": str(e)}).encode(); code = 500
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers(); self.wfile.write(payload); return
        if path == "/api/stats":
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            rng = (qs.get("range") or ["all"])[0]
            cl = (qs.get("cloud") or ["0"])[0] in ("1", "true", "on")
            try:
                payload = json.dumps(stats_payload(rng, cloud=cl)).encode()
                code = 200
            except Exception as e:
                payload = json.dumps({"error": str(e)}).encode(); code = 500
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers(); self.wfile.write(payload); return

        if path == "/office3d" or path == "/office3d/" or path == "/office3d/index.html":
            # 3D office retired Nov 11 2026 (Michael); files kept, page redirects to 2D.
            self.send_response(302); self.send_header("Location", "/office2d"); self.send_header("Content-Length", "0"); self.end_headers(); return
        if False:
            fp = office3d_safe_file("index.html")
            if fp is None:
                self._send(404, b"office3d missing - generate index.html first", "text/plain; charset=utf-8"); return
            b = fp.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers(); self.wfile.write(b); return
        if path.startswith("/office3d/"):
            rel = path[len("/office3d/"):]
            fp = office3d_safe_file(rel)
            if fp is None:
                self._send(404, b"not found", "text/plain; charset=utf-8"); return
            ctype = OFFICE3D_TYPES.get(fp.suffix.lower(), "application/octet-stream")
            b = fp.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            if "/vendor/" in path:
                self.send_header("Cache-Control", "public, max-age=86400")
            else:
                self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers(); self.wfile.write(b); return

        if path == "/office2d/recent.json":
            b = json.dumps(office2d_recent_payload()).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Cache-Control", "no-store"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
        if path == "/office2d/roster.json":
            b = json.dumps(office2d_roster_payload()).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Cache-Control", "no-store"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
        if path == "/office2d/tokens.json":
            b = json.dumps(office2d_tokens_payload()).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json; charset=utf-8"); self.send_header("Cache-Control", "no-store"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
        if path == "/office2d" or path.startswith("/office2d/"):
            root = (Path(__file__).resolve().parent / "office2d").resolve()
            rel = path[len("/office2d"):].lstrip("/") or "index.html"
            fp = (root / rel).resolve()
            if ".." in rel.split("/") or not str(fp).startswith(str(root)) or not fp.is_file():
                self._send(404, b"not found", "text/plain; charset=utf-8"); return
            ctype = {".html":"text/html; charset=utf-8",".js":"application/javascript; charset=utf-8",".ttf":"font/ttf",".css":"text/css"}.get(fp.suffix.lower(), "application/octet-stream")
            b = fp.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "public, max-age=86400" if fp.suffix == ".ttf" else "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers(); self.wfile.write(b); return
        if path == "/office" or path == "/office/":
            try:
                b = page_office().encode(); code = 200
            except Exception as e:
                b = ("error: %s" % e).encode(); code = 500
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers(); self.wfile.write(b); return
        if path == "/office/json":
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            since = (qs.get("since") or [None])[0]
            since_sec = None
            if since:
                m = re.match(r"^(\d+)(s|m|h)?$", since.strip())
                if m:
                    n = int(m.group(1))
                    unit = m.group(2) or "s"
                    since_sec = n * ({"s": 1, "m": 60, "h": 3600}[unit])
            try:
                payload = json.dumps(office_json_payload(since_sec)).encode()
                code = 200
            except Exception as e:
                payload = json.dumps({"error": str(e)}).encode(); code = 500
            self._send(code, payload, "application/json; charset=utf-8"); return
        if path == "/office/events":
            self._office_sse(); return
        try:
            if path in ("/m", "/mobile"):
                b = page_mobile().encode(); code = 200
            elif path in ("/cloud/m", "/cloud/mobile"):
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(self.path).query)
                rk = (qs.get("range") or ["all"])[0]
                b = page_cloud(mobile=True, range_key=rk).encode(); code = 200
            elif path == "/cloud":
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(self.path).query)
                mob = (qs.get("m") or [""])[0] in ("1", "true", "yes")
                rk = (qs.get("range") or ["all"])[0]
                b = page_cloud(mobile=mob, range_key=rk).encode(); code = 200
            else:
                b = page().encode(); code = 200
        except Exception as e: b = ("error: %s" % e).encode(); code = 500
        self.send_response(code); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)


class TS(ThreadingMixIn, http.server.HTTPServer): daemon_threads = True; allow_reuse_address = True

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Token Annihilation Dashboard")
    ap.add_argument("--proxy-bind", default=PROXY_BIND); ap.add_argument("--proxy-port", type=int, default=PROXY_PORT)
    ap.add_argument("--dash-bind", default=DASH_BIND); ap.add_argument("--dash-port", type=int, default=DASH_PORT)
    ap.add_argument("--backfill", nargs="?", const="", default=None, metavar="UNIX_TS_CUTOFF")
    a = ap.parse_args()
    PROXY_BIND, PROXY_PORT, DASH_BIND, DASH_PORT = a.proxy_bind, a.proxy_port, a.dash_bind, a.dash_port
    db().close()
    if a.backfill is not None:
        backfill(float(a.backfill) if a.backfill else None); sys.exit()
    threading.Thread(target=TS((DASH_BIND, DASH_PORT), Dash).serve_forever, daemon=True).start()
    print(f"proxy {PROXY_BIND}:{PROXY_PORT} -> {UP_HOST}:{UP_PORT}; dashboard http://{DASH_BIND}:{DASH_PORT}", flush=True)
    TS((PROXY_BIND, PROXY_PORT), Proxy).serve_forever()
