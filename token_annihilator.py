from pathlib import Path
#!/usr/bin/env python3
"""Token Annihilation Dashboard: transparent Ollama token-counting proxy + tiny dashboard. Stdlib only."""
import http.client, http.server, json, math, os, re, sqlite3, subprocess, sys, threading, time, html
from datetime import datetime
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
DASH_BIND, DASH_PORT = os.environ.get("DASH_BIND") or _tailscale_ip(), int(os.environ.get("DASH_PORT", 11480))
TRACK = ("/api/generate", "/api/chat", "/v1/chat/completions", "/v1/completions", "/v1/responses", "/api/embed", "/api/embeddings", "/v1/embeddings")
HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "proxy-connection"}
lock = threading.Lock()

def db():
    c = sqlite3.connect(DB, timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY, ts REAL, model TEXT, endpoint TEXT,
      input_tokens INTEGER, output_tokens INTEGER, duration_s REAL, eval_s REAL, status INTEGER, source TEXT, dedupe TEXT UNIQUE)""")
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

def calendar(c):
    from datetime import date, timedelta
    rows = c.execute("SELECT date(ts,'unixepoch','localtime'), SUM(COALESCE(input_tokens,0)+COALESCE(output_tokens,0)) FROM requests WHERE source='proxy' GROUP BY 1").fetchall()
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

def months_join(m):
    return "".join(m)

def page():
    c = db(); q = lambda s, *a: c.execute(s, a).fetchall()
    P = "source='proxy'"
    tin, tout, nreq = q(f"SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),COUNT(*) FROM requests WHERE {P}")[0]
    tps = q(f"SELECT SUM(output_tokens)/SUM(eval_s) FROM requests WHERE {P} AND eval_s>0 AND output_tokens>0")[0][0]
    days = q(f"SELECT date(ts,'unixepoch','localtime') d, SUM(COALESCE(input_tokens,0)), SUM(COALESCE(output_tokens,0)) FROM requests WHERE {P} GROUP BY d ORDER BY d DESC LIMIT 8")
    models = q(f"SELECT COALESCE(model,'?'), COUNT(*), SUM(COALESCE(input_tokens,0)+COALESCE(output_tokens,0)) t FROM requests WHERE {P} GROUP BY 1 ORDER BY t DESC LIMIT 6")
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
    _medal = {0: "#1 champion", 1: "#2 contender", 2: "#3 challenger"}
    modrows = "".join(
        "<div class='card hof'><div class=lbl>%s</div><div class=mname>%s</div>"
        "<div class='big dseg seg' data-val='%s' data-ghost='%s'>%s</div>"
        "<div class=muted>%s requests · %.0f%% of total</div></div>"
        % (_medal.get(i, "#%d" % (i + 1)), html.escape(m), fmt(t), ghost_digits(fmt(t)), fmt(t), fmt(n), 100 * (t or 0) / _tot)
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
        sav += (
            "<div class='card bill'><div class=bill-row>"
            "<div class='big dseg seg' data-val='%s' data-ghost='%s'>%s</div>"
            "<div class=bill-meta><div class=lbl title='%s'>Kept from %s</div>"
            "<div class=muted>$%s/$%s per M in/out</div></div></div></div>"
            % (mv, ghost_digits(mv), mv, html.escape(p["label"], quote=True), html.escape(p["label"]),
               p["input_per_mtok"], p["output_per_mtok"])
        )

    tps_v = float(tps) if tps else 0.0
    tps_max = 50.0
    tot = tin + tout
    tps_disp = ("%.1f" % tps_v) if tps else "-"
    tach = seg_tach(36, tps_v / tps_max, True)
    tach_scale = "".join("<span>%d</span>" % v for v in (0, 10, 20, 30, 40, 50))
    lights = [("IGN", True, "green"), ("PROXY", True, "green"), (str(PROXY_PORT), True, "green"),
              ("LOCAL ONLY", True, "green"), ("CLOUD", False, "amber")]
    lights_html = "".join('<span class="wl %s%s">%s</span>' % (c, " on" if on else "", lab) for lab, on, c in lights)

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
  flex:1;min-height:0;width:100%;overflow:hidden;
  display:grid;gap:clamp(5px,.5vh,8px);
  grid-template-rows:auto auto auto minmax(0,1.1fr) minmax(0,1.05fr) minmax(0,1.05fr) auto minmax(0,1.2fr) auto;
  border:3px solid var(--line-strong);border-radius:10px;
  padding:clamp(8px,.7vh,12px) clamp(10px,.85vw,14px);
  background:linear-gradient(180deg,#07100a,#030505);
  box-shadow:inset 0 0 24px #39ff1412,0 0 12px #39ff1420;
}}
.hdr{{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;}}
h1{{margin:0;color:var(--green-bright);letter-spacing:.08em;text-transform:uppercase;
  font-size:clamp(14px,1.55vw,20px);font-weight:800;
  text-shadow:0 0 5px rgba(57,255,20,.35),0 0 1px rgba(200,255,154,.55);}}
.wl-row{{display:flex;flex-wrap:wrap;gap:6px;justify-content:flex-end;}}
.wl{{font-size:clamp(11px,.85vw,13px);letter-spacing:.08em;padding:3px 9px;border:2px solid #333;color:#777;
  background:#0a0a0a;border-radius:3px;text-transform:uppercase;font-weight:700;}}
.wl.green.on{{color:#041204;background:var(--green);border-color:var(--green);
  box-shadow:0 0 6px rgba(57,255,20,.5),inset 0 0 5px rgba(255,255,255,.15);}}
.wl.amber{{border-color:#5a3a00;color:#9a7a30;}}
.tagline{{color:var(--muted);font-size:clamp(11px,.9vw,13px);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.hero{{
  display:flex;flex-direction:column;align-items:center;justify-content:flex-start;gap:clamp(4px,.4vh,8px);
  padding:clamp(10px,1vh,16px) clamp(8px,.8vh,14px) clamp(6px,.6vh,12px);
  border:3px solid var(--line-strong);border-radius:10px;background:#050a07;
  box-shadow:inset 0 0 18px #39ff1418;overflow:hidden;flex-shrink:0;
}}
.hero .odo-lbl,.hero .odo-unit{{font-size:clamp(12px,1.05vw,16px);color:var(--label);letter-spacing:.18em;font-weight:800;
  margin:0 0 clamp(2px,.2vh,4px);padding:0;line-height:1.2;position:relative;z-index:2;
  background:transparent;flex-shrink:0;}}
.hero .odo{{
  position:relative;display:inline-block;isolation:isolate;overflow:hidden;flex-shrink:0;
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
.card.hof{{padding:clamp(6px,.45vh,12px) clamp(8px,.65vw,14px);gap:clamp(1px,.2vh,5px);}}
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
.card.hof .big{{font-size:clamp(12px,1.55vw,28px);}}
.card.hof .big.dseg{{padding:.32em .14em .42em;line-height:1.6;}}
.card.speed .big{{font-size:clamp(18px,2.6vw,42px);line-height:1.35;}}
.seg{{font-variant-numeric:tabular-nums;}}
.lbl{{color:var(--label);font-size:clamp(13px,1.15vw,16px);letter-spacing:.03em;margin:0 0 clamp(6px,.45vh,10px);font-weight:800;
  line-height:1.3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.muted{{color:var(--muted);font-size:clamp(11px,.9vw,13px);line-height:1.35;margin-top:clamp(6px,.45vh,10px);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.mname{{font-size:clamp(14px,1.25vw,18px);font-weight:800;color:var(--cyan);margin:0 0 clamp(6px,.5vh,10px);word-break:break-all;
  line-height:1.3;text-shadow:0 0 4px rgba(126,240,255,.3);}}
.card.hof .mname{{font-size:clamp(15px,1.4vw,20px);margin-bottom:clamp(8px,.55vh,12px);}}
.tach-track{{display:flex;align-items:flex-end;gap:2px;height:clamp(28px,5vh,52px);border-bottom:2px solid var(--line-strong);width:100%;}}
.card.speed .tach-track{{height:clamp(28px,6.5vh,64px);gap:3px;border-bottom-width:3px;flex-shrink:0;}}
.day-tach{{height:clamp(7px,1.05vh,11px);gap:1px;border-bottom-width:1px;align-items:stretch;}}
.day-tach i{{height:100% !important;border-radius:1px;min-width:1px;}}
.panel.daily{{gap:clamp(2px,.25vh,5px);padding:clamp(5px,.4vh,8px) clamp(8px,.6vw,11px);}}
.panel.daily table.day-log{{height:auto;}}
.panel.daily td{{padding:clamp(2px,.2vh,4px) clamp(4px,.4vw,7px);}}
.panel.daily td.day-col{{width:16%}} .panel.daily td.gauge-col{{width:54%}} .panel.daily td.seg{{width:15%;font-size:clamp(12px,1.1vw,16px);}}
.panel.daily td.day-io{{
  font-family:ui-monospace,"Cascadia Mono","SF Mono",Menlo,Consolas,monospace;
  font-weight:400;letter-spacing:0;text-shadow:none;color:var(--muted);
  font-size:clamp(12px,1.05vw,15px);line-height:1.25;
}}
.tach-track i{{display:block;flex:1;min-width:2px;border-radius:1px 1px 0 0;border:1px solid transparent;}}
.seg-on{{background:var(--green);border-color:#6dff4a;box-shadow:0 0 5px rgba(57,255,20,.45),inset 0 0 3px rgba(255,255,255,.18);}}
.seg-on.warn{{background:var(--warn);border-color:#ffe066;box-shadow:0 0 5px rgba(255,204,0,.45),inset 0 0 3px rgba(255,255,255,.18);}}
.seg-on.redline{{background:var(--redline);border-color:#ff8a6a;box-shadow:0 0 5px rgba(255,61,26,.5),inset 0 0 3px rgba(255,255,255,.18);}}
.seg-off{{background:var(--unlit);border-color:var(--unlit-edge);}}
.tach-scale{{display:flex;justify-content:space-between;font-size:clamp(10px,.75vw,12px);color:var(--muted);font-weight:700;}}
table{{width:100%;border-collapse:collapse;font-size:clamp(12px,1.05vw,15px);table-layout:fixed;height:100%;}}
td{{padding:clamp(4px,.35vh,6px) clamp(5px,.45vw,8px);border-bottom:2px solid var(--line);
  color:var(--green-mid);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:middle;}}
td.day-col{{width:18%}} td.gauge-col{{width:50%}} td.seg{{width:16%;font-size:clamp(14px,1.3vw,18px);}}
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
.foot{{text-align:center;color:var(--muted);font-size:clamp(11px,.85vw,13px);letter-spacing:.04em;flex-shrink:0;}}
.bill-grid{{grid-template-columns:repeat(auto-fit,minmax(min(180px,100%),1fr));height:auto;align-content:stretch;}}
.card.bill{{justify-content:center;padding:clamp(5px,.4vh,9px) clamp(6px,.5vw,9px);overflow:hidden;}}
.bill-row{{display:flex;align-items:center;gap:clamp(5px,.45vw,9px);min-width:0;width:100%;}}
.bill-row .big{{flex:0 0 auto;margin:0;font-size:clamp(13px,1.45vw,22px);}}
.bill-row .big.dseg{{padding:.12em .08em .14em;margin:0;line-height:1.35;}}
.bill-meta{{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;gap:1px;justify-content:center;}}
.bill-meta .lbl{{white-space:normal;overflow:hidden;text-overflow:ellipsis;margin:0;font-size:clamp(9px,.8vw,12px);line-height:1.2;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;}}
.bill-meta .muted{{white-space:normal;overflow:hidden;text-overflow:ellipsis;margin:0;font-size:clamp(8px,.7vw,11px);line-height:1.2;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;}}
.hof-grid{{grid-template-columns:repeat(auto-fit,minmax(min(160px,100%),1fr));height:auto;overflow:visible;}}
.card.hof .mname{{margin-bottom:clamp(1px,.15vh,5px);flex-shrink:1;min-height:0;}}
.card.hof{{justify-content:flex-start;}}
.card.hof .lbl{{flex-shrink:0;margin-bottom:clamp(1px,.15vh,4px);}}
@media(max-width:900px){{
  .grid{{grid-template-columns:repeat(2,minmax(0,1fr));}}
  .card.speed{{grid-column:span 2;}}
}}
</style></head><body><div class=cluster>
<div class=hdr>
  <h1>{T}</h1>
  <div class=wl-row>{lights_html}</div>
</div>
<div class=tagline>Every token counted by the proxy on port {PROXY_PORT} is a token the cloud will never bill you for. Last carnage report: {datetime.now():%b %d %H:%M}.</div>

<div class=hero>
  <div class=odo-lbl>TOTAL TOKENS ANNIHILATED</div>
  <div class="odo dseg seg" data-val="{fmt(tot)}" data-ghost="{ghost_digits(fmt(tot))}">{fmt(tot)}</div>
</div>

<div class="panel body">
  <h2>Body count</h2>
  <div class=grid>
    <div class=card><div class=lbl>Tokens annihilated</div><div class="big dseg seg" data-val="{fmt(tot)}" data-ghost="{ghost_digits(fmt(tot))}">{fmt(tot)}</div></div>
    <div class=card><div class=lbl>Prompts devoured (input)</div><div class="big dseg seg" data-val="{fmt(tin)}" data-ghost="{ghost_digits(fmt(tin))}">{fmt(tin)}</div></div>
    <div class=card><div class=lbl>Words unleashed (output)</div><div class="big dseg seg" data-val="{fmt(tout)}" data-ghost="{ghost_digits(fmt(tout))}">{fmt(tout)}</div></div>
    <div class=card><div class=lbl>Requests vanquished</div><div class="big dseg seg" data-val="{fmt(nreq)}" data-ghost="{ghost_digits(fmt(nreq))}">{fmt(nreq)}</div></div>
    <div class="card speed">
      <div class=lbl>Annihilation speed (out tok/s)</div>
      <div class=speed-row>
        <div class=speed-tach>
          <div class=tach-track>{tach}</div>
          <div class=tach-scale>{tach_scale}</div>
        </div>
        <div class=speed-readout>
          <div class="big dseg seg" data-val="{tps_disp}" data-ghost="{ghost_digits(tps_disp)}">{tps_disp}</div>
          <div class=speed-unit>tok/s</div>
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
  <table class=day-log><tr class=muted><td>Day</td><td></td><td>In</td><td>Out</td></tr>{dayrows}</table>
</div>

<div class=panel>
  <h2>Hall of fame: hungriest models</h2>
  <div class="grid hof-grid">{modrows}</div>
</div>

<p class=foot>No GPUs were harmed. Several cloud invoices were.</p>
</div>
<script>var t=document.getElementById('tip');document.querySelectorAll('rect[data-t]').forEach(function(r){{r.onmousemove=function(e){{t.textContent=r.dataset.t;t.style.display='block';t.style.left=(e.clientX+12)+'px';t.style.top=(e.clientY+12)+'px'}};r.onmouseleave=function(){{t.style.display='none'}}}});</script>
</body></html>"""

class Dash(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
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
        try: b = page().encode(); code = 200
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
