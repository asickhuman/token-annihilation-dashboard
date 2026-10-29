# Token Annihilation Dashboard — Ollama routing

**Date:** 2026-10-28  
**Goal:** All token-using **Ollama** traffic on the Spark goes through the counting proxy.

| Role | Address |
|------|---------|
| Ollama (unchanged) | `127.0.0.1:11434` |
| Proxy (token-annihilator) | `127.0.0.1:11435` → forwards to `:11434` |
| Dashboard | `http://100.126.27.23:11480` |
| Service | `systemctl --user status token-annihilator` |

## One-line revert

```bash
# restore every file edited for this routing change
for f in \
  ~/artqc/config.yaml \
  ~/artqc/config.after-calib.yaml \
  ~/evals/harness.py \
  ~/nightly/jobs/tonight/common.sh \
  ~/nightly/jobs/tonight/qwen_edit_harness.py \
  ~/nightly/jobs/ui-reskin.sh \
  ~/nightly/work/ui-pass2/run_pass2.py \
  ~/nightly/jobs/flash-vs-27b.md \
  ~/.codex/config.toml \
  ~/.codex/ollama-launch.config.toml \
  ~/.bashrc \
  ~/.profile \
  ~/token-annihilation-dashboard/token_annihilator.py
 do
  [ -f "${f}.bak-tad" ] && cp -a "${f}.bak-tad" "$f" && echo "restored $f"
 done
rm -f ~/.config/environment.d/99-ollama-proxy.conf
systemctl --user unset-environment OLLAMA_HOST || true
systemctl --user restart token-annihilator
# then open a new shell (or unset OLLAMA_HOST) so CLI clients hit :11434 again
```

## Files changed (backups: `*.bak-tad`)

### Client configs → `:11435`

| File | Change |
|------|--------|
| `~/artqc/config.yaml` | `ollama.base_url` → `http://127.0.0.1:11435` |
| `~/artqc/config.after-calib.yaml` | same |
| `~/evals/harness.py` | default `OLLAMA_API` → `http://127.0.0.1:11435/api/chat` |
| `~/nightly/jobs/tonight/common.sh` | `API` + `/api/ps` curls → `:11435` |
| `~/nightly/jobs/tonight/qwen_edit_harness.py` | `API` → `:11435` |
| `~/nightly/jobs/ui-reskin.sh` | bash + embedded python `API` → `:11435` |
| `~/nightly/work/ui-pass2/run_pass2.py` | `API` → `:11435` (running process still on old URL until restart) |
| `~/nightly/jobs/flash-vs-27b.md` | doc table Ollama URL → `:11435` |
| `~/.codex/config.toml` | added `[model_providers.ollama] base_url = "http://127.0.0.1:11435/v1"` (`wire_api = "responses"`). **Built-in `openai` provider untouched** (GPT Codex / cloud). |
| `~/.codex/ollama-launch.config.toml` | `base_url` → `http://127.0.0.1:11435/v1/` |

### Environment

| File | Change |
|------|--------|
| `~/.bashrc` | `export OLLAMA_HOST=127.0.0.1:11435` |
| `~/.profile` | same |
| `~/.config/environment.d/99-ollama-proxy.conf` | `OLLAMA_HOST=127.0.0.1:11435` (systemd user services inherit after login / `import-environment`) |

Imported into the current user systemd session with `systemctl --user import-environment OLLAMA_HOST`.

### Proxy itself

| File | Change |
|------|--------|
| `~/token-annihilation-dashboard/token_annihilator.py` | TRACK also includes `/v1/responses` (Codex Responses API). Usage parse also accepts `input_tokens` / `output_tokens`. Service restarted. |

**Ollama binary/port and ComfyUI were not changed.**

## Streaming / usage

- Native Ollama `/api/generate` and `/api/chat` always return token counts (`prompt_eval_count` / `eval_count`) when `stream: false` (our harnesses use that).
- OpenAI-compatible streaming clients must send `"stream_options": {"include_usage": true}` or the proxy records the request with null token counts. Codex uses the Responses API; `/v1/responses` is now tracked.

## Not routable through this proxy

### Flash Next (TensorFold / docker on `:8888`)

Flash Next is **not** served by Ollama. The annihilator only upstreams to `127.0.0.1:11434`.  
Tonight's eval (`~/evals/run_flash_vs_27b.sh`) still uses:

- Flash: `EVAL_BACKEND=openai EVAL_BASE_URL=http://127.0.0.1:8888/v1`
- 27b: Ollama via harness default (now `:11435`) ← **counted**

**Options if Flash tokens should be counted too:**

1. **Extend the dashboard** to a second upstream (e.g. path prefix or Host-based routing to `:8888`) and point `EVAL_BASE_URL` at the proxy — best long-term.
2. **Separate lightweight counter** in the harness that logs Flash `usage` into the same SQLite DB (`source='flash'`).
3. **Leave Flash uncounted** (speed/quality eval only); keep annihilator Ollama-only.

### GPT Codex (cloud `model_provider=openai`)

Intentionally **not** routed. Nightly GPT fallbacks use `-c model_provider="openai"` → api.openai.com. Cloud traffic cannot go through this local proxy.

### ComfyUI

No installed Ollama custom nodes calling `:11434` (only a catalog mention in ComfyUI-Manager). Image gen does not use Ollama tokens.

### `ollama` CLI management

`ollama pull` / `list` / `serve` talk to `OLLAMA_HOST`. With the env set they hit the proxy; non-TRACK paths (`/api/tags`, `/api/pull`, …) still forward transparently and are not counted. **Do not** point `ollama serve` itself at `:11435`.

## Running jobs at cutover

| Job | Effect |
|-----|--------|
| `calibrate_v2` (BEFORE) | Had loaded old `config.yaml` (`:11434`). Interrupted mid-run around verify; **resumed via watchdog** from JSONL checkpoint — new process reads `:11435`. |
| `ui-pass2/run_pass2.py` | Still in-memory on `:11434` until that process exits; file on disk is updated for next launch. |
| Turret SE judge | Started after config edit → already on `:11435` (visible in stats). |
| ComfyUI / `ollama serve` | Untouched. |

## Verification (2026-10-28)

Tiny calls through `:11435` increased SQLite `requests` with `source='proxy'`:

- `/api/generate` `qwen2.5vl:32b` → 24 in / 2 out  
- `/api/chat` `qwen3-coder-next` → 13 in / 2 out  
- Concurrent VL `/api/chat` from artqc judge also counted  

Check live: `sqlite3 ~/token-annihilation-dashboard/stats.db "SELECT COUNT(*), SUM(input_tokens), SUM(output_tokens) FROM requests WHERE source='proxy';"`  
or open the dashboard URL above.
