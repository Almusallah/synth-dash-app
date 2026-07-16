"""Status dashboard for the synthetic-trader bot.

The bot's Mac pushes a JSON snapshot to POST /api/push (Bearer SYNC_TOKEN);
the app keeps it in memory and mirrors it to /tmp/synth_state.json so a
restart reloads the last known state. GET / renders a single dark-theme page
with zero frontend dependencies: all HTML, CSS and the equity chart (inline
SVG polyline) are produced server-side. Every section tolerates missing or
partial snapshot data.

A built-in cloud medic (daemon thread, every 15 minutes) triages the stored
state 24/7 — no LLM, no external deps — and can trigger ONE rate-limited
worker restart via env SYNTH_DEPLOY_HOOK. The Mac medic (scripts/medic.py)
still POSTs richer reports (with LLM diagnosis) to /api/medic; those
overwrite the cloud report and are labelled source "mac".
"""
from __future__ import annotations

import hmac
import html
import json
import os
import re
import threading
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request

STATE_PATH = Path(os.environ.get("SYNTH_STATE_PATH", "/tmp/synth_state.json"))
MAX_SERIES = 2880        # ~2 days of 1/min equity points
OFFLINE_AFTER_S = 600    # last sync older than this -> "BOT OFFLINE?"
TOKEN_WARN_DAYS = 7
APP_START = time.time()

app = Flask(__name__)

# snapshot: last pushed payload; series: [[ts, equity], ...] accumulated
# across pushes (or replaced wholesale if the push carries equity_series);
# coach: last daily brief posted by scripts/coach.py (survives snapshot pushes);
# medic: last health report — written by the in-process cloud medic loop and
# overwritten by scripts/medic.py POSTs (survives pushes too);
# medic_cloud: cloud medic's restart rate-limiter state (last_restart_ts).
_state: dict[str, Any] = {"snapshot": {}, "series": [], "received_at": None,
                          "coach": None, "medic": None, "medic_cloud": {}}


# ---------------------------------------------------------------- utilities
def _num(v: Any, default: float | None = None) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _rows(v: Any) -> list[dict]:
    return [r for r in v if isinstance(r, dict)] if isinstance(v, list) else []


def esc(v: Any) -> str:
    return html.escape(str(v), quote=True) if v is not None else ""


def _usd(v: float | None, signed: bool = False) -> str:
    if v is None:
        return "—"
    sign = "+" if signed and v > 0 else ""
    return f"{sign}${v:,.2f}" if v >= 0 else f"-${abs(v):,.2f}"


def _age(seconds: float | None) -> str:
    if seconds is None:
        return "never"
    s = int(max(0.0, seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    if s < 86400:
        return f"{s // 3600}h {s % 3600 // 60}m"
    return f"{s // 86400}d {s % 86400 // 3600}h"


def _fmt_ts(v: Any) -> str:
    """Epoch seconds or preformatted string -> short display timestamp."""
    n = _num(v)
    if n is not None and n > 1e9:
        return datetime.fromtimestamp(n, tz=timezone.utc).strftime("%m-%d %H:%M")
    return esc(str(v)[:16]) if v not in (None, "") else "—"


def _pnl_cls(v: float | None) -> str:
    if v is None:
        return "mut"
    return "pos" if v > 0 else "neg" if v < 0 else "mut"


def _veto_counts(snap: dict) -> tuple[int, int]:
    """(vetoed, total) symbols in the analyst bias map; (0, 0) if absent."""
    analyst = snap.get("analyst") if isinstance(snap.get("analyst"), dict) else {}
    bias = analyst.get("bias") if isinstance(analyst.get("bias"), dict) else {}
    return sum(1 for b in bias.values() if str(b).lower() == "veto"), len(bias)


def _authed() -> bool:
    """Constant-time Bearer check against SYNC_TOKEN (shared by all API routes)."""
    token = os.environ.get("SYNC_TOKEN", "")
    supplied = request.headers.get("Authorization", "")
    return bool(token) and hmac.compare_digest(supplied, f"Bearer {token}")


# ------------------------------------------------------------- persistence
def _load_state() -> None:
    try:
        data = json.loads(STATE_PATH.read_text())
    except (OSError, ValueError):
        return
    if isinstance(data, dict) and isinstance(data.get("snapshot"), dict):
        series = data.get("series")
        _state["snapshot"] = data["snapshot"]
        _state["received_at"] = _num(data.get("received_at"))
        _state["series"] = [
            p for p in (series if isinstance(series, list) else [])
            if isinstance(p, list) and len(p) == 2 and _num(p[1]) is not None
        ][-MAX_SERIES:]
        _state["coach"] = data["coach"] if isinstance(data.get("coach"), dict) else None
        _state["medic"] = data["medic"] if isinstance(data.get("medic"), dict) else None
        _state["medic_cloud"] = (data["medic_cloud"]
                                 if isinstance(data.get("medic_cloud"), dict) else {})


_SAVE_LOCK = threading.Lock()  # push handler and medic thread both save


def _save_state() -> None:
    tmp = STATE_PATH.with_suffix(".json.tmp")
    try:
        with _SAVE_LOCK:
            tmp.write_text(json.dumps(_state))
            tmp.replace(STATE_PATH)
    except OSError:
        pass  # disk mirror is best-effort; the in-memory copy still serves


# --------------------------------------------------------------- API routes
@app.post("/api/push")
def push():
    if not _authed():
        return jsonify(error="bad or missing bearer token"), 401
    snap = request.get_json(silent=True)
    if not isinstance(snap, dict):
        return jsonify(error="body must be a JSON object"), 400

    _state["snapshot"] = snap
    _state["received_at"] = time.time()
    pushed = snap.get("equity_series")
    if isinstance(pushed, list) and pushed:
        # MERGE with the stored series (union by timestamp) instead of
        # replacing it: the cloud worker's journal is ephemeral, so its first
        # push after a restart carries only a couple of points — a straight
        # replace wiped the whole chart history (2026-07-16 incident).
        merged = {p[0]: p[1] for p in _state["series"]}
        for p in pushed:
            if isinstance(p, (list, tuple)) and len(p) >= 2:
                ts, eq = _num(p[0], 0.0), _num(p[1])
            elif isinstance(p, dict):
                ts, eq = _num(p.get("ts"), 0.0), _num(p.get("equity"))
            else:
                continue
            if eq is not None:
                merged[ts] = eq
        _state["series"] = [[t, merged[t]] for t in sorted(merged)][-MAX_SERIES:]
    else:
        eq = _num(snap.get("equity"))
        ts = _num(snap.get("ts")) or _state["received_at"]
        if eq is not None and (not _state["series"] or _state["series"][-1][0] != ts):
            _state["series"].append([ts, eq])
            del _state["series"][:-MAX_SERIES]
    _save_state()
    return jsonify(ok=True, points=len(_state["series"]))


@app.get("/api/state")
def get_state():
    """Last stored snapshot, for offline consumers (e.g. the daily coach)."""
    if not _authed():
        return jsonify(error="bad or missing bearer token"), 401
    snap = _state.get("snapshot")
    if not isinstance(snap, dict) or not snap:
        return jsonify(error="no snapshot stored yet"), 404
    return jsonify(snap)


@app.post("/api/coach")
def post_coach():
    """Store the coach's daily brief (rendered on the page, survives pushes)."""
    if not _authed():
        return jsonify(error="bad or missing bearer token"), 401
    brief = request.get_json(silent=True)
    if not isinstance(brief, dict):
        return jsonify(error="body must be a JSON object"), 400
    _state["coach"] = brief
    _save_state()
    return jsonify(ok=True)


@app.post("/api/medic")
def post_medic():
    """Store the medic's health report (rendered in the Ops Panel, survives pushes).

    Mac medic reports (which carry an LLM diagnosis) overwrite the cloud
    medic's report; they are tagged source "mac" unless they say otherwise.
    """
    if not _authed():
        return jsonify(error="bad or missing bearer token"), 401
    report = request.get_json(silent=True)
    if not isinstance(report, dict):
        return jsonify(error="body must be a JSON object"), 400
    report.setdefault("source", "mac")
    _state["medic"] = report
    _save_state()
    return jsonify(ok=True)


@app.get("/api/health")
def health():
    mc = _state.get("medic_cloud") or {}
    return jsonify(
        ok=True,
        last_sync=_state["received_at"],
        # cloud-medic liveness — proves the 24/7 triage thread is ticking even
        # when the Mac medic owns the display. No secrets (deploy hook is not here).
        medic_cloud={
            "last_cycle_ts": mc.get("last_cycle_ts"),
            "last_status": mc.get("last_status"),
            "last_findings": mc.get("last_findings"),
            "last_restart_ts": mc.get("last_restart_ts"),
        },
    )


# -------------------------------------------------------------- cloud medic
# Self-contained 24/7 triage loop, ported from scripts/medic.py (the dash
# deploys standalone — no synth package, no scripts/). A daemon thread wakes
# every MEDIC_INTERVAL_S and triages the in-memory _state directly: the age
# of received_at replaces the Mac medic's /api/state fetch, so a fresh dash
# boot (received_at is None) is reported as DASH_AMNESIA (info, NO restart)
# instead of a false-positive STALE_SNAPSHOT. Bounded fix: STALE_SNAPSHOT /
# DISCONNECT_LOOP / RATE_LIMITED POST the Render deploy hook (env
# SYNTH_DEPLOY_HOOK), hard-limited to 1 restart per 2h, persisted in the
# state file under "medic_cloud". No LLM in the cloud medic.
MEDIC_INTERVAL_S = 900
MEDIC_STALE_AFTER_S = 600
MEDIC_ANALYST_DEAD_S = 3 * 3600
MEDIC_TOKEN_CRIT_DAYS = 3
MEDIC_DISCONNECT_LOOP_MIN = 3
MEDIC_UNKNOWN_CRIT_MIN = 5
MEDIC_RESTART_COOLDOWN_S = 2 * 3600
MEDIC_RESTART_CODES = {"STALE_SNAPSHOT", "DISCONNECT_LOOP", "RATE_LIMITED"}

# (code, severity, min line count to emit) — order = classification priority
_MEDIC_ENGINE_CODES: tuple[tuple[str, str, int], ...] = (
    ("ANALYST_FALLBACK", "warn", 1),
    ("TRENDBAR_FAIL", "warn", 1),
    ("DISCONNECT_LOOP", "critical", MEDIC_DISCONNECT_LOOP_MIN),
    ("RATE_LIMITED", "critical", 1),
    ("ORDER_REJECTED", "warn", 1),
)


def _medic_finding(code: str, severity: str, detail: str,
                   count: int | None = None) -> dict:
    f: dict[str, Any] = {"code": code, "severity": severity, "detail": detail}
    if count is not None:
        f["count"] = count
    return f


def _medic_run_lines(snap: dict) -> list[str]:
    """Decision-log lines from the CURRENT engine run (stale ones excluded)."""
    out: list[str] = []
    for d in snap.get("decisions") or []:
        if isinstance(d, dict):
            if d.get("text") and not d.get("stale"):
                out.append(str(d["text"]))
        elif d:
            out.append(str(d))
    return out


def _medic_classify(line: str) -> str | None:
    """Map one decision line to an engine-error code (first match wins)."""
    low = line.lower()
    if "analyst unavailable" in low:
        return "ANALYST_FALLBACK"
    if "trendbars" in low:
        return "TRENDBAR_FAIL"
    if "disconnected" in low:
        return "DISCONNECT_LOOP"
    if "rate limited" in low or "blocked_payload_type" in low:
        return "RATE_LIMITED"
    if "rejected" in low:
        return "ORDER_REJECTED"
    if "error" in low:
        return "UNKNOWN_ERROR"
    return None


def _medic_triage(snap: dict, received_at: float | None, now: float) -> list[dict]:
    """Deterministic findings over the stored state. Pure: no I/O, no clock —
    `now` is injected so tests can pin it."""
    findings: list[dict] = []

    # -- staleness: age of received_at replaces the snapshot fetch ---------
    if received_at is None:
        findings.append(_medic_finding(
            "DASH_AMNESIA", "info",
            "dashboard restarted and no push received yet — worker state "
            "unknown; waiting for the next push (no restart on this)"))
    elif now - received_at > MEDIC_STALE_AFTER_S:
        findings.append(_medic_finding(
            "STALE_SNAPSHOT", "critical",
            f"last push {int(now - received_at)}s ago "
            f"(limit {MEDIC_STALE_AFTER_S}s) — worker down or stuck"))

    # -- ENGINE_ERRORS: classify the current run's decision lines ----------
    health_d = snap.get("health") if isinstance(snap.get("health"), dict) else {}
    errs = int(_num(health_d.get("errors_current_run")) or 0)
    if errs > 0:
        counts: dict[str, int] = {}
        samples: dict[str, str] = {}
        for line in _medic_run_lines(snap):
            code = _medic_classify(line)
            if code:
                counts[code] = counts.get(code, 0) + 1
                samples.setdefault(code, line.strip())
        emitted = False
        for code, severity, min_n in _MEDIC_ENGINE_CODES:
            n = counts.get(code, 0)
            if n >= min_n:
                findings.append(_medic_finding(
                    code, severity,
                    f"{n} line(s) this run, e.g. “{samples[code][:160]}”",
                    count=n))
                emitted = True
        unknown = counts.get("UNKNOWN_ERROR", 0)
        if unknown or not emitted:
            n = unknown or errs
            findings.append(_medic_finding(
                "UNKNOWN_ERROR",
                "critical" if n >= MEDIC_UNKNOWN_CRIT_MIN else "warn",
                (f"{unknown} unclassified error line(s), e.g. "
                 f"“{samples['UNKNOWN_ERROR'][:160]}”" if unknown else
                 f"health reports {errs} error(s) this run but no matching "
                 "decision lines were pushed"),
                count=n))

    # -- KILL_SWITCH --------------------------------------------------------
    risk = snap.get("risk") if isinstance(snap.get("risk"), dict) else {}
    if risk.get("kill_switch"):
        findings.append(_medic_finding(
            "KILL_SWITCH", "critical",
            "kill switch file present — all new entries halted; needs human "
            "review before re-arming"))

    # -- TOKEN_EXPIRY --------------------------------------------------------
    expires = str(snap.get("token_expires") or "")
    days: int | None = None
    try:
        days = (datetime.strptime(expires, "%Y-%m-%d").date()
                - datetime.fromtimestamp(now, tz=timezone.utc).date()).days
    except ValueError:
        pass
    if days is not None:
        if days < 0:
            findings.append(_medic_finding(
                "TOKEN_EXPIRY", "critical",
                f"cTrader token EXPIRED {-days}d ago ({expires}) — renew now"))
        elif days < MEDIC_TOKEN_CRIT_DAYS:
            findings.append(_medic_finding(
                "TOKEN_EXPIRY", "critical",
                f"cTrader token expires in {days}d ({expires}) — renew now"))
        elif days < TOKEN_WARN_DAYS:
            findings.append(_medic_finding(
                "TOKEN_EXPIRY", "warn",
                f"cTrader token expires in {days}d ({expires}) — schedule a renewal"))

    # -- ANALYST_DEAD --------------------------------------------------------
    analyst = snap.get("analyst") if isinstance(snap.get("analyst"), dict) else {}
    a_ts = _num(analyst.get("ts"))
    if (str(analyst.get("notes") or "").lower().startswith("fallback")
            and a_ts is not None and now - a_ts > MEDIC_ANALYST_DEAD_S):
        findings.append(_medic_finding(
            "ANALYST_DEAD", "warn",
            f"analyst stuck in neutral fallback for {(now - a_ts) / 3600:.1f}h "
            "— Claude offline on the worker"))

    return findings


def _medic_status(findings: list[dict]) -> str:
    """'healthy' | 'degraded' | 'critical' from the worst finding severity."""
    severities = {str(f.get("severity")) for f in findings}
    if "critical" in severities:
        return "critical"
    if "warn" in severities:
        return "degraded"
    return "healthy"


def _medic_restart_reasons(findings: list[dict]) -> list[str]:
    """Finding codes that justify a worker restart (bounded fix policy).
    DASH_AMNESIA is deliberately NOT restart-worthy."""
    return [str(f.get("code")) for f in findings
            if str(f.get("code")) in MEDIC_RESTART_CODES]


def _medic_restart_allowed(medic_cloud: Any, now: float) -> tuple[bool, str]:
    """Rate limiter: max 1 restart per MEDIC_RESTART_COOLDOWN_S. Pure — the
    caller persists the state. Returns (allowed, reason-if-blocked)."""
    last = _num(medic_cloud.get("last_restart_ts")) if isinstance(medic_cloud, dict) else None
    if last is not None and 0 <= now - last < MEDIC_RESTART_COOLDOWN_S:
        ago = int((now - last) / 60)
        left = int((MEDIC_RESTART_COOLDOWN_S - (now - last)) / 60)
        return False, f"last restart {ago}m ago, cooldown active (~{left}m left)"
    return True, ""


def _post_deploy_hook(hook: str) -> tuple[bool, str]:
    """POST the Render deploy hook (secret lives in the URL; empty body)."""
    req = urllib.request.Request(hook, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return 200 <= resp.status < 300, f"HTTP {resp.status}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _medic_cycle(now: float | None = None) -> dict:
    """One triage -> (bounded) fix -> report pass over the stored state."""
    now = time.time() if now is None else now
    snap = _state["snapshot"] if isinstance(_state["snapshot"], dict) else {}
    # copy, don't replace: medic_cloud also carries the restart rate-limiter
    # state (last_restart_ts) which must persist across cycles.
    mc = dict(_state.get("medic_cloud") or {})
    findings = _medic_triage(snap, _state.get("received_at"), now)
    status = _medic_status(findings)

    actions: list[str] = []
    reasons = sorted(set(_medic_restart_reasons(findings)))
    if reasons:
        reason_txt = ",".join(reasons)
        hook = os.environ.get("SYNTH_DEPLOY_HOOK", "").strip()
        if not hook:
            actions.append("restart needed — env SYNTH_DEPLOY_HOOK not set")
        else:
            allowed, why = _medic_restart_allowed(mc, now)
            if not allowed:
                actions.append(f"restart skipped — {why}")
            else:
                ok, http_txt = _post_deploy_hook(hook)
                if ok:
                    mc["last_restart_ts"] = now
                    mc["last_restart_reason"] = reason_txt
                    actions.append(f"worker restart triggered via deploy hook "
                                   f"({http_txt}) — reason: {reason_txt}")
                else:
                    actions.append(f"restart attempt FAILED ({http_txt}) "
                                   f"— reason: {reason_txt}")
    else:
        actions.append("no action needed")

    report = {"ts": int(now), "status": status, "findings": findings,
              "actions": actions, "diagnosis": "—", "source": "cloud"}
    # liveness stamp: proves the cloud medic actually ticked, even while the
    # Mac medic (laptop on) currently owns the shared `medic` display field.
    mc["last_cycle_ts"] = int(now)
    mc["last_status"] = status
    mc["last_findings"] = [str(f.get("code")) for f in findings]
    _state["medic_cloud"] = mc
    _state["medic"] = report
    _save_state()
    print(f"[cloud-medic] tick status={status} "
          f"findings={mc['last_findings']} actions={actions}", flush=True)
    return report


def _medic_loop() -> None:
    """Run forever; a cycle failure must never kill the thread."""
    while True:
        try:
            _medic_cycle()
        except Exception:
            # never die — but never hide either: a silently-failing medic is
            # indistinguishable from a healthy one (learned the hard way)
            traceback.print_exc()
        time.sleep(MEDIC_INTERVAL_S)


_MEDIC_THREAD_LOCK = threading.Lock()
_medic_thread: threading.Thread | None = None


def _start_medic_thread() -> None:
    """Start the cloud medic once per process (gunicorn -w 1 imports the
    module once, but guard anyway — reloaders/tests may import twice).
    Set env SYNTH_MEDIC_LOOP=0 to disable (tests, local dev)."""
    global _medic_thread
    if os.environ.get("SYNTH_MEDIC_LOOP", "1") == "0":
        return
    with _MEDIC_THREAD_LOCK:
        if _medic_thread is not None and _medic_thread.is_alive():
            return
        _medic_thread = threading.Thread(
            target=_medic_loop, name="cloud-medic", daemon=True)
        _medic_thread.start()


# ------------------------------------------------------------ page sections
def _hero(snap: dict, received_at: float | None) -> str:
    risk = snap.get("risk") if isinstance(snap.get("risk"), dict) else {}
    positions = _rows(snap.get("open_positions"))
    equity, balance = _num(snap.get("equity")), _num(snap.get("balance"))
    start = _num(snap.get("start_balance"))
    pnl = equity - start if equity is not None and start is not None else None
    pnl_pct = pnl / start * 100 if pnl is not None and start else None

    banners = []  # (text, css class) — red for faults, amber for by-design pauses
    if received_at is None:
        banners.append(("NO DATA YET — waiting for the first push from the bot", "banner"))
    elif time.time() - received_at > OFFLINE_AFTER_S:
        banners.append((f"BOT OFFLINE? last sync {_age(time.time() - received_at)} ago", "banner"))
    if risk.get("kill_switch"):
        banners.append(("KILL SWITCH ACTIVE — entries halted", "banner"))
    veto_n, bias_n = _veto_counts(snap)
    if bias_n and veto_n * 2 >= bias_n:
        banners.append((f"MACRO FILTER PAUSE — analyst has vetoed {veto_n}/{bias_n} markets; "
                        "entries suspended by design", "banner amb"))
    banner_html = "".join(f'<div class="{cls}">{esc(b)}</div>' for b, cls in banners)

    mode = "MICRO" if risk.get("micro") else "NORMAL"
    pnl_txt = _usd(pnl, signed=True) + (f" ({pnl_pct:+.1f}%)" if pnl_pct is not None else "")
    tiles = [
        ("Equity", _usd(equity), "cy"),
        ("Balance", _usd(balance), ""),
        ("P&amp;L vs start", esc(pnl_txt) if pnl is not None else "—", _pnl_cls(pnl)),
        ("Open positions", str(len(positions)), ""),
        ("Mode", mode, "amb" if mode == "MICRO" else "pos"),
    ]
    tiles_html = "".join(
        f'<div class="tile"><div class="k">{k}</div><div class="v {cls}">{v}</div></div>'
        for k, v, cls in tiles
    )
    pos_html = ""
    if positions:
        body = "".join(
            f"<tr><td>{esc(p.get('symbol', '?'))}</td><td>{esc(p.get('side', '?'))}</td>"
            f"<td>{esc(p.get('lots', '—'))}</td><td>{esc(p.get('entry', '—'))}</td>"
            f"<td>{esc(p.get('stop', '—'))}</td>"
            f"<td class='{_pnl_cls(_num(p.get('pnl')))}'>{_usd(_num(p.get('pnl')), signed=True)}</td></tr>"
            for p in positions
        )
        pos_html = (
            '<div class="panel scroll" style="margin-top:10px"><table>'
            "<tr><th>Symbol</th><th>Side</th><th>Lots</th><th>Entry</th><th>Stop</th><th>P&amp;L</th></tr>"
            f"{body}</table></div>"
        )
    return f"{banner_html}<div class='tiles'>{tiles_html}</div>{pos_html}"


def _equity_chart(series: list) -> str:
    pts = [(_num(p[0], 0.0), _num(p[1])) for p in series
           if isinstance(p, (list, tuple)) and len(p) >= 2 and _num(p[1]) is not None]
    if len(pts) < 2:
        return '<div class="panel mut">Not enough equity points yet — the chart fills as the bot pushes.</div>'
    w, h, pad = 860.0, 240.0, 12.0
    vals = [v for _, v in pts]
    lo, hi = min(vals), max(vals)
    if hi - lo < 1e-9:
        lo, hi = lo - 1.0, hi + 1.0
    step = (w - 2 * pad) / (len(pts) - 1)
    coords = " ".join(
        f"{pad + i * step:.1f},{pad + (hi - v) * (h - 2 * pad) / (hi - lo):.1f}"
        for i, v in enumerate(vals)
    )
    lx, ly = coords.rsplit(" ", 1)[-1].split(",")
    area = f"{pad:.1f},{h - pad:.1f} {coords} {w - pad:.1f},{h - pad:.1f}"
    return (
        '<div class="panel">'
        f'<svg viewBox="0 0 {w:.0f} {h:.0f}" style="width:100%;height:auto;display:block" '
        'xmlns="http://www.w3.org/2000/svg" role="img" aria-label="equity curve">'
        f'<polygon points="{area}" fill="#22d3ee" opacity="0.08"/>'
        f'<polyline points="{coords}" fill="none" stroke="#22d3ee" stroke-width="2"/>'
        f'<circle cx="{lx}" cy="{ly}" r="3.5" fill="#34d399"/></svg>'
        f'<div class="axis"><span>{_fmt_ts(pts[0][0])}</span>'
        f'<span>low {_usd(min(vals))} · high {_usd(max(vals))} · last <b class="cy">{_usd(vals[-1])}</b></span>'
        f'<span>{_fmt_ts(pts[-1][0])}</span></div></div>'
    )


_RICH_TAG = re.compile(r"\[/?[a-z ]+\]")  # strip rich markup ([bold cyan], [/red], ...)


def _decision_cls(line: str) -> str:
    low = line.lower()
    if "opened" in low:
        return "cy"
    if "blocked" in low:
        return "amb"
    if "rejected" in low or "error" in low or "kill" in low:
        return "neg"
    return "mut"


def _decisions(snap: dict) -> str:
    trades = _rows(snap.get("trades"))[-25:][::-1]
    if trades:
        body = "".join(
            f"<tr><td>{_fmt_ts(t.get('closed_at'))}</td><td>{esc(t.get('strategy', '?'))}</td>"
            f"<td>{esc(t.get('symbol', '?'))}</td><td>{esc(t.get('side', '?'))}</td>"
            f"<td>{esc(t.get('lots', '—'))}</td>"
            f"<td class='{_pnl_cls(_num(t.get('pnl_usd')))}'>{_usd(_num(t.get('pnl_usd')), signed=True)}</td>"
            f"<td class='mut'>{esc(t.get('exit_reason', ''))}</td></tr>"
            for t in trades
        )
        trades_html = (
            '<div class="panel scroll"><table>'
            "<tr><th>Closed</th><th>Strategy</th><th>Symbol</th><th>Side</th>"
            "<th>Lots</th><th>P&amp;L</th><th>Exit</th></tr>"
            f"{body}</table></div>"
        )
    else:
        trades_html = '<div class="panel mut">No closed trades yet.</div>'

    # health banner: reflects the CURRENT run, so stale errors below never mislead
    health = snap.get("health") or {}
    banner = ""
    if isinstance(health, dict) and health.get("status"):
        started = esc(health.get("run_started") or "?")
        errs = health.get("errors_current_run") or 0
        if health["status"] == "ok" and not errs:
            banner = (f'<div class="panel" style="margin-top:10px;border-color:#1f7a4d;color:#4ade80">'
                      f'✓ ENGINE HEALTHY — running clean since {started} UTC, no errors this run. '
                      f'<span class="mut">(a quiet log is normal: trades are infrequent by design)</span></div>')
        else:
            banner = (f'<div class="panel" style="margin-top:10px;border-color:#a15;color:#f87171">'
                      f'⚠ {errs} error(s) in the current run (since {started} UTC) — check below.</div>')

    raw = snap.get("decisions", [])
    entries = []  # (text, stale)
    if isinstance(raw, list):
        for d in raw:
            if isinstance(d, dict):
                if d.get("text"):
                    entries.append((str(d["text"]), bool(d.get("stale"))))
            elif d:
                entries.append((str(d), False))
    if entries:
        items = "".join(
            f"<li class='{_decision_cls(t)}{' stale' if stale else ''}'>"
            f"{'<span class=\"mut\">[previous run] </span>' if stale else ''}{esc(_RICH_TAG.sub('', t))}</li>"
            for t, stale in entries[-60:][::-1]
        )
        log_html = f'<div class="panel scroll" style="margin-top:10px"><ul class="log">{items}</ul></div>'
    else:
        log_html = '<div class="panel mut" style="margin-top:10px">No decision log lines yet.</div>'
    return trades_html + banner + log_html


def _lessons(snap: dict) -> str:
    lessons = _rows(snap.get("lessons"))
    if not lessons:
        return '<div class="panel mut">No lessons recorded yet.</div>'
    lessons = sorted(lessons, key=lambda l: _num(l.get("ts"), 0.0) or 0.0, reverse=True)
    items = "".join(
        f'<li><span class="mut">{_fmt_ts(l.get("ts"))}</span> — {esc(l.get("insight", ""))}'
        + (f' <span class="cy">→ {esc(l.get("action"))}</span>' if l.get("action") else "")
        + "</li>"
        for l in lessons
    )
    return f'<div class="panel"><ul class="lessons">{items}</ul></div>'


_REGIME_CLS = {"trending-up": "pos", "trending-down": "neg", "ranging": "mut", "volatile": "amb"}
_KIND_CLS = {"observation": "mut", "suggestion": "amb", "action": "cy"}


def _coach(coach: Any) -> str:
    """Render the Coach's Daily Brief; tolerates any missing/malformed field."""
    if not isinstance(coach, dict) or not coach:
        return ('<div class="panel mut">No coach brief yet — '
                'the daily coach posts once per trading day.</div>')
    headline = esc(coach.get("headline") or "Daily brief")
    parts = [
        '<div class="panel">',
        '<div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px">'
        f'<b class="cy">{headline}</b>'
        f'<span class="mut">{_fmt_ts(coach.get("ts"))}</span></div>',
    ]
    regimes = _rows(coach.get("market_regime"))
    if regimes:
        body = "".join(
            f"<tr><td>{esc(r.get('symbol', '?'))}</td>"
            f"<td class='{_REGIME_CLS.get(str(r.get('regime', '')).lower(), 'mut')}'>"
            f"{esc(r.get('regime', '—'))}</td>"
            f"<td class='mut'>{esc(r.get('note', ''))}</td></tr>"
            for r in regimes
        )
        parts.append(
            '<div class="scroll" style="margin-top:10px"><table>'
            "<tr><th>Symbol</th><th>Regime</th><th>Note</th></tr>"
            f"{body}</table></div>"
        )
    if coach.get("performance_review"):
        parts.append(f'<p style="margin-top:10px">{esc(coach.get("performance_review"))}</p>')
    recs = _rows(coach.get("recommendations"))
    if recs:
        items = "".join(
            f"<li><span class='{_KIND_CLS.get(str(r.get('kind', '')).lower(), 'mut')}' "
            "style='text-transform:uppercase;font-size:10px;letter-spacing:.08em'>"
            f"{esc(r.get('kind', 'note'))}</span> <b>{esc(r.get('title', ''))}</b>"
            f"<div class='mut'>{esc(r.get('detail', ''))}</div></li>"
            for r in recs
        )
        parts.append(f'<ul class="coach" style="margin-top:10px">{items}</ul>')
    if not coach.get("llm"):
        parts.append('<div class="mut" style="font-size:11px;margin-top:10px">'
                     "statistical mode — run claude /login on the Mac for full AI analysis</div>")
    parts.append("</div>")
    return "".join(parts)


def _scorecard(snap: dict) -> str:
    trades = _rows(snap.get("trades"))
    sc = snap.get("scorecard") if isinstance(snap.get("scorecard"), dict) else {}
    paused = sc.get("paused") if isinstance(sc.get("paused"), dict) else {}
    stats: dict[str, dict[str, float]] = {}
    for t in trades:
        d = stats.setdefault(str(t.get("strategy", "?")), {"n": 0, "pnl": 0.0, "wins": 0})
        pnl = _num(t.get("pnl_usd"), 0.0) or 0.0
        d["n"] += 1
        d["pnl"] += pnl
        d["wins"] += pnl > 0
    for name in paused:
        stats.setdefault(str(name), {"n": 0, "pnl": 0.0, "wins": 0})
    if not stats:
        return '<div class="panel mut">No strategy data yet.</div>'
    rows = []
    for name, d in sorted(stats.items(), key=lambda kv: kv[1]["pnl"], reverse=True):
        wr = f"{d['wins'] / d['n'] * 100:.0f}%" if d["n"] else "—"
        status = (
            f'<span class="neg">PAUSED since {_fmt_ts(paused[name])}</span>'
            if name in paused else '<span class="pos">active</span>'
        )
        rows.append(
            f"<tr><td>{esc(name)}</td><td>{int(d['n'])}</td>"
            f"<td class='{_pnl_cls(d['pnl'] if d['n'] else None)}'>{_usd(d['pnl'], signed=True) if d['n'] else '—'}</td>"
            f"<td>{wr}</td><td>{status}</td></tr>"
        )
    return (
        '<div class="panel scroll"><table>'
        "<tr><th>Strategy</th><th>Trades</th><th>P&amp;L</th><th>Win rate</th><th>Status</th></tr>"
        + "".join(rows)
        + '</table><div class="mut" style="font-size:11px;margin-top:6px">Stats over the pushed trade window (last 100 closed trades).</div></div>'
    )


_SEV_CLS = {"critical": "neg", "warn": "amb", "info": "mut"}
_MEDIC_PILL = {"healthy": "pos", "degraded": "amb", "critical": "neg"}


def _medic(medic: Any, medic_cloud: Any = None) -> str:
    """Compact Medic block for the Ops Panel; tolerates any missing field.

    The main line shows whichever medic wrote last (Mac reports carry an LLM
    diagnosis and overwrite the cloud report). The cloud-medic liveness footer
    is always shown when available, so a silently-dead cloud thread can't hide
    behind an active Mac medic — this is the laptop-off health signal."""
    cloud_html = ""
    cts = _num(medic_cloud.get("last_cycle_ts")) if isinstance(medic_cloud, dict) else None
    if cts:
        cstatus = str(medic_cloud.get("last_status") or "unknown").lower()
        cpill = _MEDIC_PILL.get(cstatus, "mut")
        cloud_html = (
            "<div class='mut' style='font-size:11px;margin-top:6px'>"
            f"cloud medic (24/7): <span class='pill {cpill}'>{esc(cstatus)}</span> "
            f"· last tick {_fmt_ts(cts)} · {_age(time.time() - cts)} ago</div>")
    if not isinstance(medic, dict) or not medic:
        return ('<div class="panel mut" style="margin-top:10px">'
                "Medic: no report yet — the built-in cloud medic checks every "
                "15 minutes." + cloud_html + "</div>")
    status = str(medic.get("status") or "unknown").lower()
    pill = _MEDIC_PILL.get(status, "mut")
    source = str(medic.get("source") or "mac").lower()
    ts = _num(medic.get("ts"))
    checked = (f"via {source} · last check {_fmt_ts(ts)} · {_age(time.time() - ts)} ago"
               if ts else f"via {source} · last check time unknown")

    findings = _rows(medic.get("findings"))
    if findings:
        items = "".join(
            f"<li><b class='{_SEV_CLS.get(str(f.get('severity', '')).lower(), 'mut')}'>"
            f"{esc(f.get('code', '?'))}</b> "
            f"<span class='mut'>{esc(str(f.get('detail', ''))[:140])}</span></li>"
            for f in findings[:8]
        )
        findings_html = f"<ul class='medic'>{items}</ul>"
    else:
        findings_html = "<div class='mut' style='font-size:12px'>no findings — all checks passed</div>"

    actions = medic.get("actions")
    action = str(actions[-1]) if isinstance(actions, list) and actions else ""
    action_html = (f"<div class='mut' style='font-size:12px;margin-top:6px'>"
                   f"action: {esc(action[:180])}</div>" if action else "")
    diagnosis = str(medic.get("diagnosis") or "")
    diag_html = (f"<div class='mut' style='font-size:12px;margin-top:6px'>"
                 f"diagnosis: {esc(diagnosis[:300])}</div>" if diagnosis else "")
    return (
        '<div class="panel" style="margin-top:10px">'
        '<div style="display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:6px">'
        f'<b>Medic</b><span class="pill {pill}">{esc(status)}</span>'
        f'<span class="mut" style="font-size:11px">{esc(checked)}</span></div>'
        f'<div style="margin-top:8px">{findings_html}</div>{action_html}{diag_html}{cloud_html}</div>'
    )


def _ops(snap: dict, received_at: float | None, medic: Any = None,
         medic_cloud: Any = None) -> str:
    rows: list[tuple[str, str, str]] = []

    expires = str(snap.get("token_expires") or "")
    try:
        days = (datetime.strptime(expires, "%Y-%m-%d").date()
                - datetime.now(timezone.utc).date()).days
    except ValueError:
        days = None
    if days is None:
        rows.append(("cTrader token", "expiry unknown — set token_expires in the push", "mut"))
    elif days < 0:
        rows.append(("cTrader token", f"EXPIRED {-days}d ago ({esc(expires)}) — renew cTrader token!", "neg"))
    elif days < TOKEN_WARN_DAYS:
        rows.append(("cTrader token", f"{days}d left ({esc(expires)}) — renew cTrader token!", "neg"))
    else:
        rows.append(("cTrader token", f"{days}d left (expires {esc(expires)})", "pos"))

    analyst = snap.get("analyst") if isinstance(snap.get("analyst"), dict) else {}
    fallback = bool(analyst.get("fallback")) or str(analyst.get("notes", "")).lower().startswith("fallback")
    a_age = _age(time.time() - _num(analyst.get("ts"))) if _num(analyst.get("ts")) else "unknown age"
    veto_n, bias_n = _veto_counts(snap)
    veto_txt = f" · {veto_n}/{bias_n} markets vetoed" if bias_n else ""
    if not analyst:
        rows.append(("Analyst", "no analyst data in snapshot", "mut"))
    elif fallback:
        rows.append(("Analyst", f"NEUTRAL FALLBACK ({a_age}) — Claude offline on the bot Mac; run `claude` + /login", "amb"))
    else:
        rows.append(("Analyst", f"active, view refreshed {a_age} ago{veto_txt}",
                     "amb" if bias_n and veto_n * 2 >= bias_n else "pos"))

    sync_age = time.time() - received_at if received_at is not None else None
    rows.append(("Last sync", f"{_age(sync_age)}" + (" ago" if sync_age is not None else ""),
                 "pos" if sync_age is not None and sync_age <= OFFLINE_AFTER_S else "neg"))
    rows.append(("Dashboard uptime", f"{_age(time.time() - APP_START)} — free-tier instances sleep when idle; "
                 "the bot's pushes keep this awake", "mut"))

    body = "".join(
        f"<tr><td class='mut'>{k}</td><td class='{cls}'>{v}</td></tr>" for k, v, cls in rows
    )
    return (f'<div class="panel scroll"><table class="ops">{body}</table></div>'
            + _medic(medic, medic_cloud))


_CSS = """
:root{--bg:#0b0e14;--panel:#121722;--edge:#1e2634;--txt:#d9dee9;--mut:#8b93a7;
--cyan:#22d3ee;--grn:#34d399;--red:#f87171;--amb:#fbbf24}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--txt);padding:16px;max-width:980px;margin:0 auto;
font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
header{display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:6px}
h1{font-size:19px;letter-spacing:.06em}h1 b{color:var(--cyan)}
h2{font-size:12px;text-transform:uppercase;letter-spacing:.14em;color:var(--cyan);margin:26px 0 10px}
.chip{font-size:12px;color:var(--mut)}
.banner{background:#3a1114;border:1px solid var(--red);color:#ffd9d9;padding:12px 16px;
border-radius:10px;font-weight:700;margin:14px 0;letter-spacing:.03em}
.banner.amb{background:#332508;border-color:var(--amb);color:#ffe8b0}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:14px}
.tile{background:var(--panel);border:1px solid var(--edge);border-radius:10px;padding:12px 14px}
.tile .k{font-size:10.5px;text-transform:uppercase;letter-spacing:.09em;color:var(--mut)}
.tile .v{font-size:21px;font-weight:700;margin-top:2px;font-variant-numeric:tabular-nums}
.panel{background:var(--panel);border:1px solid var(--edge);border-radius:10px;padding:13px}
.scroll{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:13px;white-space:nowrap}
th{color:var(--mut);text-transform:uppercase;font-size:10px;letter-spacing:.08em;text-align:left;
padding:5px 10px;border-bottom:1px solid var(--edge)}
td{padding:5px 10px;border-bottom:1px solid var(--edge);font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:none}
.ops td{white-space:normal}
ul.log{list-style:none;font:12px/1.75 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
ul.lessons{list-style:none}ul.lessons li{padding:5px 0;border-bottom:1px solid var(--edge)}
ul.lessons li:last-child{border-bottom:none}
ul.coach{list-style:none}ul.coach li{padding:6px 0;border-bottom:1px solid var(--edge)}
ul.coach li:last-child{border-bottom:none}
ul.medic{list-style:none;font-size:12px}ul.medic li{padding:3px 0}
.pill{display:inline-block;padding:1px 10px;border-radius:999px;font-size:10.5px;
font-weight:700;letter-spacing:.08em;text-transform:uppercase;border:1px solid currentColor}
.axis{display:flex;justify-content:space-between;font-size:11px;color:var(--mut);margin-top:6px;gap:8px;flex-wrap:wrap}
.pos{color:var(--grn)}.neg{color:var(--red)}.mut{color:var(--mut)}.amb{color:var(--amb)}.cy{color:var(--cyan)}
ul.log li.stale{opacity:.42;font-style:italic}
footer{color:var(--mut);font-size:11px;margin:26px 0 8px;text-align:center}
"""


@app.get("/")
def index() -> Response:
    snap = _state["snapshot"] if isinstance(_state["snapshot"], dict) else {}
    received_at = _state["received_at"]
    page = (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>Synthetic Trader</title>"
        f"<style>{_CSS}</style></head><body>"
        "<header><h1>SYNTHETIC <b>TRADER</b></h1>"
        f"<span class='chip'>snapshot ts {_fmt_ts(snap.get('ts'))} · rendered {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC</span></header>"
        + _hero(snap, received_at)
        + "<h2>Equity Chart</h2>" + _equity_chart(_state["series"])
        + "<h2>Decisions</h2>" + _decisions(snap)
        + "<h2>Lessons Learned</h2>" + _lessons(snap)
        + "<h2>Coach's Daily Brief</h2>" + _coach(_state.get("coach"))
        + "<h2>Strategy Scorecard</h2>" + _scorecard(snap)
        + "<h2>Ops Panel</h2>" + _ops(snap, received_at, _state.get("medic"),
                                      _state.get("medic_cloud"))
        + "<footer>synthetic-trader dashboard · state is pushed by the bot's Mac · refresh for latest</footer>"
        "</body></html>"
    )
    return Response(page, mimetype="text/html")


_load_state()
# Start the medic lazily on the first request, NOT at import time: under
# `gunicorn --preload` the module imports in the MASTER process, so a thread
# started here would live in the master — whose _state never receives pushes
# (those go to forked workers). That medic saw DASH_AMNESIA forever and the
# workers served a frozen medic_cloud (the 2-day-blind-medic incident,
# 2026-07-16). before_request runs in the serving worker, so the thread
# shares the _state that pushes actually mutate. _start_medic_thread is
# lock-guarded and idempotent, so the per-request call is a cheap no-op after
# the first. (With -w >1 each worker gets its own medic thread; restart
# cooldown is per-process — run this service with a single worker.)
@app.before_request
def _ensure_medic_thread() -> None:
    _start_medic_thread()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5077")))
