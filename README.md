# synth-dash

Read-only status dashboard for the synthetic-trader bot. The bot's Mac pushes a JSON
snapshot (equity, trades, decisions, lessons, scorecard, analyst, risk, token expiry)
to `POST /api/push` with header `Authorization: Bearer $SYNC_TOKEN`; the app mirrors it
to `/tmp/synth_state.json` and renders everything server-side at `GET /` — no JS, no CDN.

Deploy: push this repo to GitHub, then Render → New → Blueprint → pick `dash/render.yaml`
(or create a web service manually with rootDir `dash`, build `pip install -r requirements.txt`,
start `gunicorn -w 1 -b 0.0.0.0:$PORT app:app`). Health check: `GET /api/health`.

Env var: `SYNC_TOKEN` — shared secret; generate one (`openssl rand -hex 24`) and set it
both in Render and in whatever pushes the snapshot from the Mac.
