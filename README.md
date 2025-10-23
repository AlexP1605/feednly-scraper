# Feednly Scraper – anti-bot hardened

## Features
- Axios-first extraction with automatic Puppeteer fallback for dynamic or blocked pages.
- Residential proxy pool with rotation, auth support, cooldown on failures, and health metrics.
- Shared cookie jar between Axios and Puppeteer plus automatic consent dismissal.
- Advanced stealth hardening (puppeteer-extra + custom navigator/canvas/WebGL/audio patches).
- Network capture of XHR/Fetch JSON payloads with optional debug dumps via `dumpNetwork=1`.
- Structured JSON logs, `/health` and `/debug` endpoints exposing browser/pool/proxy status.
- Cloud Run ready Docker image (Chromium + fonts + headless flags).

## Installation
```bash
npm install
# or, to skip dev dependencies
npm install --omit=dev
```

Quick sanity check:
```bash
node --check server.js
```

Start the service locally:
```bash
npm start
# defaults to http://localhost:8080
```

## Environment variables
| Variable | Description | Default |
| --- | --- | --- |
| `PORT` | HTTP port | `8080` |
| `DEBUG` | Enable verbose browser/network logs | `false` |
| `MAX_RETRIES` | Retry count for Axios/Puppeteer attempts | `2` |
| `SCRAPER_WAIT_AFTER_LOAD_MS` | Extra wait after page load (ms) | `1500` |
| `SCRAPER_NAVIGATION_TIMEOUT_MS` | Puppeteer navigation timeout (ms) | `60000` |
| `SCRAPER_AXIOS_MAX_REDIRECTS` | Max redirects followed by Axios | `10` |
| `SCRAPER_PROXY_POOL` | CSV/space separated list of proxies (`http://user:pass@host:port`) | *(none)* |
| `SCRAPER_PROXY_FALLBACK` | Single fallback proxy (legacy `SCRAPER_PROXY` still read) | *(none)* |
| `SCRAPER_PROXY_FAILURE_COOLDOWN_MS` | Cooldown before reusing a failing proxy | `120000` |
| `SCRAPER_PROXY_MAX_FAILURES` | Failures before proxy blacklisted | `3` |
| `SCRAPER_CACHE_TTL` | Cache TTL in seconds | `180` |
| `DISABLE_PUPPETEER` | Disable headless browser usage | `false` |

## Proxy configuration
- `SCRAPER_PROXY_POOL`: accept CSV or whitespace separated list (`http://host:port`, `http://user:pass@host:port`).
- `SCRAPER_PROXY_FALLBACK`: optional backup proxy if pool exhausted.
- Each proxy is randomly rotated. 403/429 (and anti-bot detections) trigger temporary blacklisting.
- Proxies are applied to both Axios (`http(s)` proxy agents) and Puppeteer (`--proxy-server` + `page.authenticate`).

## API
- `GET /scrape?url=...` – main entry point.
  - Optional `waitFor` (comma-separated selectors), `waitAfterLoadMs`, `dumpNetwork=1`.
- `GET /health` – browser/cache/proxy/cookie stats.
- `GET /debug` – runtime configuration snapshot & recent pool status.

### Debug dumps
Appending `&dumpNetwork=1` to `/scrape` returns additional `diagnostics.network[]` entries containing captured XHR payloads (full JSON bodies) plus lightweight headers/metadata.

## Docker
Build and run locally:
```bash
docker build -t feednly-scraper .
docker run --rm -p 8080:8080 \
  -e SCRAPER_PROXY_POOL="http://user:pass@proxy1:1234 http://user:pass@proxy2:1234" \
  feednly-scraper
```

## Deploy to Cloud Run
1. Build & push the image:
   ```bash
   gcloud builds submit --tag gcr.io/PROJECT_ID/feednly-scraper
   ```
2. Deploy:
   ```bash
   gcloud run deploy feednly-scraper \
     --image gcr.io/PROJECT_ID/feednly-scraper \
     --platform managed \
     --region REGION \
     --allow-unauthenticated \
     --set-env-vars "SCRAPER_PROXY_POOL=http://user:pass@proxy1:1234 http://user:pass@proxy2:1234" \
     --set-env-vars "DEBUG=false"
   ```
3. Add additional env vars (`SCRAPER_PROXY_FAILURE_COOLDOWN_MS`, etc.) as needed.

## Manual tests
Replace `<BASE>` with your endpoint (e.g. `http://localhost:8080`).

### Fnac product
```bash
FNAC_URL="https://www.fnac.com/Apple-iPhone-15-128-Go-Noir-microphone-Reconditionne/a18181374/w-4"
curl -sS "${BASE}/scrape?url=${FNAC_URL}" | jq '{title, price, image: .images[0], antiBot: .meta.antiBotDetected}'
```

### Zara product
```bash
ZARA_URL="https://www.zara.com/fr/fr/robe-midi-en-tricot-p06064029.html"
curl -sS "${BASE}/scrape?url=${ZARA_URL}" | jq '{title, price, image: .images[0], antiBot: .meta.antiBotDetected}'
```

### Debug fallback (shows diagnostics if blocked)
```bash
curl -sS "${BASE}/scrape?url=${FNAC_URL}&dumpNetwork=1" | \
  jq '{ok, title, price, meta: {antiBotDetected, antiBotReasons, axios: .meta.axios, puppeteer: .meta.puppeteer}}'
```
- If extraction fails the JSON includes `meta.antiBotDetected=true`, `antiBotReasons`, `meta.puppeteer.attempts[]`, `diagnostics.network[]`.
- Proxied retry is automatic when anti-bot is detected and proxies are provided.

### Expected output (abridged)
```json
{
  "ok": true,
  "title": "Robe midi en tricot",
  "price": "49.95",
  "images": ["https://static.zara.net/.../1/w/1024/robe.jpg"],
  "meta": {
    "antiBotDetected": false,
    "axios": { "proxy": null },
    "puppeteer": { "proxy": "http://user:***@proxy1:1234", "attempts": [ ... ] }
  }
}
```

If no proxy is configured and a challenge is returned, expect:
```json
{
  "ok": true,
  "meta": {
    "antiBotDetected": true,
    "antiBotReasons": ["datadome", "http-403"],
    "puppeteer": {
      "attempts": [
        {"proxy": null, "antiBotDetected": true},
        {"proxy": "http://user:***@proxy1:1234", "antiBotDetected": false}
      ]
    }
  }
}
```

## Structured logging
- Axios completion, errors, and proxy usage emit JSON logs (`event=axios_complete`, `axios_error`, etc.).
- Puppeteer navigation and completion emit analogous events with `proxy`, `userAgent`, and anti-bot flags.

## Local troubleshooting
- `curl http://localhost:8080/health | jq` – verify browser pool & proxy health.
- `curl http://localhost:8080/debug | jq` – inspect runtime config & cache stats.
- Ensure Chromium is installed (Dockerfile already ships it) and run with `--no-sandbox` (handled automatically).

