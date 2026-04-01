import os
import json
import base64
import urllib.parse
import threading
import time
import sys
from flask import Flask, request, Response
import requests


AUTH_URL = "https://login.xero.com/identity/connect/authorize"
TOKEN_URL = "https://identity.xero.com/connect/token"
CONNECTIONS_URL = "https://api.xero.com/connections"

# Scopes: adjust as needed
SCOPES = (
    "offline_access "
    "accounting.reports.read "
    "accounting.transactions.read "
    "accounting.journals.read "
    "accounting.settings.read "
    "accounting.contacts.read "
    "accounting.attachments.read"
)

# IF ALREADY AVAILABLE...
def _default_tokens_path() -> str:
    env_path = os.getenv("TOKENS_FILE")
    if env_path:
        return env_path
    if os.path.isdir("/data"):
        return "/data/xero_tokens.json"
    return "xero_tokens.json"

TOKENS_FILE = _default_tokens_path()

# Event used to block main thread until callback completes
AUTH_DONE = threading.Event()
AUTH_ERROR: list[str] = []  # store error messages if any
SAVED_TOKENS: dict | None = None

# ======= Flask app for /callback =======
app = Flask(__name__)

def build_consent_url() -> str:
    client_id = os.environ["XERO_CLIENT_ID"]
    redirect_uri = os.environ["XERO_REDIRECT_URI"]
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": SCOPES,
        "state": "xero_local",
    }
    return AUTH_URL + "?" + urllib.parse.urlencode(params)


def _basic_auth_header() -> dict:
    client_id = os.environ["XERO_CLIENT_ID"]
    client_secret = os.environ["XERO_CLIENT_SECRET"]
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return {"Authorization": f"Basic {basic}"}


def exchange_code_for_tokens(code: str) -> dict:
    """
    Exchange authorization code -> access/refresh tokens using client secret.
    """
    redirect_uri = os.environ["XERO_REDIRECT_URI"]
    headers = _basic_auth_header()
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def refresh_access_token(refresh_token: str) -> dict:
    headers = _basic_auth_header()
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def get_tenant_id(access_token: str) -> str:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    r = requests.get(CONNECTIONS_URL, headers=headers, timeout=30)
    r.raise_for_status()
    conns = r.json()
    if not conns:
        raise RuntimeError(
            "No organisations connected. In the Xero consent screen, select an organisation."
        )
    return os.environ.get("tenant_id") or conns[0]["tenantId"]


def save_tokens(token_bundle: dict) -> None:
    os.makedirs(os.path.dirname(TOKENS_FILE) or ".", exist_ok=True)
    with open(TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(token_bundle, f, indent=2)
    print(f"[xerobootstrap] Saved tokens to: {TOKENS_FILE}", flush=True)


@app.route("/callback")
def callback() -> Response:
    global SAVED_TOKENS
    try:
        code = request.args.get("code")
        if not code:
            return Response("Missing ?code= in callback", status=400)

        tokens = exchange_code_for_tokens(code)

        tenant_id = get_tenant_id(tokens["access_token"])


        to_save = {
            "access_token": tokens.get("access_token"),
            "refresh_token": tokens.get("refresh_token"),
            "expires_in": tokens.get("expires_in"),
            "id_token": tokens.get("id_token"),
            "token_type": tokens.get("token_type"),
            "tenant_id": tenant_id,
            "saved_at": int(time.time()),
        }
        save_tokens(to_save)
        SAVED_TOKENS = to_save

        # 4) Unblock the main thread
        AUTH_DONE.set()
        return Response("Xero authentication complete. You can close this tab.", status=200)

    except Exception as e:
        msg = f"/callback error: {e}"
        print(f"[xerobootstrap] {msg}", flush=True)
        AUTH_ERROR.append(msg)
        AUTH_DONE.set()
        return Response("Error during authentication. Check container logs.", status=500)


def _parse_redirect_host_port():
    """
    Ensure we bind the Flask server to whatever host:port is in XERO_REDIRECT_URI.
    Typically: http://localhost:8080/callback  -> host=0.0.0.0 in container, port=8080
    """
    redirect_uri = os.environ["XERO_REDIRECT_URI"]
    parsed = urllib.parse.urlparse(redirect_uri)
    port = parsed.port or 8080
    # Always bind to 0.0.0.0 inside Docker so the host can reach it via port mapping.
    return "0.0.0.0", port


def main():

    consent_url = build_consent_url()
    print("\n[xerobootstrap] ================== ACTION REQUIRED ==================", flush=True)
    print("[xerobootstrap] Open this URL in your browser and complete Xero consent:", flush=True)
    print(consent_url, flush=True)
    print("[xerobootstrap] After consent, you will be redirected to /callback and tokens will be saved.\n", flush=True)

    # 2) Start the Flask callback server in a background thread
    host, port = _parse_redirect_host_port()
    t = threading.Thread(
        target=lambda: app.run(host=host, port=port, debug=False, use_reloader=False),
        daemon=True,
    )
    t.start()

    # 3) Wait for the callback to complete
    #    You can add a timeout if you want to fail fast.
    AUTH_DONE.wait()

    if AUTH_ERROR:
        raise SystemExit(f"[xerobootstrap] Failed: {AUTH_ERROR[0]}")

    if not SAVED_TOKENS:
        raise SystemExit("[xerobootstrap] No tokens saved (unexpected).")

    print("[xerobootstrap] Authentication finished successfully.", flush=True)


if __name__ == "__main__":
    # Validate env upfront for clearer errors
    required = ["XERO_CLIENT_ID", "XERO_CLIENT_SECRET", "XERO_REDIRECT_URI"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise SystemExit(f"[xerobootstrap] Missing required env vars: {', '.join(missing)}")

    try:
        main()
    except KeyboardInterrupt:
        print("[xerobootstrap] Cancelled by user.", flush=True)
        sys.exit(130)

