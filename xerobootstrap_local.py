import os
import json
import webbrowser
import base64
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests

AUTH_URL = "https://login.xero.com/identity/connect/authorize"
TOKEN_URL = "https://identity.xero.com/connect/token"
CONNECTIONS_URL = "https://api.xero.com/connections"
TOKENS_FILE = "xero_tokens.json"

#SCOPES = "offline_access accounting.reports.read"  # minimal for Bank Summary automation
SCOPES = "offline_access accounting.reports.read accounting.transactions.read accounting.journals.read accounting.settings.read accounting.contacts.read accounting.attachments.read"

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        if "code" not in qs:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing ?code= in callback")
            return

        code = qs["code"][0]
        self.server.auth_code = code

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"You can close this tab now. Auth code received.")

def build_consent_url():
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

def exchange_code_for_tokens(code: str) -> dict:
    client_id = os.environ["XERO_CLIENT_ID"]
    client_secret = os.environ["XERO_CLIENT_SECRET"]
    redirect_uri = os.environ["XERO_REDIRECT_URI"]

    # Basic auth header
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}"}
    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri}

    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def refresh_access_token(refresh_token: str) -> dict:
    client_id = os.environ["XERO_CLIENT_ID"]
    client_secret = os.environ["XERO_CLIENT_SECRET"]
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}"}
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
        raise RuntimeError("No organisations connected. In Xero consent screen, select an organisation.")
    return conns[0]["tenantId"]

def main():
    # 1) Start a tiny local server to catch the callback
    redirect_uri = os.environ["XERO_REDIRECT_URI"]
    parsed = urllib.parse.urlparse(redirect_uri)
    host = parsed.hostname or "localhost"
    port = parsed.port or 8080

    httpd = HTTPServer((host, port), CallbackHandler)

    # 2) Open consent URL
    url = build_consent_url()
    print("Opening browser for consent:\n", url)
    webbrowser.open(url)

    # 3) Wait for callback
    print(f"Waiting for callback on {host}:{port} ...")
    httpd.handle_request()
    code = getattr(httpd, "auth_code", None)
    if not code:
        raise RuntimeError("Did not receive auth code")

    # 4) Exchange code -> tokens
    tokens = exchange_code_for_tokens(code)

    # 5) Immediately fetch tenant_id using access_token
    tenant_id = get_tenant_id(tokens["access_token"])

    # 6) Save minimal token bundle
    to_save = {
        "refresh_token": tokens["refresh_token"],
        "tenant_id": tenant_id,
    }
    with open(TOKENS_FILE, "w", encoding="utf-8") as f:
        json.dump(to_save, f, indent=2)

    print("Saved:", TOKENS_FILE)
    print("tenant_id:", tenant_id)

if __name__ == "__main__":
    main()