"""Read-only snapshot downloads; use only on a trusted LAN or encrypted VPN."""
import argparse
import base64
import hmac
import json
import secrets
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Downloads(SimpleHTTPRequestHandler):
    def do_GET(self):
        if not self.authorized():
            return
        super().do_GET()

    def do_HEAD(self):
        if not self.authorized():
            return
        super().do_HEAD()

    def authorized(self):
        if not hmac.compare_digest(self.headers.get('Authorization', ''), self.server.authorization):
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="BigData snapshots"')
            self.end_headers()
            return False
        target = Path(self.translate_path(self.path)).resolve()
        if not target.is_relative_to(Path(self.directory).resolve()):
            self.send_error(403)
            return False
        return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    downloads = root / 'runtime/shared_downloads'
    credentials = root / 'runtime/share_credentials.json'
    downloads.mkdir(parents=True, exist_ok=True)
    if not credentials.exists():
        try:
            with credentials.open('x', encoding='utf-8') as handle:
                json.dump({'username': 'bigdata', 'password': secrets.token_urlsafe(24)}, handle)
        except FileExistsError:
            pass
    login = json.loads(credentials.read_text(encoding='utf-8'))
    encoded = base64.b64encode(f"{login['username']}:{login['password']}".encode()).decode()
    server = ThreadingHTTPServer((args.host, args.port), partial(Downloads, directory=str(downloads)))
    server.authorization = 'Basic ' + encoded
    print(f'Download server: http://{args.host}:{args.port}; credentials: {credentials}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
