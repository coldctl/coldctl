"""Loopback-only test fixture for the GCS XML emulator's missing create precondition.

NOT a production proxy. Only models x-goog-if-generation-match: 0, under a single
process lock. All mutations for a test prefix must pass through this fixture.
Real GCS IAM, retention, generation races and durability require live testing.
"""
import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--listen-port', type=int, default=14445)
parser.add_argument('--upstream-port', type=int, default=14443)
args = parser.parse_args()
assert 1024 <= args.listen_port <= 65535 and 1024 <= args.upstream_port <= 65535
assert args.listen_port != args.upstream_port
lock = threading.Lock()
stats = {'conditional_creates': 0, 'precondition_failures': 0}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, status, body=b'', headers=()):
        self.send_response(status)
        for key, value in headers:
            if key.lower() not in ('transfer-encoding', 'connection', 'content-length', 'server', 'date'):
                self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    def upstream(self, method, body=None):
        headers = {key: value for key, value in self.headers.items()
                   if key.lower() not in ('host', 'connection', 'content-length', 'transfer-encoding', 'expect', 'authorization')}
        if method == 'HEAD':
            headers.pop('x-goog-if-generation-match', None)
        connection = http.client.HTTPConnection('127.0.0.1', args.upstream_port, timeout=30)
        try:
            connection.request(method, self.path, body=body, headers=headers)
            response = connection.getresponse()
            return response.status, response.read(), response.getheaders()
        finally:
            connection.close()

    def handle_request(self):
        if self.path == '/__fixture_stats' and self.command == 'GET':
            return self.respond(200, json.dumps(stats).encode())
        # SDK 0.14 PUT emits an empty Bearer header with disable_oauth.
        if not self.path.startswith('/') or self.path.startswith('//') or self.headers.get('Authorization', '').strip() not in ('', 'Bearer'):
            return self.respond(400, b'Anonymous local fixture only')
        if self.headers.get('Transfer-Encoding'):
            return self.respond(400, b'Chunked uploads are outside this fixture')
        try:
            length = int(self.headers.get('Content-Length', 0))
            if not 0 <= length <= 128 * 1024 * 1024:
                return self.respond(413)
            body = self.rfile.read(length) if length else None
            with lock:
                if self.command == 'PUT' and self.headers.get('x-goog-if-generation-match') is not None:
                    if self.headers['x-goog-if-generation-match'] != '0':
                        return self.respond(400, b'Only generation-match zero is modeled')
                    stats['conditional_creates'] += 1
                    status, _, _ = self.upstream('HEAD')
                    if status == 200:
                        stats['precondition_failures'] += 1
                        return self.respond(412, b'<Error><Code>PreconditionFailed</Code></Error>')
                    if status != 404:
                        return self.respond(502, b'Unexpected fixture HEAD status')
                status, response, headers = self.upstream(self.command, body)
                # HEAD must retain the upstream object's content length.
                if self.command == 'HEAD':
                    self.send_response(status)
                    for key, value in headers:
                        if key.lower() not in ('connection', 'transfer-encoding', 'server', 'date'):
                            self.send_header(key, value)
                    self.end_headers()
                else:
                    self.respond(status, response, headers)
        except (OSError, ValueError, http.client.HTTPException):
            self.respond(502, b'Local fixture transport failed')

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request


print(f'GCS precondition test fixture on 127.0.0.1:{args.listen_port}', flush=True)
ThreadingHTTPServer(('127.0.0.1', args.listen_port), Handler).serve_forever()
