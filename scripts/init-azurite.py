"""Create only the local Azurite fixture container. No Azure account is contacted."""
import base64
import email.utils
import hashlib
import hmac
import time
import urllib.error
import urllib.request

ENDPOINT = "http://127.0.0.1:11000/coldctltest/coldctl-test?restype=container"
FIXTURE_KEY = b"coldctl-azurite-fixture-key-only"


def create_container():
    date = email.utils.formatdate(usegmt=True)
    headers = {"x-ms-date": date, "x-ms-version": "2021-12-02", "Content-Type": "application/octet-stream"}
    canonical = f"x-ms-date:{date}\nx-ms-version:2021-12-02\n/coldctltest/coldctltest/coldctl-test\nrestype:container"
    signed = "\n".join(["PUT", "", "", "", "", "application/octet-stream", "", "", "", "", "", "", canonical])
    signature = base64.b64encode(hmac.new(FIXTURE_KEY, signed.encode(), hashlib.sha256).digest()).decode()
    headers["Authorization"] = "SharedKey coldctltest:" + signature
    request = urllib.request.Request(ENDPOINT, data=b"", headers=headers, method="PUT")
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=5) as response:
            assert response.status == 201
    except urllib.error.HTTPError as error:
        if error.code != 409 or b"ContainerAlreadyExists" not in error.read():
            raise


if __name__ == "__main__":
    for attempt in range(30):
        try:
            create_container()
            print("Azurite container coldctl-test ready at 127.0.0.1:11000")
            break
        except urllib.error.HTTPError:
            raise
        except urllib.error.URLError:
            if attempt == 29:
                raise
            time.sleep(1)
