Synthetic local TLS test fixture only.

ca.pem and identity.p12 are a self-signed localhost certificate and its test-only
private key. PKCS#12 password: coldctl-test-only. Valid 2020-2050. They have no
production use or external trust and must never be installed in an OS trust store.
The handshake test starts a loopback listener and checks custom-root acceptance,
untrusted-root rejection, and hostname mismatch rejection.
