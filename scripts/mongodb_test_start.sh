#!/bin/sh
# Disposable MongoDB 8.0 fixture only. Run as the container entrypoint.
set -eu
openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/ca.key -out /tmp/ca.crt -days 2 -subj /CN=coldctl-fixture-ca
openssl req -newkey rsa:2048 -nodes -keyout /tmp/server.key -out /tmp/server.csr -subj /CN=localhost
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n' > /tmp/server.ext
openssl x509 -req -in /tmp/server.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key -CAcreateserial -out /tmp/server.crt -days 2 -extfile /tmp/server.ext
cat /tmp/server.key /tmp/server.crt > /tmp/server.pem
openssl rand -base64 756 > /tmp/replica.key
chmod 600 /tmp/replica.key /tmp/server.pem
chown mongodb:mongodb /tmp/replica.key /tmp/server.pem
exec /usr/local/bin/docker-entrypoint.sh mongod --replSet coldctlPhase5 --bind_ip_all --keyFile /tmp/replica.key --tlsMode preferTLS --tlsCertificateKeyFile /tmp/server.pem --tlsCAFile /tmp/ca.crt --tlsAllowConnectionsWithoutCertificates --setParameter enableTestCommands=1
