# Run only inside a disposable MySQL test container as root.
set -eu
cd /var/lib/mysql
openssl req -x509 -newkey rsa:2048 -nodes -keyout phase4-ca-key.pem -out phase4-ca.pem -days 2 -subj '/CN=Coldctl disposable test CA' 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout phase4-server-key.pem -out phase4-server.csr -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' 2>/dev/null
openssl x509 -req -in phase4-server.csr -CA phase4-ca.pem -CAkey phase4-ca-key.pem -CAcreateserial -out phase4-server.pem -days 2 -copy_extensions copy 2>/dev/null
chown mysql:mysql phase4-*.pem
chmod 600 phase4-*-key.pem
MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -e "SET GLOBAL ssl_ca='/var/lib/mysql/phase4-ca.pem'; SET GLOBAL ssl_cert='/var/lib/mysql/phase4-server.pem'; SET GLOBAL ssl_key='/var/lib/mysql/phase4-server-key.pem'; ALTER INSTANCE RELOAD TLS; CREATE DATABASE IF NOT EXISTS coldctl_test_restore;"
