"""Fail if the shipped base has a database driver in its normal dependency tree."""
import subprocess

tree = subprocess.check_output(
    ["cargo", "tree", "--locked", "-p", "coldctl", "--edges", "normal", "--prefix", "none", "--format", "{p}"],
    text=True,
)
names = {line.split()[0] for line in tree.splitlines()}
forbidden = {"tokio-postgres", "postgres-native-tls", "postgres-protocol", "postgres-types", "mysql", "mysql_async", "mysql_common", "mongodb"}
if names & forbidden:
    raise SystemExit("Database driver leaked into the base: " + ", ".join(sorted(names & forbidden)))
print("Base dependency boundary: no PostgreSQL/MySQL/MongoDB drivers")
