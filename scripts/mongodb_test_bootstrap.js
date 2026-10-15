// Dedicated disposable fixture; never run against a customer deployment.
try {
    rs.initiate({_id: "coldctlPhase5", members: [{_id: 0, host: "localhost:27017"}]});
} catch (error) {
    if (error.code !== 23) throw error; // Permit recovery after initialization only.
}
const deadline = Date.now() + 60000;
while (!db.hello().isWritablePrimary) {
    if (Date.now() >= deadline) throw new Error("replica set did not become primary");
    sleep(250);
}
const password = "coldctl_mongodb_fixture_only";
db.getSiblingDB("admin").createUser({user: "fixture_admin", pwd: password, roles: ["root"]});
db.getSiblingDB("admin").auth("fixture_admin", password);
db.getSiblingDB("coldctl_mongo_source").createUser({user: "archive_reader", pwd: password, roles: [{role: "read", db: "coldctl_mongo_source"}, {role: "clusterMonitor", db: "admin"}]});
db.getSiblingDB("coldctl_mongo_source").createUser({user: "deletion_writer", pwd: password, roles: [{role: "readWrite", db: "coldctl_mongo_source"}, {role: "clusterMonitor", db: "admin"}]});
db.getSiblingDB("coldctl_mongo_restore").createUser({user: "restore_writer", pwd: password, roles: [{role: "readWrite", db: "coldctl_mongo_restore"}, {role: "clusterMonitor", db: "admin"}]});
