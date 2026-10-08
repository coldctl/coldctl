// Dedicated disposable fixture; never run against a customer deployment.
rs.initiate({_id: "coldctlPhase5", members: [{_id: 0, host: "localhost:27017"}]});
assert.soon(() => db.hello().isWritablePrimary, "replica set primary", 60000);
const password = "coldctl_mongodb_fixture_only";
db.getSiblingDB("admin").createUser({user: "fixture_admin", pwd: password, roles: ["root"]});
db.getSiblingDB("admin").auth("fixture_admin", password);
db.getSiblingDB("coldctl_mongo_source").createUser({user: "archive_reader", pwd: password, roles: [{role: "read", db: "coldctl_mongo_source"}, {role: "clusterMonitor", db: "admin"}]});
db.getSiblingDB("coldctl_mongo_restore").createUser({user: "restore_writer", pwd: password, roles: [{role: "readWrite", db: "coldctl_mongo_restore"}, {role: "clusterMonitor", db: "admin"}]});
