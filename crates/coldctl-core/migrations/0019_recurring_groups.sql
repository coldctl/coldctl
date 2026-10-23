CREATE TABLE recurring_groups (
 id TEXT PRIMARY KEY,
 config_json TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('active','failed','stopped')),
 phase TEXT NOT NULL CHECK(phase IN ('waiting','archive','delete')),
 interval_seconds INTEGER NOT NULL,
 next_due INTEGER NOT NULL,
 cycle INTEGER NOT NULL DEFAULT 0,
 cycles INTEGER NOT NULL DEFAULT 0,
 position INTEGER NOT NULL DEFAULT 0,
 current_queue TEXT REFERENCES archive_queue(id),
 last_error TEXT
);
CREATE TABLE recurring_group_reservations (
 policy_id TEXT PRIMARY KEY REFERENCES policies(id),
 group_id TEXT NOT NULL REFERENCES recurring_groups(id)
);
CREATE TABLE recurring_group_members (
 group_id TEXT NOT NULL REFERENCES recurring_groups(id),
 cycle INTEGER NOT NULL,
 ordinal INTEGER NOT NULL,
 queue_id TEXT NOT NULL UNIQUE REFERENCES archive_queue(id),
 PRIMARY KEY(group_id,cycle,ordinal)
);
