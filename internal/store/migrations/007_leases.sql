-- Leases let several managers share this database while only one of them
-- performs a given duty (for example reconciling). Token is a fencing token:
-- it increases whenever the lease changes hands.

CREATE TABLE leases (
    name       text PRIMARY KEY,
    holder     text NOT NULL,
    token      bigint NOT NULL CHECK (token >= 1),
    expires_at timestamptz NOT NULL
);
