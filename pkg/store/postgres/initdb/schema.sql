CREATE TABLE IF NOT EXISTS cfa_meta_migrations (
  name TEXT PRIMARY KEY,
  dirty BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cfa_meta_store (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO cfa_meta_store (key, value)
VALUES ('schema_version', '0')
ON CONFLICT (key) DO NOTHING;
