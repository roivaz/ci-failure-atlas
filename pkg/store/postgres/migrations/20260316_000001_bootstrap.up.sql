-- Bootstrap migration for PostgreSQL store scaffold.
CREATE TABLE IF NOT EXISTS cfa_meta_store_version (
  id INTEGER PRIMARY KEY,
  version TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO cfa_meta_store_version (id, version)
VALUES (1, 'postgres-scaffold-v1')
ON CONFLICT (id)
DO UPDATE SET version = EXCLUDED.version, updated_at = NOW();
