CREATE OR REPLACE FUNCTION cfa_parse_rfc3339_utc_date(value TEXT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  normalized_value TEXT;
  parsed TIMESTAMPTZ;
BEGIN
  normalized_value := BTRIM(value);
  IF normalized_value IS NULL OR normalized_value = '' THEN
    RETURN NULL;
  END IF;
  IF normalized_value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$' THEN
    RETURN NULL;
  END IF;

  BEGIN
    parsed := REPLACE(normalized_value, 'Z', '+00:00')::TIMESTAMPTZ;
  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END;

  RETURN (parsed AT TIME ZONE 'UTC')::DATE;
END;
$$;

CREATE INDEX IF NOT EXISTS cfa_runs_occurred_date_idx
  ON cfa_runs (cfa_parse_rfc3339_utc_date(occurred_at));

CREATE INDEX IF NOT EXISTS cfa_runs_environment_occurred_date_idx
  ON cfa_runs (environment, cfa_parse_rfc3339_utc_date(occurred_at), occurred_at, run_url);

CREATE INDEX IF NOT EXISTS cfa_raw_failures_environment_occurred_date_idx
  ON cfa_raw_failures (environment, cfa_parse_rfc3339_utc_date(occurred_at), occurred_at, row_id);
