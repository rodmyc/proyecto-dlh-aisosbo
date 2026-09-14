CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS history;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS etl.ingestion_batch (
    batch_id uuid PRIMARY KEY,
    dataset text NOT NULL,
    source_filename text NOT NULL,
    file_sha256 char(64) NOT NULL,
    file_size_bytes bigint NOT NULL CHECK (file_size_bytes >= 0),
    status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    rows_read bigint NOT NULL DEFAULT 0,
    rows_inserted bigint NOT NULL DEFAULT 0,
    rows_updated bigint NOT NULL DEFAULT 0,
    rows_unchanged bigint NOT NULL DEFAULT 0,
    rows_rejected bigint NOT NULL DEFAULT 0,
    error_message text,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (dataset, file_sha256)
);

CREATE INDEX IF NOT EXISTS ingestion_batch_status_started_idx
    ON etl.ingestion_batch (status, started_at DESC);

CREATE TABLE IF NOT EXISTS raw.salesforce_record_current (
    object_type text NOT NULL,
    salesforce_id text NOT NULL,
    payload jsonb NOT NULL,
    record_hash char(64) NOT NULL,
    first_batch_id uuid NOT NULL REFERENCES etl.ingestion_batch(batch_id),
    last_batch_id uuid NOT NULL REFERENCES etl.ingestion_batch(batch_id),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object_type, salesforce_id)
);

CREATE INDEX IF NOT EXISTS salesforce_record_payload_gin_idx
    ON raw.salesforce_record_current USING gin (payload);

CREATE TABLE IF NOT EXISTS raw.salesforce_opportunity_current (
    id_opportunity text PRIMARY KEY,
    id_contact text,
    id_account text,
    id_campaign text,
    id_recurring_donation text,
    amount numeric(18, 4),
    close_date date,
    collection_date date,
    accounting_date date,
    stage_name text NOT NULL,
    status_group text NOT NULL CHECK (status_group IN ('cobrada', 'perdida', 'pendiente', 'otro')),
    is_closed boolean,
    payload jsonb NOT NULL,
    record_hash char(64) NOT NULL,
    first_batch_id uuid NOT NULL REFERENCES etl.ingestion_batch(batch_id),
    last_batch_id uuid NOT NULL REFERENCES etl.ingestion_batch(batch_id),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS opportunity_contact_idx
    ON raw.salesforce_opportunity_current (id_contact);
CREATE INDEX IF NOT EXISTS opportunity_account_idx
    ON raw.salesforce_opportunity_current (id_account);
CREATE INDEX IF NOT EXISTS opportunity_close_date_idx
    ON raw.salesforce_opportunity_current (close_date);
CREATE INDEX IF NOT EXISTS opportunity_status_idx
    ON raw.salesforce_opportunity_current (status_group, stage_name);

CREATE TABLE IF NOT EXISTS history.salesforce_opportunity_status (
    status_version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_opportunity text NOT NULL,
    stage_name text NOT NULL,
    status_group text NOT NULL,
    amount numeric(18, 4),
    id_contact text,
    id_account text,
    close_date date,
    valid_from timestamptz NOT NULL DEFAULT now(),
    valid_to timestamptz,
    batch_id uuid NOT NULL REFERENCES etl.ingestion_batch(batch_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS opportunity_status_one_current_idx
    ON history.salesforce_opportunity_status (id_opportunity)
    WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS opportunity_status_as_of_idx
    ON history.salesforce_opportunity_status (id_opportunity, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS raw.historical_donation (
    id_donation text PRIMARY KEY,
    id_salesforce text,
    amount numeric(18, 4),
    frequency text,
    collection_date date NOT NULL,
    person_number text,
    external_numbers jsonb,
    friend_id text,
    psn text,
    psn_c text,
    source_origin text,
    record_hash char(64) NOT NULL,
    first_batch_id uuid NOT NULL REFERENCES etl.ingestion_batch(batch_id),
    last_batch_id uuid NOT NULL REFERENCES etl.ingestion_batch(batch_id),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS historical_donation_contact_idx
    ON raw.historical_donation (id_salesforce);
CREATE INDEX IF NOT EXISTS historical_donation_collection_date_idx
    ON raw.historical_donation (collection_date);

CREATE OR REPLACE VIEW analytics.donor_current AS
SELECT
    salesforce_id AS donor_id,
    CASE object_type WHEN 'contacts' THEN 'contact' ELSE 'account' END AS donor_type,
    payload ->> 'NAME' AS donor_name,
    payload ->> 'FIRSTNAME' AS first_name,
    payload ->> 'LASTNAME' AS last_name,
    payload ->> 'EMAIL' AS email,
    COALESCE(payload ->> 'MOBILEPHONE', payload ->> 'PHONE') AS phone,
    last_seen_at
FROM raw.salesforce_record_current
WHERE object_type IN ('contacts', 'accounts');

CREATE OR REPLACE VIEW analytics.fact_donation_kardex AS
SELECT
    'historical:' || id_donation AS donation_key,
    'historical'::text AS source_system,
    id_donation AS source_id,
    id_salesforce AS donor_id,
    'contact'::text AS donor_type,
    amount,
    collection_date AS movement_date,
    collection_date,
    EXTRACT(YEAR FROM collection_date)::integer AS management_year,
    'Cobrada'::text AS stage_name,
    'cobrada'::text AS status_group,
    true AS is_effective,
    frequency,
    last_batch_id
FROM raw.historical_donation
UNION ALL
SELECT
    'salesforce:' || id_opportunity AS donation_key,
    'salesforce'::text AS source_system,
    id_opportunity AS source_id,
    COALESCE(id_contact, id_account) AS donor_id,
    CASE WHEN id_contact IS NOT NULL THEN 'contact' ELSE 'account' END AS donor_type,
    amount,
    CASE
        WHEN status_group = 'cobrada'
            THEN COALESCE(accounting_date, collection_date, close_date)
        ELSE close_date
    END AS movement_date,
    collection_date,
    EXTRACT(YEAR FROM close_date)::integer AS management_year,
    stage_name,
    status_group,
    status_group = 'cobrada' AS is_effective,
    payload ->> 'FRECUENCIA__C' AS frequency,
    last_batch_id
FROM raw.salesforce_opportunity_current;

CREATE OR REPLACE VIEW analytics.fact_effective_payment AS
SELECT *
FROM analytics.fact_donation_kardex
WHERE is_effective;

CREATE OR REPLACE VIEW analytics.donor_kardex AS
SELECT
    fact.*,
    donor.donor_name,
    donor.first_name,
    donor.last_name
FROM analytics.fact_donation_kardex AS fact
LEFT JOIN analytics.donor_current AS donor
    ON donor.donor_id = fact.donor_id
    AND donor.donor_type = fact.donor_type;
