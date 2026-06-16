-- vm_audit_events stores KubeVirt VirtualMachine mutations sourced from the
-- kube-apiserver audit log.
--
-- audit_id is the per-request UUID assigned by the kube-apiserver
-- (audit.k8s.io/v1 Event.AuditID).  It is stable across Kafka redelivery
-- and Vector restarts, making it safe to use as PRIMARY KEY for idempotent
-- ON CONFLICT DO NOTHING upserts.

CREATE TABLE IF NOT EXISTS vm_audit_events (
    audit_id              TEXT        PRIMARY KEY,
    verb                  TEXT        NOT NULL,
    stage                 TEXT        NOT NULL,
    namespace             TEXT,
    name                  TEXT,
    uid                   TEXT,
    resource_version      TEXT,
    response_code         INT,
    request_received_at   TIMESTAMPTZ NOT NULL,
    stage_timestamp       TIMESTAMPTZ NOT NULL,
    username              TEXT,
    request_object        JSONB,
    response_object       JSONB,
    -- raw stores the full canonical audit event JSON for forensic queries.
    raw                   JSONB       NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vm_audit_ns_name
    ON vm_audit_events (namespace, name, stage_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_vm_audit_verb
    ON vm_audit_events (verb, stage_timestamp DESC);
