DROP INDEX IF EXISTS idx_audit_log_entity;
CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_table, entity_id);