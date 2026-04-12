CREATE OR REPLACE FUNCTION func_audit_log()
RETURNS TRIGGER AS $$
DECLARE
    entity_id TEXT;
BEGIN
    entity_id := CASE
        WHEN TG_TABLE_NAME = 'dim_authors'    THEN COALESCE(NEW.author_id,    OLD.author_id)
        WHEN TG_TABLE_NAME = 'dim_publishers' THEN COALESCE(NEW.publisher_id, OLD.publisher_id)
        WHEN TG_TABLE_NAME = 'dim_books'      THEN COALESCE(NEW.book_id,      OLD.book_id)
        WHEN TG_TABLE_NAME = 'dim_users'      THEN COALESCE(NEW.user_id,      OLD.user_id)
        WHEN TG_TABLE_NAME = 'fact_ratings'   THEN COALESCE(NEW.rating_id,    OLD.rating_id)
    END;

    INSERT INTO public.audit_log (
        entity_table,
        entity_id,
        action,
        changed_by,
        old_data,
        new_data,
        ip_address
    )
    VALUES (
        TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
        entity_id,
        TG_OP,
        current_setting('audit.user_id', true)::INT,
        CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END,
        CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE to_jsonb(NEW) END,
        inet_client_addr()
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;