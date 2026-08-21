-- Source DB. `employees` is the table apps would actually write to.
-- `emp_cdc` + the trigger below are what turn every INSERT/UPDATE/DELETE
-- on `employees` into an appendable, ordered change log that producer.py
-- can scan incrementally instead of re-diffing the whole table.

CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

CREATE TABLE IF NOT EXISTS emp_cdc (
    -- cdc_id is a SERIAL, not emp_id, on purpose: it's a strictly
    -- increasing sequence across *all* changes to *any* employee, which
    -- is exactly what producer.py needs as a resumable cursor
    -- ("WHERE cdc_id > last_seen").
    cdc_id SERIAL PRIMARY KEY,
    emp_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- One trigger function handles all three operations so there's a single
-- place that defines "what a change record looks like," instead of three
-- near-duplicate trigger functions to keep in sync.
CREATE OR REPLACE FUNCTION capture_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO emp_cdc (
            emp_id,
            first_name,
            last_name,
            dob,
            city,
            salary,
            action
        )
        VALUES (
            NEW.emp_id,
            NEW.first_name,
            NEW.last_name,
            NEW.dob,
            NEW.city,
            NEW.salary,
            'INSERT'
        );

        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO emp_cdc (
            emp_id,
            first_name,
            last_name,
            dob,
            city,
            salary,
            action
        )
        VALUES (
            NEW.emp_id,
            NEW.first_name,
            NEW.last_name,
            NEW.dob,
            NEW.city,
            NEW.salary,
            'UPDATE'
        );

        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        -- Uses OLD, not NEW: for a DELETE, NEW doesn't exist -- OLD is
        -- the only row data Postgres gives the trigger, and it's what we
        -- want anyway (the last known values before the row disappeared).
        INSERT INTO emp_cdc (
            emp_id,
            first_name,
            last_name,
            dob,
            city,
            salary,
            action
        )
        VALUES (
            OLD.emp_id,
            OLD.first_name,
            OLD.last_name,
            OLD.dob,
            OLD.city,
            OLD.salary,
            'DELETE'
        );

        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS employee_cdc_trigger ON employees;

-- AFTER, not BEFORE: we want to log the change once Postgres has already
-- committed to making it, not attempt to log a change that might still
-- get rejected by a later constraint check.
CREATE TRIGGER employee_cdc_trigger
AFTER INSERT OR UPDATE OR DELETE
ON employees
FOR EACH ROW
EXECUTE FUNCTION capture_employee_changes();

-- Durable cursor for the producer's position in emp_cdc. Living in the
-- source DB (not in the producer's memory) means restarting producer.py
-- resumes from where it left off instead of replaying every change.
CREATE TABLE IF NOT EXISTS producer_state (
    state_key VARCHAR(100) PRIMARY KEY,
    state_value INT
);

INSERT INTO producer_state (state_key, state_value)
VALUES ('last_cdc_id', 0)
ON CONFLICT (state_key) DO NOTHING;
