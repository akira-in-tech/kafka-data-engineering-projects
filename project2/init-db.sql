CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

CREATE TABLE IF NOT EXISTS emp_cdc (
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

CREATE TRIGGER employee_cdc_trigger
AFTER INSERT OR UPDATE OR DELETE
ON employees
FOR EACH ROW
EXECUTE FUNCTION capture_employee_changes();

CREATE TABLE IF NOT EXISTS producer_state (
    state_key VARCHAR(100) PRIMARY KEY,
    state_value INT
);

INSERT INTO producer_state (state_key, state_value)
VALUES ('last_cdc_id', 0)
ON CONFLICT (state_key) DO NOTHING;