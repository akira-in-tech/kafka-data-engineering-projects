-- Schema mirrors the project spec directly:
--   department_employee        -- one row per (filtered, transformed) record
--   department_employee_salary -- running total per department

CREATE TABLE IF NOT EXISTS department_employee (
    department VARCHAR(200),
    department_division VARCHAR(200),
    position_title VARCHAR(200),
    hire_date DATE,
    salary DECIMAL
);

CREATE TABLE IF NOT EXISTS department_employee_salary (
    department VARCHAR(200) NOT NULL,
    total_salary INT4 NULL,
    -- department is the natural key: it's exactly what consumer.py's
    -- "ON CONFLICT (department)" upsert targets to accumulate totals.
    CONSTRAINT department_employee_salary_pk PRIMARY KEY (department)
);
