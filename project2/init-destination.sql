-- Destination DB. Deliberately just a plain mirror of the source's
-- `employees` shape -- no CDC table, trigger, or state table here, since
-- this side only ever receives changes, it never originates them.

CREATE TABLE IF NOT EXISTS employees (
    -- emp_id is a plain INT PRIMARY KEY, not SERIAL: the destination must
    -- keep the *same* id the source assigned (that's what the consumer's
    -- UPSERT/UPDATE/DELETE all key on), not generate its own sequence.
    emp_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);
