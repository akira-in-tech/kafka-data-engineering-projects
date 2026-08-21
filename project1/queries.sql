-- Project 1: Kafka Employee Salary Analysis

-- 1. Verify employee data loaded by Kafka consumer
SELECT *
FROM department_employee;


-- 2. Verify total number of employee records
SELECT COUNT(*) AS total_employees
FROM department_employee;


-- 3. Total salary for each target department
SELECT
    department,
    SUM(salary) AS total_salary
FROM department_employee
WHERE department IN ('CIT', 'ECC', 'EMS')
GROUP BY department
ORDER BY department;


-- 4. Verify the aggregated result table
SELECT *
FROM department_employee_salary
ORDER BY department;