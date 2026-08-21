import json
import psycopg2
from kafka import KafkaConsumer


TOPIC = "employee_salary"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="employee_salary_group",
    value_deserializer=lambda message: json.loads(message.decode("utf-8"))
)


conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    database="kafka_project",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()

print("Consumer is waiting for messages...")


try:
    for message in consumer:
        employee = message.value

        cur.execute(
            """
            INSERT INTO department_employee
            (department, department_division, position_title, hire_date, salary)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                employee["department"],
                employee["department_division"],
                employee["position_title"],
                employee["hire_date"],
                employee["salary"]
            )
        )

        cur.execute(
            """
            INSERT INTO department_employee_salary
            (department, total_salary)
            VALUES (%s, %s)
            ON CONFLICT (department)
            DO UPDATE SET total_salary =
                department_employee_salary.total_salary + EXCLUDED.total_salary
            """,
            (
                employee["department"],
                employee["salary"]
            )
        )

        conn.commit()

        print(
            employee["department"],
            employee["position_title"],
            employee["salary"]
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    cur.close()
    conn.close()
    consumer.close()