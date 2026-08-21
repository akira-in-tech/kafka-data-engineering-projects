"""Load step: read employee messages off Kafka and write them into Postgres.

No functions here on purpose -- this is a single long-running loop, not a
service with reusable pieces.
"""

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

        # --- Write the fact row: one line per employee record consumed,
        # kept as-is for auditing/analytics beyond just the department totals.
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

        # --- Update the running per-department total via UPSERT: the first message for a
        # department INSERTs the row, every message after that hits the
        # ON CONFLICT branch and adds to the existing total. This avoids a
        # separate "SELECT to check if the row exists" round trip and is
        # atomic, so concurrent consumers (if we ever scale out) can't race.
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

        # Commit per message (not batched) so the fact row and the running
        # total always move together as one Postgres transaction -- never
        # a fact row with no matching total, or vice versa. Note this is
        # still at-least-once, not exactly-once: Kafka's offset auto-commit
        # runs on its own timer, independent of this DB commit, so a crash
        # in that window can replay a message the DB already applied.
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
