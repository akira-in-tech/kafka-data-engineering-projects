import json
import logging

import psycopg2
from kafka import KafkaConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class CDCConsumer:

    def __init__(self):
        self.topic = "employee_cdc"

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers="localhost:29092",
            group_id="employee-sync-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda message:
                json.loads(message.decode("utf-8"))
        )

        self.db_conn = psycopg2.connect(
            host="127.0.0.1",
            port=5435,
            database="destination_db",
            user="postgres",
            password="postgres"
        )

    def process_change(self, change):
        action = change["action"]

        with self.db_conn.cursor() as cursor:

            if action in ("INSERT", "SNAPSHOT"):
                cursor.execute(
                    """
                    INSERT INTO employees (
                        emp_id,
                        first_name,
                        last_name,
                        dob,
                        city,
                        salary
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (emp_id)
                    DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        dob = EXCLUDED.dob,
                        city = EXCLUDED.city,
                        salary = EXCLUDED.salary
                    """,
                    (
                        change["emp_id"],
                        change["first_name"],
                        change["last_name"],
                        change["dob"],
                        change["city"],
                        change["salary"]
                    )
                )

            elif action == "UPDATE":
                cursor.execute(
                    """
                    UPDATE employees
                    SET
                        first_name = %s,
                        last_name = %s,
                        dob = %s,
                        city = %s,
                        salary = %s
                    WHERE emp_id = %s
                    """,
                    (
                        change["first_name"],
                        change["last_name"],
                        change["dob"],
                        change["city"],
                        change["salary"],
                        change["emp_id"]
                    )
                )

            elif action == "DELETE":
                cursor.execute(
                    """
                    DELETE FROM employees
                    WHERE emp_id = %s
                    """,
                    (change["emp_id"],)
                )

            else:
                logger.warning(
                    "Unknown action: %s",
                    action
                )
                return

        self.db_conn.commit()

        logger.info(
            "Processed CDC %s | employee %s | %s",
            change["cdc_id"],
            change["emp_id"],
            action
        )

    def run(self):
        logger.info("CDC Consumer started")

        try:
            for message in self.consumer:
                self.process_change(message.value)

        except KeyboardInterrupt:
            logger.info("CDC Consumer stopped")

        except Exception as exc:
            logger.exception("Consumer error: %s", exc)

        finally:
            self.consumer.close()
            self.db_conn.close()


if __name__ == "__main__":
    consumer = CDCConsumer()
    consumer.run()