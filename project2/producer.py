import json
import logging
import time

import psycopg2
from kafka import KafkaProducer

from employee import EmployeeChange


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class CDCProducer:

    def __init__(self):
        self.topic = "employee_cdc"

        self.db_conn = psycopg2.connect(
            host="127.0.0.1",
            port=5434,
            database="source_db",
            user="postgres",
            password="postgres"
        )

        self.producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda value:
                json.dumps(value).encode("utf-8")
        )

    def get_last_cdc_id(self):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT state_value
                FROM producer_state
                WHERE state_key = 'last_cdc_id'
                """
            )

            row = cursor.fetchone()

            return row[0] if row else 0

    def save_last_cdc_id(self, cdc_id):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO producer_state (
                    state_key,
                    state_value
                )
                VALUES ('last_cdc_id', %s)
                ON CONFLICT (state_key)
                DO UPDATE SET state_value = EXCLUDED.state_value
                """,
                (cdc_id,)
            )

        self.db_conn.commit()

    def get_max_cdc_id(self):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(MAX(cdc_id), 0)
                FROM emp_cdc
                """
            )

            return cursor.fetchone()[0]

    def send_snapshot(self):
        logger.info("Starting snapshot")

        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    emp_id,
                    first_name,
                    last_name,
                    dob,
                    city,
                    salary
                FROM employees
                ORDER BY emp_id
                """
            )

            rows = cursor.fetchall()

        for row in rows:
            snapshot = {
                "cdc_id": 0,
                "emp_id": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "dob": str(row[3]) if row[3] else None,
                "city": row[4],
                "salary": row[5],
                "action": "SNAPSHOT"
            }

            self.producer.send(
                self.topic,
                value=snapshot
            )

            logger.info(
                "Sent SNAPSHOT | employee %s",
                row[0]
            )

        self.producer.flush()

        logger.info(
            "Snapshot completed | %s employees",
            len(rows)
        )

    def get_changes(self):
        last_cdc_id = self.get_last_cdc_id()

        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    cdc_id,
                    emp_id,
                    first_name,
                    last_name,
                    dob,
                    city,
                    salary,
                    action
                FROM emp_cdc
                WHERE cdc_id > %s
                ORDER BY cdc_id
                """,
                (last_cdc_id,)
            )

            return cursor.fetchall()

    def send_change(self, row):
        change = EmployeeChange(
            cdc_id=row[0],
            emp_id=row[1],
            first_name=row[2],
            last_name=row[3],
            dob=str(row[4]) if row[4] else None,
            city=row[5],
            salary=row[6],
            action=row[7]
        )

        self.producer.send(
            self.topic,
            value=change.to_dict()
        )

        self.producer.flush()

        self.save_last_cdc_id(change.cdc_id)

        logger.info(
            "Sent CDC %s | employee %s | %s",
            change.cdc_id,
            change.emp_id,
            change.action
        )

    def run(self):
        logger.info("CDC Producer started")

        try:
            last_cdc_id = self.get_last_cdc_id()

            # First startup: perform initial snapshot.
            if last_cdc_id == 0:
                max_cdc_before_snapshot = self.get_max_cdc_id()

                self.send_snapshot()

                # Existing CDC records are already represented
                # by the current snapshot.
                self.save_last_cdc_id(max_cdc_before_snapshot)

                logger.info(
                    "CDC position initialized to %s",
                    max_cdc_before_snapshot
                )

            while True:
                rows = self.get_changes()

                for row in rows:
                    self.send_change(row)

                time.sleep(0.2)

        except KeyboardInterrupt:
            logger.info("CDC Producer stopped")

        except Exception as exc:
            logger.exception(
                "Producer error: %s",
                exc
            )

        finally:
            self.producer.close()
            self.db_conn.close()


if __name__ == "__main__":
    producer = CDCProducer()
    producer.run()