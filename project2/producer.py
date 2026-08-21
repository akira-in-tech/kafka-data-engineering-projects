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
    """Reads change events out of the source DB's emp_cdc table and ships them to Kafka.

    Job breakdown, method by method:
      __init__            -- open the source-DB connection and Kafka producer
      get_last_cdc_id      -- where did we leave off? (read the saved cursor)
      save_last_cdc_id     -- remember where we got to (write the cursor)
      get_max_cdc_id        -- what's the newest cdc_id in emp_cdc right now?
      send_snapshot          -- one-time: ship every existing `employees` row
      get_changes             -- fetch any emp_cdc rows we haven't sent yet
      send_change               -- ship one change row, then advance the cursor
      run                        -- the entry point: snapshot once, then loop forever
    """
    # Design choice: trigger-populated CDC table, not polling `employees`
    # directly (the PDF's other suggested approach). Polling can only ever
    # notice new rows -- there's no way to tell an UPDATE or DELETE
    # happened just by re-scanning the table -- so it can't satisfy the
    # "any insert/update/delete must replicate" requirement on its own.

    def __init__(self):
        """Open the source-DB connection and Kafka producer this instance will reuse."""
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
        """Return the cdc_id this producer last sent (0 if it's never run before)."""
        # Position is persisted in the source DB (producer_state), not
        # just held in memory, so a restarted producer resumes from where
        # it left off instead of replaying the entire CDC table.
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
        """Persist `cdc_id` as the new cursor position, so a restart can resume from here."""
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
        """Return the highest cdc_id currently in emp_cdc (0 if the table is empty)."""
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(MAX(cdc_id), 0)
                FROM emp_cdc
                """
            )

            return cursor.fetchone()[0]

    def send_snapshot(self):
        """Send every current `employees` row to Kafka as a one-time SNAPSHOT message."""
        # Snapshot phase: before we start streaming incremental changes,
        # the destination needs a full copy of whatever already exists in
        # `employees` -- otherwise rows that predate the pipeline running
        # would never show up on the other side.
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
                # Its own action label (not "INSERT") so the consumer -- or
                # anyone reading the topic -- can tell "initial load" apart
                # from a genuine incremental insert.
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
        """Fetch every emp_cdc row newer than the saved cursor, oldest first."""
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
        """Send one emp_cdc row to Kafka, then advance the saved cursor past it."""
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

        # Only advance the saved position after the message is actually
        # flushed to Kafka. If we crashed between the send() and here, the
        # position wouldn't move, and we'd simply resend that change next
        # time -- resending is safe (the consumer's writes are idempotent
        # upserts/keyed deletes), silently skipping a change is not.
        self.save_last_cdc_id(change.cdc_id)

        logger.info(
            "Sent CDC %s | employee %s | %s",
            change.cdc_id,
            change.emp_id,
            change.action
        )

    def run(self):
        """Entry point: snapshot once on first run, then poll emp_cdc forever."""
        logger.info("CDC Producer started")

        try:
            last_cdc_id = self.get_last_cdc_id()

            # First startup: perform initial snapshot.
            if last_cdc_id == 0:
                # Capture the CDC high-water mark *before* sending the
                # snapshot, not after. Anything written to emp_cdc while
                # the snapshot is in flight is already reflected in the
                # `employees` rows we just read, but the streaming loop
                # below still needs to see and replay it -- so we save
                # this pre-snapshot value rather than the current max,
                # which would let those in-flight changes slip through.
                max_cdc_before_snapshot = self.get_max_cdc_id()

                self.send_snapshot()

                self.save_last_cdc_id(max_cdc_before_snapshot)

                logger.info(
                    "CDC position initialized to %s",
                    max_cdc_before_snapshot
                )

            while True:
                rows = self.get_changes()

                for row in rows:
                    self.send_change(row)

                # Short poll interval keeps replication close to
                # real-time (spec target: < 1 sec) without turning this
                # into a tight busy loop hammering Postgres.
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
