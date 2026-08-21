import logging

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class KafkaTopicAdmin:
    """Creates Kafka topics for project1 (setup step, run before producer/consumer)."""
    # Topic provisioning is kept separate from producer/consumer so it can
    # be run once (or safely re-run) as a setup step, instead of coupling
    # topic creation to whichever script happens to run first.

    def __init__(self, bootstrap_servers="localhost:9092"):
        """Remember which Kafka broker to talk to."""
        self.bootstrap_servers = bootstrap_servers

    def create_topic(self, topic_name):
        """Create `topic_name` if it doesn't exist yet; no-op if it already does."""
        admin = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="project1-admin"
        )

        try:
            # Single partition / replication factor 1: this is a local,
            # single-broker demo, not a throughput or fault-tolerance
            # exercise, so there's no need for more.
            topic = NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )

            admin.create_topics([topic])

            logger.info("Created topic: %s", topic_name)

        except TopicAlreadyExistsError:
            # Re-running admin.py (e.g. after restarting the pipeline)
            # should be a no-op, not a crash.
            logger.info("Topic already exists: %s", topic_name)

        except Exception as exc:
            logger.exception("Failed to create topic: %s", exc)

        finally:
            admin.close()


if __name__ == "__main__":
    kafka_admin = KafkaTopicAdmin()
    kafka_admin.create_topic("employee_salary")
