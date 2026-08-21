import logging

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class KafkaTopicAdmin:
    # Topic provisioning lives here, separate from producer/consumer, so
    # it's a one-time (or safely re-runnable) setup step rather than
    # something baked into the streaming scripts themselves.

    def __init__(self, bootstrap_servers="localhost:29092"):
        self.bootstrap_servers = bootstrap_servers

    def create_topic(self, topic_name):
        admin = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="project2-admin"
        )

        try:
            # Single partition is enough here: CDC events for a given
            # emp_id need to stay in arrival order (no key-based
            # partitioning is used), and a single-broker local Kafka
            # doesn't benefit from a higher replication factor.
            topic = NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )

            admin.create_topics([topic])

            logger.info("Created topic: %s", topic_name)

        except TopicAlreadyExistsError:
            # Re-running admin.py after a restart shouldn't crash just
            # because the topic already exists.
            logger.info("Topic already exists: %s", topic_name)

        except Exception as exc:
            logger.exception("Failed to create topic: %s", exc)

        finally:
            admin.close()


if __name__ == "__main__":
    kafka_admin = KafkaTopicAdmin()
    kafka_admin.create_topic("employee_cdc")
