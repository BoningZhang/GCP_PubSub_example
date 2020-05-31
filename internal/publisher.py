import logging
from google.cloud import pubsub_v1

def get_callback(api_future, data):
    """Wrap message data in the context of the callback function."""

    def log_pub_result(api_future):
        try:
            logging.info(
                "Published message {} now has message ID {}".format(
                    data, api_future.result()
                )
            )
        except Exception:
            logging.info(
                "A problem occurred when publishing {}: {}\n".format(
                    data, api_future.exception()
                )
            )
            raise

    return log_pub_result


def publish_batch_messages(message_data_arr):
    """Publishes a message to a Pub/Sub topic"""
    # message_data should be in string
    logging.info("Publishing message...")

    # Configure batch settings
    batch_settings = pubsub_v1.types.BatchSettings(
        max_bytes=10240, max_latency=60  # Ten kilobyte  # One minutes
    )

    # Initialize a Publisher client.
    client = pubsub_v1.PublisherClient.from_service_account_json(
        filename=consts.PUBSUB_INFO['gcp_keyfile'],
        batch_settings=batch_settings
    )
    # [END pubsub_quickstart_pub_client]
    # Create a fully qualified identifier in the form of
    # `projects/{project_id}/topics/{topic_name}`
    topic_path = client.topic_path(consts.PUBSUB_INFO['project_id'], consts.PUBSUB_INFO['topic_name'])

    for message_data in message_data_arr:
        # When you publish a message, the client returns a future.
        api_future = client.publish(topic_path, data=message_data.encode(‘utf-8’))
        api_future.add_done_callback(get_callback(api_future, message_data))

    logging.info("Published messages successfully!")

#
