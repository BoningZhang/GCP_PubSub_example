import logging
import time
import multiprocessing

from google.cloud import pubsub_v1


ACK_DEADLINE_SECONDS         = 500 # 500 seconds to renew the ack window
ACK_DEADLINE_TIMES           = 20   # times to renuew the ack window
ACK_DEADLINE_BUFFER_SECONDS  = 200
ACK_SLEEP_INTERVALS_SECONDS  = 30
SCHEDULE_INTERVAL_SECONDS    = 60


def start_workers(fn, messages_info):
    workers = {}
    worker_num = 0
    manager = multiprocessing.Manager()
    return_vals_list = []

    for message in messages_info.received_messages:
        return_vals = manager.dict()
        return_vals_list.append(return_vals)

        worker = multiprocessing.Process(target=fn, args=(return_vals, message))

        msg_data = message.message.data
        # (ack id, message, count of renew window, worker #)
        workers[worker] = (message.ack_id, msg_data, 1, worker_num)

        logging.info("starting worker with message data %s..." % msg_data)
        worker.start()
        worker_num += 1

    return workers, return_vals_list


def process_worker_alive(workers, worker, subscriber, subscription_path, total_time):
    ack_id, msg_data, renew_count, worker_num = workers[worker]

    # should renew the ack window now
    if total_time + ACK_DEADLINE_BUFFER_SECONDS > renew_count * ACK_DEADLINE_SECONDS:
        # total ack time window would be roughly ACK_DEADLINE_SECONDS * (ACK_DEADLINE_TIMES + 1)
        if renew_count < ACK_DEADLINE_TIMES:
            subscriber.modify_ack_deadline(
                subscription_path,
                [ack_id],
                ack_deadline_seconds = ACK_DEADLINE_SECONDS
            )

            logging.info("renewing the ack window for msg %s for %d time with worker %d" % (
                msg_data, renew_count, worker_num))
            renew_count += 1
            workers[worker] = (ack_id, msg_data, renew_count, worker_num)
        else:
            # ack the message since it is already finished
            # deal with exit code in the future
            logging.info("trying to terminate worker thread %d with data %s since it misses sla..." % (worker_num, msg_data))
            worker.terminate()
            # pop the worker out to avoid dupe processing
            workers.pop(worker)

# we are using sync way to pull messages so we can renew the
# ack window before it becomes to outstanding
def process_messages(fn, subscriber, subscription_path):

    response = subscriber.pull(subscription_path,
                                   max_messages=8,
                                   return_immediately = True)
    if not response.received_messages:
        logging.debug("no message to consume...retry after schedule interval")
        return

    workers, return_vals_list = start_workers(fn, response)
    total_time                = 0

    while workers:
        for worker in list(workers):
            ack_id, msg_data, renew_count, worker_num = workers[worker]

            # total ack time window would be ACK_DEADLINE_SECONDS * ACK_DEADLINE_TIMES
            if worker.is_alive():
                process_worker_alive(workers, worker, subscriber,
                                     subscription_path, total_time)
            else:
                # ack the message since it is already finished
                # deal with exit code in the future
                return_info = return_vals_list[worker_num]
                status = return_info.get("status", consts.ERROR_UNKNOWN)
                err_msg = return_info.get("err_msg", "unknown")
                if status in [consts.DONE_STATUS]:
                    # task completed successfully, acknowledge the message
                    subscriber.acknowledge(subscription_path, [ack_id])
                else:
                    # task is not completed, do not acknowledge the message
                    logging.error("failed to process message error num %s err msg %s" % (status, err_msg))

                workers.pop(worker)
                logging.info("acked message \n%s\n with id %s return info \n%s" % (
                                msg_data, ack_id, return_info)
                            )

        if workers:
            time.sleep(ACK_SLEEP_INTERVALS_SECONDS)
            total_time += ACK_SLEEP_INTERVALS_SECONDS


def connect_to_queue():
    subscriber = pubsub_v1.SubscriberClient.from_service_account_json(consts.PUBSUB_INFO['gcp_keyfile'])
    subscription_path = subscriber.subscription_path(
                            consts.PUBSUB_INFO['project_id'],
                            consts.PUBSUB_INFO['subscription_name']
                        )

    return subscriber, subscription_path


def listen(fn):
    def subscriber_wrapper():
        subscriber, subscription_path = connect_to_queue()

        while True:
            process_messages(fn, subscriber, subscription_path)
            logging.info("\nSleeping for %s seconds...\n" % str(SCHEDULE_INTERVAL_SECONDS))
            time.sleep(SCHEDULE_INTERVAL_SECONDS)

    return subscriber_wrapper

#
