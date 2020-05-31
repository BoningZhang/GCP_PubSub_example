import logging
import json
import os

from internal.subscriber import listen

def record_job_status(message, status, err_msg, return_vals):
    # return_vals is for communication with worker
    return_vals['status'] = status
    return_vals['err_msg'] = err_msg

    # may also consider recording the job status in your mysql database
    update_job_status(message, status)  # not implement



@listen
def start_service(return_vals, message):
    try:
        logging.info("Processing message: \n%s" % message.message.data)

        status, err_msg = run_step1(message.message.data)
        if status!=None:
            logging.error("Failed to run step1!")
            record_job_status(message, status, err_msg, return_vals)
            return

        status, err_msg = run_step2(message.message.data)
        if status!=None:
            logging.error("Failed to run step2!")
            record_job_status(message, status, err_msg, return_vals)
            return

        logging.info("Message completely processed: \n%s" % message.message.data)
        # may also consider recording the job status in your mysql database
        update_job_status(message, "DONE")  # not implement

        return_vals['status'] = consts.DONE_STATUS
        return_vals['err_msg'] = ""
    except:
        logging.info("Process message failed: \n%s" % message.message.data)
        return_vals['status'] = consts.ERROR_UNKNOWN
        return_vals['err_msg'] = "unknown"

        # may also consider recording the job status in your mysql database
        update_job_status(message, "FAILED")  # not implement
        raise

if __name__=='__main__':
    try:
        # create console handler and set level to debug
        data_logger = logging.getLogger()
        data_logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        data_logger.addHandler(ch)

        start_service()
    except:
        logging.info("failed to start service...")
        raise

#
