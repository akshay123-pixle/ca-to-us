import time
import logging
import os
import boto3

import config
import connections

logLevel = (os.getenv("logLevel", "INFO")).upper()
STAGE = (os.getenv("stage", "TEST")).upper()
SLEEP_TIMER = int(os.getenv("sleep_timer_CA_TO_US", 60))
CA_TO_US_BATCH_PATH = os.getenv("CA_TO_US_BATCH_PATH")

logging.basicConfig(
    level=logging.INFO if logLevel == "INFO" else logging.ERROR,
    datefmt="%H:%M:%S",
    format="%(levelname)s: %(module)s:%(funcName)s:%(lineno)d: %(asctime)s: %(message)s",
)
LOG = logging.getLogger(__name__)


def execute_batch(vin=None):
    """
    Execute the batch on the server
    :param vin : string, vin to be processed
    :return : a boolean , whether the batch execution is successful or not 
    """

    assert vin is not None, "Please provide proper VIN"

    try:
        LOG.info("Making changes in local file and preparing for upload..")
        local_changes = change_local_files_batch_execution(vin)
        assert local_changes is not False, "File changes failed for Upload"

        server_connection_flag, client = connections.server_connection(STAGE)
        assert server_connection_flag is not 0, "Server connection not established"

        execute_batch = upload_and_execute_batch(client)
        assert (execute_batch is not False), "Error occurred in executing batch for Factory Feed"
        time.sleep(SLEEP_TIMER)
        return execute_batch, False

    except Exception as e:
        LOG.error(f"Error Occurred : {str(e)}")
        return False, e


def change_local_files_batch_execution(vin):

    """
    create a local file with vin to upload on the server
    :param vin : string ,vin to write on the local file
    :return update_flag : boolean , whether the file is successfully created or not
    """

    txt_file_write = False
    update_flag = False
    try:
        file_to_write_vin = config.filetowrite_vin
        data_to_write = vin
        txt_file_write = write_files(data_to_write, file_to_write_vin)
        if txt_file_write:
            LOG.info("Vin changed in Local .txt file")
            update_flag = True
    except Exception as e:
        LOG.error(f"Error Occurred : {str(e)}")
        update_flag = False
    return update_flag


def upload_and_execute_batch(client):
    """
    Upload the file created on the server and run the  batch from the specified location
    :param client: object, connection to server
    :return execution_script_flag : boolean , whether the batch is sucessfully executed or not
    """

    execution_script_flag = False

    sftp = client.open_sftp()

    LOG.info("Uploading Destination-CA-US-panaupdate-query.txt on server")

    try:
        file_upload_path = config.file_path
        sftp.put(config.filetowrite_vin, config.file_path)
        upload_to_server_flag = True
        LOG.info("Files uploaded to server ")
    except Exception as e:
        LOG.error(e)
        LOG.error("Problem while uploading file to the server")
        upload_to_server_flag = False
        execution_script_flag = False

    if upload_to_server_flag:
        LOG.info("Executing the Script...")
        try:
            stdin, stdout, stderr = client.exec_command("(" + CA_TO_US_BATCH_PATH + ")")

            error = stderr.readlines()
            if len(error) > 0:
                LOG.error(f"Errors while execution : {str(error)}")
            LOG.info(f"Output of execution : {str(stdout.readlines())}")
            if error:
                execution_script_flag = False
            else:
                execution_script_flag = True
        except Exception as e:
            LOG.error(e)
            LOG.error("Problem executing the Script")
            execution_script_flag = False
    sftp.close()
    LOG.info("Closing client connection...")
    client.close()
    LOG.info("Client connection closed")
    return execution_script_flag


def write_files(data, file):
    """
    Creates local file in lambda at run time

    :param data: payload of the file
    :param file: file path
    :return: boolean True for success False otherwise
    """
    try:
        with open(file, "w") as file_to_write:
            file_to_write.writelines(data)
        LOG.info("Local file created")
        return True
    except Exception as e:
        LOG.error(e)
        LOG.error("Problem occurred while creating local file ")
        return False
