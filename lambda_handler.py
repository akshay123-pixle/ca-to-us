import boto3
from boto3 import client
import logging
import json
import os

import batch_execute
import dao
import connections


root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logLevel = os.getenv("logLevel", "INFO").upper()
logging.basicConfig(
    level=(logging.INFO if logLevel == "INFO" else logging.ERROR),
    datefmt="%H:%M:%S",
    format="%(levelname)s: %(module)s:%(funcName)s:%(lineno)d: %(asctime)s: %(message)s",
)
LOG = logging.getLogger(__name__)

STAGE = os.getenv("stage", "PROD").upper()
driveIT_Update_lambda = os.getenv("driveIT_update_lambda")
TBM_factory_feed_lambda = os.getenv("TBM_factory_feed_lambda")
VP4R_factory_feed_lambda = os.getenv("VP4R_factory_feed_lambda")
USE_CASE = "KB0014169 : CA_TO_US"
SUCCESS = "Success"
FAILED = "Failed"
# CLOSE_NOTES = "close_notes"
WORK_NOTES = "work_notes"
# INCIDENT_ROUTE = "incidentRoute"
INCIDENT_UPDATE = "incidentUpdate"
# NO_FILE_FOUND = "No file found"
VIN = "vin"
INCIDENT = "incident"
CHECK_FF_FIRST = "check_ff_first"


def main(event, context):
    """
    :param event :event with Vin and incident number
    :param context :
    :return Output if the vehicle is TBM/VP4R and dataitem that contains Vin,incidentnumber and check_ff_first
    """
    LOG.info(event)
    vin = None
    incident_number = None
    status = None
    message = None
    vehicle_type = None
    check_ff_first = False

    if type(event) == str:
        event_obj = json.loads(event)
    else:
        event_obj = event

    if VIN in event_obj and INCIDENT in event_obj:
        vin = event_obj[VIN]
        incident_number = event_obj[INCIDENT]
        vin = vin.strip()
        execution_flag = True
    else:
        execution_flag = False
        LOG.info("VIN or Incident id not provided")

    if execution_flag and vin:
        execution_result, response_code = ca_to_us_conversion(vin)
        if response_code == SUCCESS:
            vehicle_type = execution_result
            result = {
                "vin": vin,
                "incident": incident_number,
                "vehicle_type": vehicle_type,
                "check_ff_first": check_ff_first
            }
            LOG.info(result)
            status = SUCCESS
            if result["vehicle_type"] == "TBM":
                invoke_tbm_factory_feed_lambda(json.dumps(result))
            elif result["vehicle_type"] == "VP4R":
                invoke_vp4r_factory_feed_lambda(json.dumps(result))


        elif response_code == FAILED:
            update_work_notes = {WORK_NOTES: execution_result}
            LOG.info(update_work_notes)
            message = update_work_notes
            status = FAILED

    else:
        update_work_notes = {
            WORK_NOTES: "Required data not passed or does not meet the criteria to execute Factory Feed" + str(
                event_obj)}
        LOG.info(update_work_notes)
        status = FAILED
        message = update_work_notes

    if status == FAILED:
        result = {"vin": vin, "status": status, "message": message}

        LOG.info(f"Result is {json.dumps(result)}")

        body = {"result": result}

        incident_status = INCIDENT_UPDATE
        response = {
            "eventType": incident_status,
            "incident": incident_number,
            "body": json.dumps(body),
        }

        LOG.info(response)
        invoke_lambda(json.dumps(response))
        return response


def ca_to_us_conversion(vin):
    final_result = None
    response_code = None
    error = None
    LOG.info(f"Stage: {STAGE}")
    try:
        LOG.info("Connecting to DB2...")
        db_connection = connections.database_connection("CVP_" + STAGE)
        vin_in_vehicle, is_tbm, is_vp4r = dao.check_vehicle_table(db_connection, vin)
        if vin_in_vehicle:
            if not(is_tbm or is_vp4r):
                LOG.info("This VIN " + vin + " is neither a TBM nor VP4R.Therefore batch execution will fail.")
                final_result = (f"The VIN {vin} is neither a TBM nor VP4R.Therefore batch execution will fail")
                response_code = FAILED
            else :
                before_check = dao.check_vehicle_table_for_conversion(db_connection, vin)
                if before_check:
                    LOG.info("The VIN " + vin + "is already US. Therefore batch execution will not happen.")
                    final_result = (f"The VIN {vin} is already US. Therefore batch execution will not happen.")
                    response_code = FAILED
                else:
                 try:
                     batch_execution, execution_error = batch_execute.execute_batch(vin)
                     if batch_execution:
                         vin_conversion_check = dao.check_vehicle_table_for_conversion(db_connection, vin)
                         if vin_conversion_check:
                             if is_tbm:
                                 LOG.info("This VIN " + vin + " is a TBM vin ")
                                 final_result = "TBM"
                                 response_code = SUCCESS
                             elif is_vp4r:
                                 LOG.info("This VIN " + vin + " is a VP4R vin ")
                                 final_result = "VP4R"
                                 response_code = SUCCESS
                         else:
                             LOG.info(f"conversion for VIN {vin} is incomplete even after batch execution ")
                             final_result = (f"conversion for VIN {vin} is incomplete even after batch execution ")
                             response_code = FAILED
                     else:
                         LOG.error(execution_error)
                         final_result = str(execution_error)
                         response_code = FAILED
                 except Exception as e:
                     LOG.error(f"Error Occurred {str(e)}")
                     final_result = str(e)
                     response_code = FAILED
                 finally:
                     LOG.info("Closing Connection")
                     connections.close_connection(db_connection)
                     LOG.info("Connection Closed")
                     LOG.info(f"Final result : {final_result}")
                     return final_result, response_code
        else:
            LOG.info("The vin " + vin + " is not found in the vehicle table. please check the vin again.")
            final_result = (f"The VIN {vin} is not found in the vehicle table. please check the vin again.")
            response_code = FAILED
    except Exception as e:
        LOG.error(e)
        final_result = str(e)
        response_code = FAILED
    finally:
        LOG.info(final_result)
        return final_result, response_code

        
def invoke_lambda(data_item):
    """
    Invokes driveIT wrapper lambda for updating the incident
    :param data_item: payload for invoking lambda
    :return:
    """
    lambda_client = client("lambda", region_name="us-east-1")
    LOG.info(f"Invoking Lambda {driveIT_Update_lambda}")
    lambda_client.invoke(
        FunctionName=driveIT_Update_lambda,
        InvocationType="Event",
        Payload=json.dumps(data_item),
    )


def invoke_vp4r_factory_feed_lambda(data_item):
    """
    Invokes vp4r lambda to complete factory feed of a vp4r vehicle
    :param data_item: payload for invoking lambda
    :return:
    """
    lambda_client = client("lambda" , region_name= "us-east-1")
    LOG.info(f"Invoking Lambda{VP4R_factory_feed_lambda}")
    lambda_client.invoke(
        FunctionName=VP4R_factory_feed_lambda,
        InvocationType="Event",
        Payload=json.dumps(data_item),
    )


def invoke_tbm_factory_feed_lambda(data_item):
    """
    Invokes tbm lambda to complete factory feed of a tbm vehicle
    :param data_item: payload for invoking lambda
    :return:
    """
    lambda_client = client("lambda" , region_name= "us-east-1")
    LOG.info(f"Invoking Lambda{TBM_factory_feed_lambda}")
    lambda_client.invoke(
        FunctionName=TBM_factory_feed_lambda,
        InvocationType="Event",
        Payload=json.dumps(data_item),
    )
 
