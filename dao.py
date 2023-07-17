import ibm_db
import logging
import os

US = "US"
CA = "CA"

logLevel = (os.getenv("logLevel", "INFO")).upper()

logging.basicConfig(
    level=logging.INFO if logLevel == "INFO" else logging.ERROR,
    datefmt="%H:%M:%S",
    format="%(levelname)s: %(module)s:%(funcName)s:%(lineno)d: %(asctime)s: %(message)s",
)
LOG = logging.getLogger(__name__)




def check_vehicle_table_for_conversion(db_connection=None, vin=None):
    """
    Check if the conversion is done in database by the batch
    :param db_connection: ibm_db connect object
    :param vin: string, vin to be processed
    :return boolean
    """

    assert db_connection is not None, "Connection not established"
    assert vin is not None, "Please provide proper VIN"

    LOG.info("Checking the destination country for VIN " + vin)

    vehicle_query = f"Select N_DEST_CNTRY from CVP.VEHICLE  WHERE I_VIN = ('{vin}')"
    vehicle_result = search_database(db_connection, vehicle_query)
    vehicle_cnty = vehicle_result[0].replace(" ","")
    if vehicle_cnty == US:
        LOG.info("The destination country is: " + vehicle_cnty)
        return  True
       
    elif vehicle_cnty == CA:
        LOG.info("The destination country is : " + vehicle_cnty)
        return False
        
    


def check_vehicle_table(db_connection=None, vin=None):
    """Checks if the vin is present in VEHICLE table and if the vehicle is TBM or VP4R
    :param db_connection: ibm_db connect object
    :param vin: string, vin to be processed
    :return boolean
    """
    assert db_connection is not None, "Connection not established"
    assert vin is not None, "Please provide proper VIN"

    isTBM = False
    isVP4R = False

    LOG.info("Checking VIN " + vin + " in VEHICLE table...")
    vehicle_query = f"SELECT C_VHCL_TYP from CVP.VEHICLE where I_VIN in ('{vin}') with ur"
    LOG.info("vehicle_query")
    vehicle_result = search_database(db_connection, vehicle_query)
    if vehicle_result :
        vehicle_name = vehicle_result[0].replace(" ","")
        if vehicle_name == "CVP_TBM":
            LOG.info("Vin " + vin + " found in VEHICLE table and is TBM")
            isTBM = True
            return True,isTBM,isVP4R
        elif vehicle_name == "CVP_SXM":
            LOG.info("Vin " + vin + " found in VEHICLE table and is VP4R")
            isVP4R = True
            return True,isTBM,isVP4R
        else:
            LOG.info("Vin " + vin + " found in VEHICLE table and is neither TBM nor VP4R")
            return True,isTBM,isVP4R
    else:
        LOG.info("Vin " + vin + " not found in vehicle table. please check vin again .")
        return False,isTBM,isVP4R
    
            
def search_database(db_connection=None, sql_query=None):
    """Used to execute Query.

    :param db_connection: ibm_db connect object
    :param sql_query: string, sql query
    :return result of query
    """
    assert db_connection is not None, "Connection not established"
    assert sql_query is not None, "No query provided"

    execute_query = ibm_db.exec_immediate(db_connection, sql_query)
    result = ibm_db.fetch_tuple(execute_query)
    return result
