import ibm_db
import configparser
import logging
from paramiko import SSHClient
import paramiko
import os
import time
from functools import wraps
import kms_decrypt

config_parser = configparser.ConfigParser()
config_parser.read('ca_to_us.ini')

logLevel = (os.getenv("logLevel", "INFO")).upper()
logging.basicConfig(level=logging.INFO if logLevel == "INFO" else logging.ERROR, datefmt='%H:%M:%S',
                    format='%(levelname)s: %(module)s:%(funcName)s:%(lineno)d: %(asctime)s: %(message)s')
LOG = logging.getLogger(__name__)


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    LOG.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry(Exception, tries=3)
def server_connection(server=None):
    """
    Establishes the server connection.

    :param server: String, name of server
    :return connection_flag: boolean
    :return client: object
    """

    assert server is not None, "Provide a server name"

    LOG.info("Trying to establish connection with  Server...")
    host_name = config_parser[server]['server']
    user_name = kms_decrypt.decrypt(os.environ.get("username"))
    password = kms_decrypt.decrypt(os.environ.get("password"))
    client = SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()
    connection_flag = 0
    try:
        client.connect(host_name, username=user_name, password=password)
        connection_flag = 1
        LOG.info(f"Connection established with {server} server")
    except Exception as e:
        LOG.error(f"Connection not established with {server} server")
        raise Exception(f"Connection not established with {server} server. Error : {str(e)}")
    finally:
        return connection_flag, client


@retry(Exception, tries=3)
def database_connection(db=None):
    """
    Establishes the connection to database
    :param db: string, name of database
    :return connection object
    """

    assert db is not None, "Please provide the name of Database"

    LOG.info(f"Establishing connection with {db} database...")
    try:
        uid = kms_decrypt.decrypt(os.environ.get("CVP_username"))
        password = kms_decrypt.decrypt(os.environ.get("CVP_password"))

        connection = ibm_db.connect("DATABASE=" + config_parser[db]['database'] + ";Instance=" + config_parser[db]['instance'] + ";HOSTNAME=" + config_parser[db]['hostname'] + ";PORT=" + config_parser[db]['port'] + ";PROTOCOL=" + config_parser[db]['protocol'] + ";UID=" + uid + ";PWD=" + password + ";", "", "")
        LOG.info(f"Connected to {db} database")
        return connection
    except Exception as e:
        LOG.error(f"Connection not established with {db} database. Error  : {str(e)}")
        raise Exception(f"Connection not established with {db} database. Error  : {str(e)}")
        
def close_connection(db_connection):
    """
    Closes the database connection.

    :param db_connection: ibm_db connect object
    :return ibm_db close
    """
    ibm_db.close(db_connection)
