from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)

def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() has started. The '{envn}' environment is used.")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
    except NameError as exp:
        logger.error("NameError in the method - get_spark_object(). Please check the Stack Trace. " + str(exp), exc_info= True)
        raise
    except Exception as exp:
        logger.error("Error in the method - get_spark_object(). Please check the Stack Trace. " + str(exp), exc_info= True)
    else:
        logger.info("Spark Object is created ...")
    return spark




