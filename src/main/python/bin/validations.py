import logging
import logging.config

logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)


def get_current_date(spark):

    try:
        opDF = spark.sql(""" select current_date """)
        logger.info("Validate the Spark object by printing Current Date -" + str(opDF.collect()))

    except NameError as exp:
        logger.error("NameError in the method - get_curr_date(). Please check the Stack Trace. " + str(exp), exc_info= True)
        raise
    except Exception as exp:
        logger.error("Error in the method - get_curr_date(). Please check the Stack Trace. " + str(exp), exc_info= True)

    else:
        logger.info("Spark object is validated. Spark Object is created")