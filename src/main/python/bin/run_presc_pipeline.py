import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_current_date
import sys
import logging
import logging.config

logging.config.fileConfig(fname="../util/logging_to_file.conf")
def main():

    logging.info("Main function has started")
    try:
        spark = get_spark_object(gav.envn, gav.appName)
        logging.info("Spark Object is created ...")
        get_current_date(spark)
        logging.info("Run Prescriber Pipeline has been completed")
    except Exception as exp:
        logging.error("Error in the method - main(). Please check the Stack Trace to go to the respective module and fix it. " + str(exp), exc_info = True)
        sys.exit(1)

if __name__ == '__main__':
    logging.info("Run Prescriber Pipeline has started")
    main()

