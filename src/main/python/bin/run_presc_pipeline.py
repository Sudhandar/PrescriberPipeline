import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_current_date, df_count, df_top10_rec, df_print_schema
import sys
import logging
import logging.config
import os
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean

logging.config.fileConfig(fname="../util/logging_to_file.conf")
def main():

    logging.info("Main function has started")
    try:
        spark = get_spark_object(gav.envn, gav.appName)
        logging.info("Spark Object is created ...")
        get_current_date(spark)

        for file in os.listdir(gav.staging_dim_city):
            print("File is "+ file)
            file_dir = gav.staging_dim_city + '/' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_city = load_files(spark = spark, file_dir = file_dir, file_format = file_format, header = header, inferSchema = inferSchema)
        df_count(df_city, "US City Dataframe" )
        df_top10_rec(df_city, "US City Dataframe")

        for file in os.listdir(gav.staging_fact):
            print("File is "+ file)
            file_dir = gav.staging_fact + '/' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_fact = load_files(spark = spark, file_dir = file_dir, file_format = file_format, header = header, inferSchema = inferSchema)
        df_count(df_fact, "US Facts Dataframe" )
        df_top10_rec(df_fact, "US Facts Dataframe")

        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)
        df_top10_rec(df_city_sel, "US City DataFrame Selected")
        df_top10_rec(df_fact_sel, "US Fact DataFrame Selected")
        df_print_schema(df_fact_sel, "US Fact Dataframe Selected Columns")

        logging.info("Run Prescriber Pipeline has been completed")

    except Exception as exp:
        logging.error("Error in the method - main(). Please check the Stack Trace to go to the respective module and fix it. " + str(exp), exc_info = True)
        sys.exit(1)

if __name__ == '__main__':
    logging.info("Run Prescriber Pipeline has started")
    main()