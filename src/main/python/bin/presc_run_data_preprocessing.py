import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, isnan, when, avg, round, coalesce
from pyspark.sql.window import Window

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def perform_data_clean(df1, df2):
    try:
        logger.info(f"perform_data_clean() has started ...")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)

        logger.info(f"perform_data_clean() has started for df_fact dataframe...")
        # Select only required Columns
        # Rename the columns
        df_fact_sel = df2.select(df2.npi.alias("presc_id"),df2.nppes_provider_last_org_name.alias("presc_lname"),\
                             df2.nppes_provider_first_name.alias("presc_fname"),df2.nppes_provider_city.alias("presc_city"),\
                             df2.nppes_provider_state.alias("presc_state"),df2.specialty_description.alias("presc_spclt"), df2.years_of_exp,\
                             df2.drug_name,df2.total_claim_count.alias("trx_cnt"),df2.total_day_supply,\
                             df2.total_drug_cost)

        # Add a Country Field 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name",lit("USA"))

        # Clean years of experience field
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))

        # Convert years of experience to string
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", col("years_of_exp").cast("int"))

        # Combine first and last name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")

        # Check the null values in each column
        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()

        # Delete the records where the PRESC_ID is NULL
        df_fact_sel = df_fact_sel.dropna(subset="presc_id")

        # Delete the records where the DRUG_NAME is NULL
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")

        # Impute TRX_CNT where it is null as avg of trx_cnt for that prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce("trx_cnt",round(avg("trx_cnt").over(spec))))
        df_fact_sel=df_fact_sel.withColumn("trx_cnt",col("trx_cnt").cast('integer'))

        # Check and clean all the Null/Nan Values
        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_fact_sel.columns]).show()

    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() has completed...")
    return df_city_sel, df_fact_sel