import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_current_date

def main():

    spark = get_spark_object(gav.envn, gav.appName)
    print("Spark Object is created ...")
    get_current_date(spark)

if __name__ == '__main__':
    main()

