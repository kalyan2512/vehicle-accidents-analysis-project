from pyspark.sql import SparkSession
from pyspark import SparkFiles
from os import listdir, path
import json
from src.framework.read_data import DataReader
from src.jobs.analysis_helper import AnalysisHelper


def spark_session():
    """
    This method is used to create spark session object called spark.

    :return: spark
    """

    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("vehicle_crash_analysis_app")\
        .getOrCreate()

    return spark


def config():
    """
    This method is used to search the path of config.json file and to read it from there.
    It will also convert it to dictionary object.

    :return: config_dict
    """

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    print(spark_files_dir)
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        print('loaded config from ' + config_files[0])
        return config_dict
    else:
        print('no config file found')
        exit()


def main():
    """
    This is the main method of this framework, all the objects creation & data extract, transform and load task will be
    controlled from main method.

    :return: None
    """

    src_df = {}

    try:
        print("In main")

        print("Starting Spark application and getting Spark session object..")
        spark = spark_session()

        config_dict = config()
        print(config_dict)

        print("Reading Charges file from path : ", config_dict['in_path_charges_use'])
        src_df['charges_use'] = DataReader.read_files(spark, config_dict['in_path_charges_use'])

        print("Reading Damages file from path : ", config_dict['in_path_damages_use'])
        src_df['damages_use'] = DataReader.read_files(spark, config_dict['in_path_damages_use'])

        print("Reading Endorse file from path : ", config_dict['in_path_endorse_use'])
        src_df['endorse_use'] = DataReader.read_files(spark, config_dict['in_path_endorse_use'])

        print("Reading Primary Person file from path : ", config_dict['in_path_primary_person_use'])
        src_df['primary_person_use'] = DataReader.read_files(spark, config_dict['in_path_primary_person_use'])

        print("Reading Restrict file from path : ", config_dict['in_path_restrict_use'])
        src_df['restrict_use'] = DataReader.read_files(spark, config_dict['in_path_restrict_use'])

        print("Reading Units file from path : ", config_dict['in_path_units_use'])
        src_df['units_use'] = DataReader.read_files(spark, config_dict['in_path_units_use'])

        print("Performing Analysis 1...")
        AnalysisHelper.analysis_1(src_df, config_dict)
        print("Performing Analysis 2...")
        AnalysisHelper.analysis_2(src_df, config_dict)
        print("Performing Analysis 3...")
        AnalysisHelper.analysis_3(src_df, config_dict)
        print("Performing Analysis 4...")
        AnalysisHelper.analysis_4(src_df, config_dict)
        print("Performing Analysis 5...")
        AnalysisHelper.analysis_5(src_df, config_dict)
        print("Performing Analysis 6...")
        AnalysisHelper.analysis_6(src_df, config_dict)
        print("Performing Analysis 7...")
        AnalysisHelper.analysis_7(src_df, config_dict)
        print("Performing Analysis 8...")
        AnalysisHelper.analysis_8(src_df, config_dict)

        print('Analysis Completed.')

        spark.stop()

        return None

    except Exception as e:
        raise e


if __name__ == '__main__':
    main()


