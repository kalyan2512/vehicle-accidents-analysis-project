from src.framework.write_data import DataWriter
from pyspark.sql.functions import *
from pyspark.sql.window import Window


class AnalysisHelper:
    """
    This class is used to perform different types of analysis on the source data and produce the result as an
    spark dataframe & write it as a .csv file in a defined path through config file.
    """

    def __init__(self):
        """
        This method is used for initialization
        """
        pass


    @staticmethod
    def analysis_1(df, config):
        """
        This method is used to analyse and
        "Find the number of crashes(accidents) in which number of persons killed are male?".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:

            analysed_df = df['primary_person_use']\
                .filter((df['primary_person_use'].PRSN_GNDR_ID == 'MALE') &
                        (df['primary_person_use'].PRSN_INJRY_SEV_ID == 'KILLED'))

            analysed_df = analysed_df.select(countDistinct(analysed_df.CRASH_ID))\
                .withColumnRenamed('count(DISTINCT CRASH_ID)', 'number_of_crashes')

            print('Writing Analysis 1 Result in path : ', config['out_path_analysis_1'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_1'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e

    @staticmethod
    def analysis_2(df, config):
        """
        This method is used to analyse and
        "Find, How many two-wheelers are booked for crashes?".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:

            analysed_df = df['units_use'].filter(df['units_use'].VEH_BODY_STYL_ID.like('%MOTORCYCLE%'))

            analysed_df = analysed_df.select(count(analysed_df.CRASH_ID))\
                .withColumnRenamed('count(CRASH_ID)', 'number_of_motorcycles')

            print('Writing Analysis 2 Result in path : ', config['out_path_analysis_2'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_2'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e

    @staticmethod
    def analysis_3(df, config):
        """
        This method is used to analyse and
        "Find, Which state has highest number of accidents in which females are involved?".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:

            analysed_df = df['primary_person_use'].filter(df['primary_person_use']['PRSN_GNDR_ID'] == 'FEMALE')

            analysed_df = analysed_df.groupBy('DRVR_LIC_STATE_ID').count().orderBy(desc('count')).limit(1)

            analysed_df = analysed_df.select('DRVR_LIC_STATE_ID').withColumnRenamed('DRVR_LIC_STATE_ID','state_name')

            print('Writing Analysis 3 Result in path : ', config['out_path_analysis_3'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_3'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e

    @staticmethod
    def analysis_4(df, config):
        """
        This method is used to analyse and
        "Find, Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of
        injuries including death?".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:
            li = ['NON-INCAPACITATING INJURY', 'INCAPACITATING INJURY', 'POSSIBLE INJURY', 'KILLED']

            analysed_df1 = df['primary_person_use']\
                .filter(df['primary_person_use'].PRSN_INJRY_SEV_ID.isin(li)).select('CRASH_ID')

            analysed_df2 = df['units_use'].select('CRASH_ID', 'VEH_MAKE_ID')

            analysed_df = analysed_df1.join(analysed_df2, analysed_df1.CRASH_ID == analysed_df2.CRASH_ID, "inner")
            analysed_df = analysed_df.groupBy('VEH_MAKE_ID').count().select('VEH_MAKE_ID','count')
            analysed_df = analysed_df.withColumn('rank',rank().over(Window.orderBy(col('count').desc())))
            analysed_df = analysed_df.where((analysed_df.rank >= 5) & (analysed_df.rank <= 15)).select('VEH_MAKE_ID')

            print('Writing Analysis 4 Result in path : ', config['out_path_analysis_4'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_4'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e

    @staticmethod
    def analysis_5(df, config):
        """
        This method is used to analyse and
        "Find, For all the body styles involved in crashes, mention the top ethnic user group of
        each unique body style?".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:

            analysed_df1 = df['primary_person_use'].select('CRASH_ID','PRSN_ETHNICITY_ID')
            analysed_df2 = df['units_use'].select('CRASH_ID', 'VEH_BODY_STYL_ID')

            analysed_df = analysed_df1.join(analysed_df2, analysed_df1.CRASH_ID == analysed_df2.CRASH_ID, "inner")
            analysed_df = analysed_df.groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').count()\
                .select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID', 'count')
            analysed_df = analysed_df\
                .withColumn('rank', rank().over(Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc())))
            analysed_df = analysed_df.where(analysed_df.rank == 1).select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')

            print('Writing Analysis 5 Result in path : ', config['out_path_analysis_5'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_5'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e

    @staticmethod
    def analysis_6(df, config):
        """
        This method is used to analyse and
        "Find, Among the crashed cars, what are the Top 5 Zip Codes with highest number of crashes
        with alcohols as the contributing factor to a crash (Use Driver Zip Code)?".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:
            li = ['PASSENGER CAR, 2-DOOR', 'PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'VAN', 'PICKUP']

            analysed_df1 = df['units_use']\
                .filter(df['units_use'].VEH_BODY_STYL_ID.isin(li)) \
                .filter((df['units_use'].CONTRIB_FACTR_1_ID == 'UNDER INFLUENCE - ALCOHOL') |
                        (df['units_use'].CONTRIB_FACTR_2_ID == 'UNDER INFLUENCE - ALCOHOL') |
                        (df['units_use'].CONTRIB_FACTR_P1_ID == 'UNDER INFLUENCE - ALCOHOL'))\
                .select('CRASH_ID')

            analysed_df2 = df['primary_person_use'].filter(df['primary_person_use'].DRVR_ZIP.isNotNull())\
                .select('CRASH_ID','DRVR_ZIP')

            analysed_df = analysed_df2.join(analysed_df1, analysed_df2.CRASH_ID == analysed_df1.CRASH_ID, 'inner' )

            analysed_df = analysed_df.groupBy('DRVR_ZIP').count().orderBy(desc('count'))\
                .limit(5).select('DRVR_ZIP')

            print('Writing Analysis 6 Result in path : ', config['out_path_analysis_6'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_6'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e

    @staticmethod
    def analysis_7(df, config):
        """
        This method is used to analyse and
        "Find, Count of Distinct Crash IDs where No Damaged Property was observed and
        Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance?".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:

            li1 = ['DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST']
            li2 = ['PROOF OF LIABILITY INSURANCE', 'LIABILITY INSURANCE POLICY']

            analysed_df1 = df['units_use'] \
                .filter((df['units_use'].VEH_DMAG_SCL_1_ID.isin(li1)) |
                         (df['units_use'].VEH_DMAG_SCL_2_ID.isin(li1))) \
                .filter(df['units_use'].FIN_RESP_TYPE_ID.isin(li2)).select('CRASH_ID')

            analysed_df2 = df['damages_use'].select('CRASH_ID')

            analysed_df = analysed_df1\
                .join(analysed_df2, analysed_df1.CRASH_ID == analysed_df2.CRASH_ID, 'left_anti')

            analysed_df = analysed_df.select(countDistinct(analysed_df.CRASH_ID)) \
                .withColumnRenamed('count(DISTINCT CRASH_ID)', 'number_of_crashes')

            print('Writing Analysis 7 Result in path : ', config['out_path_analysis_7'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_7'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e

    @staticmethod
    def analysis_8(df, config):
        """
        This method is used to analyse and
        "Find, Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the
        Top 25 states with highest number of offences (to be deduced from the data)".

        :param df: Spark Dataframe dictionary, contains all the source dataframe in form of dictionary
        :param config: config file
        :return: None
        """
        try:

            li1 = ['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.', 'ID CARD']

            analysed_df1 = df['primary_person_use'].filter(df['primary_person_use'].DRVR_LIC_TYPE_ID.isin(li1))\
                .select('CRASH_ID')

            analysed_df2 = df['charges_use'].filter(df['charges_use'].CHARGE.like('%SPEED%'))\
                .select('CRASH_ID')

            over_speed_df = analysed_df1.intersect(analysed_df2)

            analysed_df3 = df['units_use'].join(df['charges_use'], df['units_use'].CRASH_ID ==
                                                df['charges_use'].CRASH_ID, 'inner')\
                .select('VEH_COLOR_ID', 'VEH_LIC_STATE_ID')

            veh_lic_state_df = analysed_df3.groupBy('VEH_LIC_STATE_ID').count().orderBy(desc('count')).limit(25)

            color_df = analysed_df3.join(veh_lic_state_df, analysed_df3.VEH_LIC_STATE_ID ==
                                         veh_lic_state_df.VEH_LIC_STATE_ID, 'inner').select('VEH_COLOR_ID')

            top_color_df = color_df.groupBy('VEH_COLOR_ID').count().orderBy(desc('count')).limit(10)

            analysed_df = df['units_use'].join(over_speed_df, df['units_use'].CRASH_ID ==
                                               over_speed_df.CRASH_ID, 'inner')

            analysed_df = analysed_df.join(top_color_df, analysed_df.VEH_COLOR_ID ==
                                           top_color_df.VEH_COLOR_ID, 'inner')\
                .select('VEH_MAKE_ID')

            analysed_df = analysed_df.groupBy('VEH_MAKE_ID').count().orderBy(desc('count')).limit(5)\
                .select('VEH_MAKE_ID')

            print('Writing Analysis 8 Result in path : ', config['out_path_analysis_8'])
            DataWriter.write_files(analysed_df, config['out_path_analysis_8'])

            return None

        except Exception as e:
            print("Analysis FAILED due to : ", e)
            raise e


