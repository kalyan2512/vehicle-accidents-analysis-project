
class DataWriter:
    """
    This class is used to write analysed data to a config defined output location.
    """


    def __init__(self):
        """
        This method is used for initialization
        """
        pass


    @staticmethod
    def write_files(analysed_dataframe, path):
        """
        This method is used to write analysed data result in .csv file format at output location

        :param analysed_dataframe: Spark Dataframe
        :param path: Output location
        :return:
        """
        try:

            analysed_dataframe.coalesce(1)\
                .write.csv(path,
                           sep=',',
                           mode='overwrite',
                           header=True,
                           encoding='UTF-8',
                           emptyValue='',
                           quote="")

        except Exception as e:
            print("Writing data FAILED due to : ", e)
            raise e