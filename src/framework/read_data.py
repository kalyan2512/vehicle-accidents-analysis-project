
class DataReader:
    """
    This class is used to read source data.
    """

    def __init__(self):
        """
        This method is used for initialization
        """
        pass

    @staticmethod
    def read_files(spark, path):
        """
        This method is used to read source files.

        :param spark: Spark Session object
        :param path: Input location
        :return:
        """
        try:

            source_dataframe = spark.read.option("header", True) \
                .option("delimiter", ",") \
                .option("encoding", "UTF-8") \
                .csv(path)

            return source_dataframe

        except Exception as e:
            print("Reading files FAILED due to : ", e)
            raise e




