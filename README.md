# Vehicle Accidents Analysis Project

## Project Structure

The basic project structure is as follows:

```bash
root/
|-- src
    |-- framework
        |-- main.py
        |-- read_data.py
        |-- write_data.py
    |-- jobs
        |-- analysis_helper.py
    |-- resources
        |-- config_file
            |-- config.json
        |-- analysis_result
            |-- analysis_1
            |-- analysis_2
            .
            .
            |-- analysis_8
     package.zip
```     

The main Python module containing the `main()`, `spark_session()`, `config()` methods is in `src/framework/main.py`. Here, `main()` method will control the initialization of spark application through `spark_session()`, reading of config file through `config()` and Extract, Transform & Load operations.

The read data module containing the `read_files()` method is in `src/framework/read_data.py`. Here, `read_files()` method will read the source .csv files one by one as called by `main()`.

The `analysis_helper.py` Python module containing all the 8 analysis modules naming `analysis_1`, `analysis_2` ... and so on is in `src/jobs/analysis_helper.py`. These modules will be called one by one and analysed results will be written at config defined output locations.

The write data module containing the `write_files()` method is in `src/framework/write_data.py`. Here, `write_files()` method will write the analysed data result in .csv file format one by one as called by AnalysisHelper methods.

## Config File

The `config.json` file contains input source file path and output path for all the 8 analysis results is in `src/resources/config_file/config.json`.

config.json file content
```
{
    "env": "dev",
	"in_path_charges_use": "C:\\Users\\talkt\\Downloads\\Data\\Data\\Charges_use.csv",
	"in_path_damages_use" : "C:\\Users\\talkt\\Downloads\\Data\\Data\\Damages_use.csv",
	"in_path_endorse_use":"C:\\Users\\talkt\\Downloads\\Data\\Data\\Endorse_use.csv",
	"in_path_primary_person_use":"C:\\Users\\talkt\\Downloads\\Data\\Data\\Primary_Person_use.csv",
	"in_path_restrict_use": "C:\\Users\\talkt\\Downloads\\Data\\Data\\Restrict_use.csv",
	"in_path_units_use": "C:\\Users\\talkt\\Downloads\\Data\\Data\\Units_use.csv",
	"out_path_analysis_1": "C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_1\\",
	"out_path_analysis_2" : "C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_2\\",
	"out_path_analysis_3":"C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_3\\",
	"out_path_analysis_4":"C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_4\\",
	"out_path_analysis_5": "C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_5\\",
	"out_path_analysis_6": "C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_6\\",
	"out_path_analysis_7": "C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_7\\",
	"out_path_analysis_8": "C:\\Users\\talkt\\PycharmProjects\\vehicle_crash_analysis_app\\src\\resources\\analysis_result\\analysis_8\\"
}
```
