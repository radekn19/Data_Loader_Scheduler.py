import datetime
import logging
import subprocess
import time
import pandas as pd
from multiprocessing.pool import ThreadPool as Pool
import xml.etree.ElementTree as ET
import copy


# Configuration to fill in
# ---------------------------------
csvPath = r"D:\Salesforce\DL_Command\v57.0.1\bin\DL_command\File\Test\Nowy dokument tekstowy.csv"
# Number of records in file - used during slitting the csv file
fileBatch = 1

processBat = r"D:\Salesforce\DL_Command\v57.0.1\bin\process.bat"
configPath = r'D:\Salesforce\DL_Command\v57.0.1\bin\DL_command'
interfacesList = ['accountUpsert', 'accountUpsert1']

# Interval given in seconds
interval = 3


# ---------------------------------
def get_file_path():
    return csvPath[:csvPath.rfind("\\")]


def get_formatted_time_now():
    return str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))


# Split CSV file according to the given fileBatch function
def slit_csv_file(file_path, batch_size):
    csvName = file_path[file_path.rfind("\\") + 1:]
    csvConvertedName = "\\" + csvName[:csvName.find(".")] + "{}" + csvName[csvName.find("."):]
    print("Start splitting file")
    logging.info("Start splitting  the '%s' file into files witch %s records", csvName, str(batch_size))
    try:
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=fileBatch)):
            chunk.to_csv(get_file_path() + csvConvertedName.format(i), index=False)
    except Exception as e:
        print("Error -  splitting file")
        logging.error("Error while splitting file %s", file_path)
        logging.error(e)
        logging.critical("Program closed")
        raise SystemExit

    print("Stop splitting file")
    logging.info("Stop splitting the '%s' file", csvName)


def get_dl_config_for_file(config_xml):
    logging.info("Start preparing the Config XML file: '%s'", config_xml + "\\process-conf.xml")
    print("Start preparing the Config XML file: " + config_xml + "\\process-conf.xml")
    tree = ET.parse(config_xml + "\\process-conf.xml")
    root = tree.getroot()
    bean = tree.find("bean")

    beanCopy = copy.deepcopy(bean)
    root.append(beanCopy)

    # Write to new config.xml file to location where is csv file
    xmlPath = get_file_path() + "\\config.xml"
    print(xmlPath)
    with open(xmlPath, "wb") as f:
        f.write(ET.tostring(root))


# RUN Data Loader with subprocess function
def run_dataLoader_process(interfaces, loop):
    print(get_formatted_time_now() + " - START loop " + str(
        loop) + ", interface" + interfaces)
    print(configPath)
    # subprocess.run(processBat + ' ' + config + ' ' + interfaces)
    # time.sleep(2)
    print(get_formatted_time_now() + " - STOP loop " + str(
        loop) + ", interface" + interfaces)


# ------------------------------------------------------------------------------------------------------------
# START
logging.basicConfig(filename=get_file_path() + "\\log.txt", format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.INFO)

# Split CSV File
slit_csv_file(csvPath, fileBatch)

# Prepare the config with beans
get_dl_config_for_file(configPath)

# RUN Data Loader at specified intervals and in separate processes
pool = Pool(5)
loopRun = 1
for interface in interfacesList:
    if loopRun != 1:
        time.sleep(interval)
    # result = pool.apply_async(run_dataLoader_process, (interface, loopRun))
    loopRun += 1

pool.close()
pool.join()
