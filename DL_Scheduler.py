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
fileBatch = 5

processBat = r"D:\Salesforce\DL_Command\v57.0.1\bin\process.bat"
configPath = r'D:\Salesforce\DL_Command\v57.0.1\bin\DL_command'
interfacesList = ['accountUpsert', 'accountUpsert1']

# Interval given in seconds
interval = 3

csv_files = []


# ---------------------------------
def get_file_path():
    return csvPath[:csvPath.rfind("\\")] + "\\"


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
            csv_files.append(csvConvertedName.format(i)[1:])
            print(csvConvertedName.format(i)[1:])
    except Exception as e:
        print("Error -  splitting file")
        logging.error("Error while splitting file %s", file_path)
        logging.error(e)
        logging.critical("Program closed")
        raise SystemExit

    print("Stop splitting file")
    logging.info("Stop splitting the '%s' file", csvName)


def prepare_dl_config(config_xml_path, csv_files_list):
    logging.info("Start preparing the config.xml file based on: '%s'", config_xml_path + "\\process-conf.xml")
    print("Start preparing the config.xml file based on: " + config_xml_path + "\\process-conf.xml")
    try:
        newConfigXmlPath = get_file_path() + "config.xml"
        tree = ET.parse(config_xml_path + "\\process-conf.xml")
        root = tree.getroot()
        beanRoot = root.find('bean')

        i = 0
        for file_l in csv_files_list:
            beanLoop = copy.deepcopy(beanRoot)
            print(file_l)
            if i == 0:
                root.find("./bean[@class='com.salesforce.dataloader.process.ProcessRunner']").attrib["id"] = file_l
                root.find("./bean/property/map/entry[@key='dataAccess.name']").attrib["value"] = get_file_path() + file_l
            else:
                beanLoop.find(".[@class='com.salesforce.dataloader.process.ProcessRunner']").attrib["id"] = file_l
                beanLoop.find("./property/map/entry[@key='dataAccess.name']").attrib["value"] = get_file_path() + file_l
                root.append(beanLoop)

            i += 1

        # Write to new config.xml file to location where is csv file
        with open(newConfigXmlPath, "wb") as f:
            f.write(ET.tostring(root))
    except Exception as e:
        print("Error -  preparing the config.xml file")
        logging.error("Error while preparing the config.xml file: %s based on %s\\process-conf.xml", newConfigXmlPath,
                      config_xml_path)
        logging.error(e)
        logging.critical("Program closed")
        raise SystemExit

    logging.info("Stop - config.xml file created with '%s' beans - '%s'", i, newConfigXmlPath)
    print("Stop - config.xml file created with beans: " + str(i))




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
logging.basicConfig(filename=get_file_path() + "log.txt", format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.INFO)

# Split CSV File
slit_csv_file(csvPath, fileBatch)

# Prepare the config with beans
prepare_dl_config(configPath, csv_files)

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
