import sys
from helper.connector import Connector
from helper.config_reader import ConfigReader
from helper.loader import Loader

class Migrator():
    def __init__(self):
        config = ConfigReader.load_config()
        connector_obj = Connector(config)
        ext_con = connector_obj.establish_connection('EXTRACT')
        com_con = connector_obj.establish_connection('COMMIT')
        helper_obj = Loader(ext_con, com_con, config['SETTINGS'])

        try:
            stop_mark = helper_obj.find_remaining_count()
        except Exception as e:
            # TBD: Handle Exception
            stop_mark = 0

        if stop_mark == 0:
            print ('Transfer Complete!!!')
            sys.exit()

        while stop_mark != 0:
            input_data = helper_obj.read_data()
            helper_obj.write_data(input_data)
            resume_point = helper_obj.find_resume_point()
            stop_mark = helper_obj.find_remaining_count()
            print (f'{resume_point} - Partition transfer completed; {stop_mark} - Documents Left')
        print ('Transfer Complete!!!')
        return None

